package synap

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

// response is one command's reply, in whichever form its transport produced.
//
// HTTP and RESP3 genuinely speak JSON, so their replies stay JSON. SynapRPC
// does not: it carries typed MessagePack values, and re-encoding them as JSON
// just to hand them to a module method destroyed any value that was not valid
// UTF-8 — Go's JSON encoder replaces every invalid sequence with U+FFFD, so
// `deadbeef` came back as `deadefbfbdefbfbd`. The binary transport now keeps
// its values as Go values all the way to the caller.
//
// Decode is the single seam both paths meet at, so a module method does not
// need to know which transport answered it.
type response struct {
	// json is set on the HTTP and RESP3 paths.
	json json.RawMessage
	// value is set on the SynapRPC path — decoded MessagePack, byte-exact.
	value interface{}
	// fromRPC distinguishes a nil RPC value from an absent one.
	fromRPC bool
}

// jsonResponse wraps a reply that really is JSON.
func jsonResponse(raw json.RawMessage) response {
	return response{json: raw}
}

// valueResponse wraps a reply that arrived as typed values.
func valueResponse(v interface{}) response {
	return response{value: v, fromRPC: true}
}

// Decode fills target from the reply.
func (r response) Decode(target interface{}) error {
	if !r.fromRPC {
		return json.Unmarshal(r.json, target)
	}
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("synap: decode target must be a non-nil pointer, got %T", target)
	}
	return assign(r.value, rv.Elem())
}

// Raw renders the reply as JSON, for the few callers that want the payload
// itself rather than a decoded struct.
//
// On the RPC path this re-encodes, so it carries the same UTF-8 caveat the rest
// of this file exists to avoid: use Decode when the value may be binary.
func (r response) Raw() (json.RawMessage, error) {
	if !r.fromRPC {
		return r.json, nil
	}
	return json.Marshal(r.value)
}

// IsNull reports whether the reply carried no value at all.
func (r response) IsNull() bool {
	if r.fromRPC {
		return r.value == nil
	}
	return len(r.json) == 0 || string(r.json) == "null"
}

// assign writes src into dst, converting between the shapes MessagePack
// decoding produces and the struct fields the modules declare.
func assign(src interface{}, dst reflect.Value) error {
	if src == nil {
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}

	// An interface{} target takes the value as-is.
	if dst.Kind() == reflect.Interface && dst.NumMethod() == 0 {
		dst.Set(reflect.ValueOf(src))
		return nil
	}

	if dst.Kind() == reflect.Ptr {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		return assign(src, dst.Elem())
	}

	switch dst.Kind() {
	case reflect.String:
		s, err := asStringValue(src)
		if err != nil {
			return err
		}
		dst.SetString(s)
		return nil

	case reflect.Bool:
		switch v := src.(type) {
		case bool:
			dst.SetBool(v)
		case int64:
			dst.SetBool(v != 0)
		case float64:
			dst.SetBool(v != 0)
		default:
			return fmt.Errorf("synap: cannot decode %T into bool", src)
		}
		return nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := asInt(src)
		if err != nil {
			return err
		}
		dst.SetInt(n)
		return nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := asInt(src)
		if err != nil {
			return err
		}
		if n < 0 {
			return fmt.Errorf("synap: cannot decode negative %d into %s", n, dst.Kind())
		}
		dst.SetUint(uint64(n))
		return nil

	case reflect.Float32, reflect.Float64:
		f, err := asFloat(src)
		if err != nil {
			return err
		}
		dst.SetFloat(f)
		return nil

	case reflect.Slice:
		// A byte slice takes the raw bytes of a string, not a per-element walk.
		if dst.Type().Elem().Kind() == reflect.Uint8 {
			s, err := asStringValue(src)
			if err != nil {
				return err
			}
			dst.SetBytes([]byte(s))
			return nil
		}
		items, ok := src.([]interface{})
		if !ok {
			return fmt.Errorf("synap: cannot decode %T into %s", src, dst.Type())
		}
		out := reflect.MakeSlice(dst.Type(), len(items), len(items))
		for i, item := range items {
			if err := assign(item, out.Index(i)); err != nil {
				return err
			}
		}
		dst.Set(out)
		return nil

	case reflect.Map:
		entries, ok := src.(map[string]interface{})
		if !ok {
			return fmt.Errorf("synap: cannot decode %T into %s", src, dst.Type())
		}
		out := reflect.MakeMapWithSize(dst.Type(), len(entries))
		for k, v := range entries {
			elem := reflect.New(dst.Type().Elem()).Elem()
			if err := assign(v, elem); err != nil {
				return err
			}
			out.SetMapIndex(reflect.ValueOf(k).Convert(dst.Type().Key()), elem)
		}
		dst.Set(out)
		return nil

	case reflect.Struct:
		entries, ok := src.(map[string]interface{})
		if !ok {
			return fmt.Errorf("synap: cannot decode %T into %s", src, dst.Type())
		}
		t := dst.Type()
		for i := range dst.NumField() {
			field := t.Field(i)
			if field.PkgPath != "" {
				continue // unexported
			}
			name, _, skip := jsonFieldName(field)
			if skip {
				continue
			}
			v, present := entries[name]
			if !present {
				continue // absent stays zero, as with encoding/json
			}
			if err := assign(v, dst.Field(i)); err != nil {
				return fmt.Errorf("field %s: %w", name, err)
			}
		}
		return nil

	default:
		return fmt.Errorf("synap: cannot decode into %s", dst.Kind())
	}
}

// asStringValue renders a scalar as a string, preserving bytes exactly.
func asStringValue(src interface{}) (string, error) {
	switch v := src.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(v), nil
	default:
		return "", fmt.Errorf("synap: cannot decode %T into string", src)
	}
}

func asInt(src interface{}) (int64, error) {
	switch v := src.(type) {
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("synap: cannot decode %T into an integer", src)
	}
}

func asFloat(src interface{}) (float64, error) {
	switch v := src.(type) {
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("synap: cannot decode %T into a float", src)
	}
}

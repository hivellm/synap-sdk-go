package synap

import (
	"reflect"
	"strings"
)

// payloadToMap converts a command payload struct into the map the wire command
// mapper reads, honouring `json` tags and `omitempty` so the mapper's key
// lookups are unchanged.
//
// It exists because `encoding/json` cannot carry binary. The RPC path used to
// marshal the payload to JSON and unmarshal it straight back into a map, purely
// to reach the fields by name — and Go's JSON encoder replaces every invalid
// UTF-8 sequence with U+FFFD. A binary value was therefore destroyed inside the
// client, before it was ever framed:
//
//	in=deadbeef  json={"value":"ޭ��"}  out=deadefbfbdefbfbd
//
// A JSON round trip in the middle of a binary transport is the defect. Walking
// the struct with reflection reaches the same fields without ever encoding, so
// strings stay byte-exact.
//
// Numbers become float64 and nested values become map/slice of interface{},
// matching what `encoding/json` produced, so every one of the mapper's 72
// command cases keeps working against the same shapes.
func payloadToMap(payload interface{}) map[string]interface{} {
	if payload == nil {
		return map[string]interface{}{}
	}

	v := reflect.ValueOf(payload)
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return map[string]interface{}{}
		}
		v = v.Elem()
	}

	// A payload that is already a map needs no field walk.
	if v.Kind() == reflect.Map {
		out := make(map[string]interface{}, v.Len())
		for _, key := range v.MapKeys() {
			out[toString(key)] = payloadValue(v.MapIndex(key))
		}
		return out
	}

	if v.Kind() != reflect.Struct {
		return map[string]interface{}{}
	}

	out := make(map[string]interface{}, v.NumField())
	t := v.Type()
	for i := range v.NumField() {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue // unexported
		}

		name, omitEmpty, skip := jsonFieldName(field)
		if skip {
			continue
		}

		fv := v.Field(i)
		if omitEmpty && isEmptyValue(fv) {
			continue
		}

		// A nil pointer is an absent optional field, exactly as `omitempty`
		// would have dropped it — the mapper tests presence by key.
		if fv.Kind() == reflect.Ptr && fv.IsNil() {
			continue
		}

		out[name] = payloadValue(fv)
	}
	return out
}

// jsonFieldName resolves a struct field's wire name from its `json` tag.
func jsonFieldName(field reflect.StructField) (name string, omitEmpty bool, skip bool) {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return "", false, true
	}
	name = field.Name
	parts := strings.Split(tag, ",")
	if parts[0] != "" {
		name = parts[0]
	}
	for _, opt := range parts[1:] {
		if opt == "omitempty" {
			omitEmpty = true
		}
	}
	return name, omitEmpty, false
}

// payloadValue converts one field to the JSON-shaped Go value the mapper
// expects — with strings passed through untouched.
func payloadValue(v reflect.Value) interface{} {
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.String:
		// The whole point: no encoding, so invalid UTF-8 survives.
		return v.String()
	case reflect.Bool:
		return v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint())
	case reflect.Float32, reflect.Float64:
		return v.Float()
	case reflect.Slice:
		// A byte slice is binary, and must not be spread into a number list.
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return string(v.Bytes())
		}
		fallthrough
	case reflect.Array:
		out := make([]interface{}, v.Len())
		for i := range v.Len() {
			out[i] = payloadValue(v.Index(i))
		}
		return out
	case reflect.Map:
		out := make(map[string]interface{}, v.Len())
		for _, key := range v.MapKeys() {
			out[toString(key)] = payloadValue(v.MapIndex(key))
		}
		return out
	case reflect.Struct:
		return payloadToMap(v.Interface())
	default:
		return nil
	}
}

// toString renders a map key.
func toString(v reflect.Value) string {
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if v.Kind() == reflect.String {
		return v.String()
	}
	return ""
}

// isEmptyValue mirrors `encoding/json`'s notion of empty for `omitempty`.
func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	default:
		return false
	}
}

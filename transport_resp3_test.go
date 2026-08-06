package synap

import (
	"bufio"
	"reflect"
	"strings"
	"testing"
)

// read parses one RESP3 value out of a wire fragment.
func read(t *testing.T, wire string) (interface{}, error) {
	t.Helper()
	return readResponse(bufio.NewReader(strings.NewReader(wire)))
}

// The server answers SSTATS, SUBSCRIBE and every SREAD entry with a RESP3 map,
// so a reader that stops at '%' cannot use streams at all — which is exactly
// how it behaved before: `unknown type byte "%"`.
func TestResp3ReaderDecodesMaps(t *testing.T) {
	value, err := read(t, "%2\r\n$4\r\nname\r\n$4\r\nchat\r\n$10\r\ngeneration\r\n:1785964676070\r\n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, ok := value.(map[string]interface{})
	if !ok {
		t.Fatalf("got %T, want a map", value)
	}
	if got["name"] != "chat" {
		t.Errorf("name is %v, want chat", got["name"])
	}
	if got["generation"] != int64(1785964676070) {
		t.Errorf("generation is %v (%T), want the int64 1785964676070", got["generation"], got["generation"])
	}
}

func TestResp3ReaderDecodesAnArrayOfMaps(t *testing.T) {
	// The shape SREAD replies with: one map per event.
	wire := "*2\r\n" +
		"%2\r\n$6\r\noffset\r\n:0\r\n$5\r\nevent\r\n$4\r\ntick\r\n" +
		"%2\r\n$6\r\noffset\r\n:1\r\n$5\r\nevent\r\n$4\r\ntick\r\n"

	value, err := read(t, wire)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events, ok := value.([]interface{})
	if !ok || len(events) != 2 {
		t.Fatalf("got %v, want two events", value)
	}
	first, ok := events[0].(map[string]interface{})
	if !ok {
		t.Fatalf("event 0 is %T, want a map", events[0])
	}
	if first["offset"] != int64(0) || first["event"] != "tick" {
		t.Errorf("event 0 decoded as %v", first)
	}
}

func TestResp3ReaderDecodesTheRemainingScalarTypes(t *testing.T) {
	cases := []struct {
		name string
		wire string
		want interface{}
	}{
		{"boolean true", "#t\r\n", true},
		{"boolean false", "#f\r\n", false},
		{"double", ",3.25\r\n", 3.25},
		{"big number", "(3492890328409238509324850943850943825024385\r\n", "3492890328409238509324850943850943825024385"},
		{"verbatim string", "=15\r\ntxt:hello world\r\n", "hello world"},
		{"null", "_\r\n", nil},
		{"set", "~2\r\n$1\r\na\r\n$1\r\nb\r\n", []interface{}{"a", "b"}},
	}

	for _, tc := range cases {
		got, err := read(t, tc.wire)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s decoded as %v (%T), want %v", tc.name, got, got, tc.want)
		}
	}
}

func TestResp3ReaderSurfacesABlobError(t *testing.T) {
	_, err := read(t, "!19\r\nERR unknown command\r\n")
	if err == nil {
		t.Fatal("a blob error must be returned as an error")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Errorf("error is %q, want the server message", err.Error())
	}
}

func TestResp3ReaderRejectsAnUnknownTypeByte(t *testing.T) {
	if _, err := read(t, "^oops\r\n"); err == nil {
		t.Error("an unknown type byte must not decode silently")
	}
}

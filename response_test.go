package synap

import "testing"

// The whole point of the response seam: a value that is not valid UTF-8 must
// reach the caller unchanged. Re-encoding replies as JSON to hand them to the
// module methods replaced every invalid sequence with U+FFFD, so `deadbeef`
// came back as `deadefbfbdefbfbd` — corrupt and unrecoverable.
func TestRPCResponseKeepsBinaryByteExact(t *testing.T) {
	payload := string([]byte{0xDE, 0xAD, 0xBE, 0xEF})

	var got string
	if err := valueResponse(payload).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got != payload {
		t.Errorf("decoded %x, want %x", got, payload)
	}
}

func TestRPCResponseDecodesAStructByJSONTag(t *testing.T) {
	src := map[string]interface{}{
		"deleted": true,
		"value":   int64(42),
		"name":    "queue-1",
	}

	var got struct {
		Deleted bool   `json:"deleted"`
		Value   int64  `json:"value"`
		Name    string `json:"name"`
		Absent  string `json:"absent"`
	}
	if err := valueResponse(src).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !got.Deleted || got.Value != 42 || got.Name != "queue-1" {
		t.Errorf("decoded %+v", got)
	}
	if got.Absent != "" {
		t.Errorf("an absent field must stay zero, got %q", got.Absent)
	}
}

func TestRPCResponseDecodesNestedSlicesAndMaps(t *testing.T) {
	src := map[string]interface{}{
		"events": []interface{}{
			map[string]interface{}{"offset": int64(1), "data": "a"},
			map[string]interface{}{"offset": int64(2), "data": "b"},
		},
	}

	var got struct {
		Events []struct {
			Offset int64  `json:"offset"`
			Data   string `json:"data"`
		} `json:"events"`
	}
	if err := valueResponse(src).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.Events) != 2 || got.Events[1].Offset != 2 || got.Events[1].Data != "b" {
		t.Errorf("decoded %+v", got)
	}
}

// The HTTP and RESP3 paths genuinely speak JSON and must keep doing so.
func TestJSONResponseStillDecodesAsJSON(t *testing.T) {
	var got struct {
		Exists bool `json:"exists"`
	}
	if err := jsonResponse([]byte(`{"exists":true}`)).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.Exists {
		t.Error("exists = false, want true")
	}
}

func TestIsNullCoversBothPaths(t *testing.T) {
	if !valueResponse(nil).IsNull() {
		t.Error("a nil RPC value must read as null")
	}
	if !jsonResponse([]byte("null")).IsNull() {
		t.Error("JSON null must read as null")
	}
	if valueResponse("").IsNull() {
		t.Error("an empty string is a value, not null")
	}
}

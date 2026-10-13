package synap

import "testing"

// The native transports speak raw commands, and a wrong name fails at runtime
// with "unknown command" rather than at compile time — `stream.consume` mapped
// to a SCONSUME the server has never implemented, so consuming a stream over
// SynapRPC or RESP3 was simply broken. These tests pin the whole stream family
// to the command names the server dispatches
// (crates/synap-server/src/protocol/resp3/command/mod.rs).
func TestStreamCommandsMapToTheNamesTheServerDispatches(t *testing.T) {
	cases := []struct {
		command string
		payload map[string]interface{}
		wantRaw string
	}{
		{"stream.create", map[string]interface{}{"room": "chat"}, "SCREATE"},
		{"stream.get_or_create", map[string]interface{}{"room": "chat"}, "SGETORCREATE"},
		{"stream.publish", map[string]interface{}{"room": "chat", "event": "e", "data": "{}"}, "SPUBLISH"},
		{"stream.consume", map[string]interface{}{"room": "chat", "subscriber_id": "s"}, "SREAD"},
		{"stream.list", map[string]interface{}{}, "SLIST"},
		{"stream.stats", map[string]interface{}{"room": "chat"}, "SSTATS"},
		{"stream.delete", map[string]interface{}{"room": "chat"}, "SDELETE"},
	}

	for _, tc := range cases {
		raw, _, err := mapCommandToWire(tc.command, tc.payload, TransportSynapRPC)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.command, err)
		}
		if raw != tc.wantRaw {
			t.Errorf("%s mapped to %q, want %q", tc.command, raw, tc.wantRaw)
		}
	}
}

func TestStreamConsumeCarriesRoomSubscriberOffsetAndLimit(t *testing.T) {
	raw, args, err := mapCommandToWire("stream.consume", map[string]interface{}{
		"room":          "chat",
		"subscriber_id": "sub-1",
		"from_offset":   float64(7),
		"limit":         float64(25),
	}, TransportSynapRPC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if raw != "SREAD" {
		t.Fatalf("raw command is %q, want SREAD", raw)
	}
	want := []interface{}{"chat", "sub-1", 7, 25}
	if len(args) != len(want) {
		t.Fatalf("got %d args (%v), want %d", len(args), args, len(want))
	}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("arg %d is %v, want %v", i, args[i], want[i])
		}
	}
}

// SREAD replies with a bare array of events; the stream module decodes an
// object with an "events" key, so the array has to be wrapped on the way back.
func TestStreamConsumeResponseWrapsTheEventArray(t *testing.T) {
	events := []interface{}{
		map[string]interface{}{"offset": int64(0), "event": "a"},
		map[string]interface{}{"offset": int64(1), "event": "b"},
	}

	mapped, ok := mapResponseFromWire("stream.consume", events).(map[string]interface{})
	if !ok {
		t.Fatalf("expected a map, got %T", mapResponseFromWire("stream.consume", events))
	}
	got, ok := mapped["events"].([]interface{})
	if !ok {
		t.Fatalf("expected an events array, got %T", mapped["events"])
	}
	if len(got) != 2 {
		t.Errorf("got %d events, want 2", len(got))
	}

	// An empty room replies with an empty array, never a nil events field.
	empty := mapResponseFromWire("stream.consume", nil).(map[string]interface{})
	if evs, ok := empty["events"].([]interface{}); !ok || len(evs) != 0 {
		t.Errorf("empty consume produced %v, want an empty events array", empty["events"])
	}
}

func TestStreamGetOrCreateResponseReadsTheCreatedFlag(t *testing.T) {
	created := mapResponseFromWire("stream.get_or_create", "CREATED").(map[string]interface{})
	if created["created"] != true {
		t.Errorf("CREATED mapped to %v, want created=true", created["created"])
	}

	existing := mapResponseFromWire("stream.get_or_create", "EXISTS").(map[string]interface{})
	if existing["created"] != false {
		t.Errorf("EXISTS mapped to %v, want created=false", existing["created"])
	}
}

func TestStreamListResponseWrapsTheRoomArray(t *testing.T) {
	mapped := mapResponseFromWire("stream.list", []interface{}{"a", "b"}).(map[string]interface{})
	rooms, ok := mapped["rooms"].([]interface{})
	if !ok || len(rooms) != 2 {
		t.Fatalf("got %v, want a two-element rooms array", mapped["rooms"])
	}
}

func TestStreamPublishResponseCarriesTheOffset(t *testing.T) {
	mapped := mapResponseFromWire("stream.publish", int64(42)).(map[string]interface{})
	if mapped["offset"] != int64(42) {
		t.Errorf("offset is %v, want 42", mapped["offset"])
	}
}

// The two native wires name topic listing differently: SynapRPC dispatches
// TOPICS and has no PUBSUB, RESP3 dispatches PUBSUB CHANNELS and has no TOPICS
// — so one mapping cannot serve both, and listing topics over RESP3 came back
// "unknown command 'TOPICS'".
func TestTopicListingFollowsTheTransport(t *testing.T) {
	raw, args, err := mapCommandToWire("pubsub.topics", map[string]interface{}{}, TransportSynapRPC)
	if err != nil {
		t.Fatalf("SynapRPC: unexpected error: %v", err)
	}
	if raw != "TOPICS" || len(args) != 0 {
		t.Errorf("SynapRPC mapped to %q %v, want TOPICS with no args", raw, args)
	}

	raw, args, err = mapCommandToWire("pubsub.topics", map[string]interface{}{}, TransportRESP3)
	if err != nil {
		t.Fatalf("RESP3: unexpected error: %v", err)
	}
	if raw != "PUBSUB" || len(args) != 1 || args[0] != "CHANNELS" {
		t.Errorf("RESP3 mapped to %q %v, want PUBSUB CHANNELS", raw, args)
	}
}

// ── Transactions (ADR 005) ───────────────────────────────────────────────────

func TestTransactionControlCommandsMapToTheirWireNames(t *testing.T) {
	cases := []struct {
		command string
		wantRaw string
	}{
		{"transaction.multi", "MULTI"},
		{"transaction.exec", "EXEC"},
		{"transaction.discard", "DISCARD"},
		{"transaction.unwatch", "UNWATCH"},
	}

	for _, tc := range cases {
		raw, args, err := mapCommandToWire(tc.command, map[string]interface{}{"client_id": "cli-1"}, TransportSynapRPC)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.command, err)
		}
		if raw != tc.wantRaw {
			t.Errorf("%s mapped to %q, want %q", tc.command, raw, tc.wantRaw)
		}
		// The client id is the command's own first argument here, never a
		// TXQUEUE wrapper.
		if len(args) == 0 || args[0] != "cli-1" {
			t.Errorf("%s args are %v, want the client id first", tc.command, args)
		}
	}
}

func TestTransactionWatchCarriesClientIdThenKeys(t *testing.T) {
	raw, args, err := mapCommandToWire("transaction.watch", map[string]interface{}{
		"client_id": "cli-1",
		"keys":      []interface{}{"a", "b"},
	}, TransportSynapRPC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if raw != "WATCH" {
		t.Fatalf("raw command is %q, want WATCH", raw)
	}
	want := []interface{}{"cli-1", "a", "b"}
	if len(args) != len(want) {
		t.Fatalf("got %v, want %v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("arg %d is %v, want %v", i, args[i], want[i])
		}
	}
}

// A write inside an open MULTI must be queued by the server, not executed, so
// it travels as TXQUEUE <client_id> <CMD> <args...> (ADR 005).
func TestWriteWithClientIdIsQueuedThroughTxqueue(t *testing.T) {
	raw, args, err := mapCommandToWire("kv.set", map[string]interface{}{
		"key":       "k",
		"value":     "v",
		"client_id": "cli-1",
	}, TransportSynapRPC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if raw != "TXQUEUE" {
		t.Fatalf("raw command is %q, want TXQUEUE", raw)
	}
	want := []interface{}{"cli-1", "SET", "k", "v"}
	if len(args) != len(want) {
		t.Fatalf("got %v, want %v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("arg %d is %v, want %v", i, args[i], want[i])
		}
	}
}

func TestCounterAmountMapsToIncrByAndDecrBy(t *testing.T) {
	// The HTTP envelope applies every `amount`, zero and negative included;
	// the native map used to send INCRBY only for amounts above 1.
	cases := []struct {
		command string
		payload map[string]interface{}
		wantCmd string
		wantArg interface{}
	}{
		{"kv.incr", map[string]interface{}{"key": "c"}, "INCR", nil},
		{"kv.decr", map[string]interface{}{"key": "c", "amount": nil}, "DECR", nil},
		{"kv.incr", map[string]interface{}{"key": "c", "amount": 10}, "INCRBY", int64(10)},
		{"kv.incr", map[string]interface{}{"key": "c", "amount": float64(0)}, "INCRBY", "0"},
		{"kv.incr", map[string]interface{}{"key": "c", "amount": float64(-7)}, "INCRBY", "-7"},
		{"kv.decr", map[string]interface{}{"key": "c", "amount": float64(4)}, "DECRBY", "4"},
		// Non-integers travel as JSON text, which the server refuses.
		{"kv.incr", map[string]interface{}{"key": "c", "amount": 1.5}, "INCRBY", "1.5"},
		{"kv.incr", map[string]interface{}{"key": "c", "amount": "10"}, "INCRBY", `"10"`},
	}

	for _, tc := range cases {
		raw, args, err := mapCommandToWire(tc.command, tc.payload, TransportSynapRPC)
		if err != nil {
			t.Fatalf("%s %v: unexpected error: %v", tc.command, tc.payload, err)
		}
		if raw != tc.wantCmd {
			t.Errorf("%s %v mapped to %q, want %q", tc.command, tc.payload, raw, tc.wantCmd)
			continue
		}
		if tc.wantArg == nil {
			if len(args) != 1 || args[0] != "c" {
				t.Errorf("%s %v: args %v, want [c]", tc.command, tc.payload, args)
			}
			continue
		}
		if len(args) != 2 || args[0] != "c" || args[1] != tc.wantArg {
			t.Errorf("%s %v: args %#v, want [c %#v]", tc.command, tc.payload, args, tc.wantArg)
		}
	}
}

func TestEveryQueueableWriteFamilyIsWrapped(t *testing.T) {
	cases := []struct {
		command   string
		payload   map[string]interface{}
		wantInner string
	}{
		{"kv.del", map[string]interface{}{"key": "k"}, "DEL"},
		{"kv.incr", map[string]interface{}{"key": "k"}, "INCR"},
		{"kv.decr", map[string]interface{}{"key": "k"}, "DECR"},
		{"hash.set", map[string]interface{}{"key": "h", "field": "f", "value": "v"}, "HSET"},
		{"list.lpush", map[string]interface{}{"key": "l", "values": []interface{}{"v"}}, "LPUSH"},
		{"set.add", map[string]interface{}{"key": "s", "members": []interface{}{"m"}}, "SADD"},
	}

	for _, tc := range cases {
		payload := map[string]interface{}{"client_id": "cli-1"}
		for k, v := range tc.payload {
			payload[k] = v
		}

		raw, args, err := mapCommandToWire(tc.command, payload, TransportSynapRPC)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.command, err)
		}
		if raw != "TXQUEUE" {
			t.Errorf("%s mapped to %q, want TXQUEUE", tc.command, raw)
			continue
		}
		if args[0] != "cli-1" || args[1] != tc.wantInner {
			t.Errorf("%s queued as %v, want [cli-1 %s ...]", tc.command, args[:2], tc.wantInner)
		}
	}
}

// A command the server cannot queue must be refused, never executed outside the
// transaction it was meant to belong to.
func TestUnqueueableCommandWithClientIdIsRefused(t *testing.T) {
	for _, command := range []string{"kv.get", "stream.publish", "queue.consume"} {
		payload := map[string]interface{}{
			"key":       "k",
			"room":      "r",
			"event":     "e",
			"queue":     "q",
			"client_id": "cli-1",
		}
		if _, _, err := mapCommandToWire(command, payload, TransportSynapRPC); err == nil {
			t.Errorf("%s inside a transaction should be refused, got no error", command)
		}
	}
}

func TestExecResponseDistinguishesCommitFromAbort(t *testing.T) {
	committed := mapResponseFromWire("transaction.exec", []interface{}{"1", "2"}).(map[string]interface{})
	if committed["success"] != true {
		t.Errorf("a result list should report success, got %v", committed)
	}
	if results, ok := committed["results"].([]interface{}); !ok || len(results) != 2 {
		t.Errorf("results are %v, want two entries", committed["results"])
	}

	// A watched key changed: the server replies null and nothing was applied.
	aborted := mapResponseFromWire("transaction.exec", nil).(map[string]interface{})
	if aborted["success"] != false || aborted["aborted"] != true {
		t.Errorf("a null reply should report an abort, got %v", aborted)
	}
}

// SSTATS replies with the stats map as-is, including the wipe discriminators
// added in Synap 1.3.1 (hivellm/synap#257).
func TestStreamStatsResponsePassesGenerationThrough(t *testing.T) {
	stats := map[string]interface{}{
		"name":        "chat",
		"created_at":  uint64(1785964676070),
		"generation":  uint64(1785964676070),
		"max_offset":  uint64(0),
		"min_offset":  uint64(0),
		"total_publi": uint64(1),
	}
	mapped, ok := mapResponseFromWire("stream.stats", stats).(map[string]interface{})
	if !ok {
		t.Fatalf("expected the stats map to pass through unchanged")
	}
	if mapped["generation"] != uint64(1785964676070) {
		t.Errorf("generation is %v, want 1785964676070", mapped["generation"])
	}
	if mapped["created_at"] != uint64(1785964676070) {
		t.Errorf("created_at is %v, want 1785964676070", mapped["created_at"])
	}
}

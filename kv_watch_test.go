package synap

import (
	"context"
	"strings"
	"testing"
)

func TestWatchEventFromEnvelopeDecodesFullEnvelope(t *testing.T) {
	event := watchEventFromEnvelope(map[string]interface{}{
		"key":     "user:1",
		"event":   "set",
		"version": float64(3),
		"value":   "alice",
	})

	if event.Key != "user:1" || event.Event != "set" {
		t.Fatalf("unexpected identity: %+v", event)
	}
	if event.Version != 3 {
		t.Fatalf("expected version 3, got %d", event.Version)
	}
	if event.Value != "alice" {
		t.Fatalf("expected value alice, got %q", event.Value)
	}
	if event.Truncated {
		t.Fatal("truncated must default to false")
	}
}

func TestWatchEventFromEnvelopeOmittedFieldsTakeDefaults(t *testing.T) {
	// A del envelope omits value and truncated entirely.
	event := watchEventFromEnvelope(map[string]interface{}{
		"key":     "k",
		"event":   "del",
		"version": float64(7),
	})

	if event.Value != "" {
		t.Fatalf("a del envelope has no value, got %q", event.Value)
	}
	if event.Truncated {
		t.Fatal("truncated must default to false")
	}
	if event.Version != 7 {
		t.Fatalf("expected version 7, got %d", event.Version)
	}
}

func TestWatchEventFromEnvelopeKeepsTruncatedFlag(t *testing.T) {
	event := watchEventFromEnvelope(map[string]interface{}{
		"key":       "big",
		"event":     "set",
		"version":   float64(1),
		"truncated": true,
	})

	if !event.Truncated {
		t.Fatal("the truncated flag must survive decoding")
	}
	if event.Value != "" {
		t.Fatalf("a truncated envelope has no value, got %q", event.Value)
	}
}

func TestWatchRequiresTheRpcTransport(t *testing.T) {
	// A client without the synap:// transport has rpc == nil.
	kv := &KVStore{client: &SynapClient{}}

	_, _, err := kv.Watch(context.Background(), "k")
	if err == nil {
		t.Fatal("Watch must fail without the SynapRPC transport")
	}
	if !strings.Contains(err.Error(), "synap://") {
		t.Fatalf("the error must point at the synap:// transport, got: %v", err)
	}
}

func TestWithNotifyModeSetsTheMode(t *testing.T) {
	options := watchOptions{mode: "value"}
	WithNotifyMode()(&options)

	if options.mode != "notify" {
		t.Fatalf("expected notify, got %q", options.mode)
	}
}

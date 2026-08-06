package synap_test

import (
	"context"
	"encoding/json"
	"testing"
)

// A transaction is keyed server-side by a client id, so every command in the
// scope — the control commands and the queued writes alike — has to carry the
// same id. Losing it on any single command silently applies that write outside
// the transaction.
func TestTransaction_ThreadsOneClientIDThroughEveryCommand(t *testing.T) {
	type seen struct {
		command  string
		clientID string
	}
	var calls []seen

	srv := mockServerFn(t, func(cmd string, payload json.RawMessage) string {
		var p struct {
			ClientID string `json:"client_id"`
		}
		_ = json.Unmarshal(payload, &p)
		calls = append(calls, seen{command: cmd, clientID: p.ClientID})

		if cmd == "transaction.exec" {
			return `{"success":true,"request_id":"x","payload":{"success":true,"results":["OK","OK"]}}`
		}
		return `{"success":true,"request_id":"x","payload":{"success":true}}`
	})
	defer srv.Close()

	ctx := context.Background()
	client := newTestClient(srv.URL)

	tx, err := client.Transaction().Multi(ctx)
	if err != nil {
		t.Fatalf("multi: %v", err)
	}
	if tx.ClientID() == "" {
		t.Fatal("the transaction has no client id")
	}
	if err := tx.KV().Set(ctx, "a", "1", 0); err != nil {
		t.Fatalf("queued set: %v", err)
	}
	if err := tx.KV().Set(ctx, "b", "2", 0); err != nil {
		t.Fatalf("queued set: %v", err)
	}

	result, err := tx.Exec(ctx)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if !result.Success || result.Aborted {
		t.Errorf("expected a committed transaction, got %+v", result)
	}
	if len(result.Results) != 2 {
		t.Errorf("got %d results, want 2", len(result.Results))
	}

	wantCommands := []string{"transaction.multi", "kv.set", "kv.set", "transaction.exec"}
	if len(calls) != len(wantCommands) {
		t.Fatalf("server saw %v, want %v", calls, wantCommands)
	}
	for i, want := range wantCommands {
		if calls[i].command != want {
			t.Errorf("call %d is %s, want %s", i, calls[i].command, want)
		}
		if calls[i].clientID != tx.ClientID() {
			t.Errorf("call %d (%s) carried client_id %q, want %q",
				i, calls[i].command, calls[i].clientID, tx.ClientID())
		}
	}
}

// The plain client must stay outside the transaction: a write issued through it
// while a Tx is open carries no client id and is applied immediately.
func TestTransaction_ScopeDoesNotLeakOntoTheParentClient(t *testing.T) {
	var lastClientID string
	srv := mockServerFn(t, func(cmd string, payload json.RawMessage) string {
		var p struct {
			ClientID string `json:"client_id"`
		}
		_ = json.Unmarshal(payload, &p)
		if cmd == "kv.set" {
			lastClientID = p.ClientID
		}
		return `{"success":true,"request_id":"x","payload":{"success":true}}`
	})
	defer srv.Close()

	ctx := context.Background()
	client := newTestClient(srv.URL)

	tx, err := client.Transaction().Multi(ctx)
	if err != nil {
		t.Fatalf("multi: %v", err)
	}
	if err := tx.KV().Set(ctx, "inside", "1", 0); err != nil {
		t.Fatalf("queued set: %v", err)
	}
	if lastClientID != tx.ClientID() {
		t.Fatalf("a scoped write carried %q, want the transaction id", lastClientID)
	}

	if err := client.KV().Set(ctx, "outside", "1", 0); err != nil {
		t.Fatalf("plain set: %v", err)
	}
	if lastClientID != "" {
		t.Errorf("a write through the plain client carried client_id %q, want none", lastClientID)
	}
}

// A watched key that changed makes the server reply null: nothing was applied,
// and the caller has to be able to tell that apart from an empty commit.
func TestTransaction_ExecReportsAnAbort(t *testing.T) {
	srv := mockServerFn(t, func(cmd string, _ json.RawMessage) string {
		if cmd == "transaction.exec" {
			return `{"success":true,"request_id":"x","payload":{"success":false,"aborted":true}}`
		}
		return `{"success":true,"request_id":"x","payload":{"success":true}}`
	})
	defer srv.Close()

	ctx := context.Background()
	tx, err := newTestClient(srv.URL).Transaction().Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Watch(ctx, "guarded"); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if err := tx.Multi(ctx); err != nil {
		t.Fatalf("multi: %v", err)
	}

	result, err := tx.Exec(ctx)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if result.Success || !result.Aborted {
		t.Errorf("expected an aborted transaction, got %+v", result)
	}
}

func TestTransaction_WatchRequiresAtLeastOneKey(t *testing.T) {
	srv := mockServerFn(t, func(string, json.RawMessage) string {
		t.Error("Watch with no keys must not reach the server")
		return `{"success":true,"request_id":"x","payload":{}}`
	})
	defer srv.Close()

	tx, err := newTestClient(srv.URL).Transaction().Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Watch(context.Background()); err == nil {
		t.Error("expected an error for a keyless Watch")
	}
}

// Resume rebinds a transaction opened elsewhere by its id.
func TestTransaction_ResumeUsesTheGivenClientID(t *testing.T) {
	var got string
	srv := mockServerFn(t, func(cmd string, payload json.RawMessage) string {
		var p struct {
			ClientID string `json:"client_id"`
		}
		_ = json.Unmarshal(payload, &p)
		got = p.ClientID
		return `{"success":true,"request_id":"x","payload":{"success":true}}`
	})
	defer srv.Close()

	tx := newTestClient(srv.URL).Transaction().Resume("cli-from-elsewhere")
	if err := tx.Discard(context.Background()); err != nil {
		t.Fatalf("discard: %v", err)
	}
	if got != "cli-from-elsewhere" {
		t.Errorf("server saw client_id %q, want cli-from-elsewhere", got)
	}
}

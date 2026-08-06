//go:build integration

// Real integration tests against a running Synap server.
// Run with: go test -tags integration -count=1 -v ./...
//
// Requires:
//   - Synap server on http://127.0.0.1:15500 (HTTP)
//   - Synap server on 127.0.0.1:15501 (SynapRPC)
//   - Synap server on 127.0.0.1:6379 (RESP3)

package synap_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	synap "github.com/hivellm/synap-sdk-go"
)

var transports = []struct {
	name string
	url  string
}{
	{"HTTP", "http://127.0.0.1:15500"},
	{"SynapRPC", "synap://127.0.0.1:15501"},
	{"RESP3", "resp3://127.0.0.1:6379"},
}

func TestIntegration_KV(t *testing.T) {
	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			cfg := synap.NewConfig(tr.url).WithTimeout(5 * time.Second)
			client := synap.NewClient(cfg)
			ctx := context.Background()
			prefix := fmt.Sprintf("go-test-%s", tr.name)

			// SET
			key := prefix + ":kv"
			err := client.KV().Set(ctx, key, "hello", 0)
			if err != nil {
				t.Fatalf("SET: %v", err)
			}

			// GET
			val, err := client.KV().Get(ctx, key)
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			if val != "hello" {
				t.Fatalf("GET expected 'hello', got '%s'", val)
			}

			// EXISTS
			exists, err := client.KV().Exists(ctx, key)
			if err != nil {
				t.Fatalf("EXISTS: %v", err)
			}
			if !exists {
				t.Fatal("EXISTS expected true")
			}

			// DEL
			deleted, err := client.KV().Delete(ctx, key)
			if err != nil {
				t.Fatalf("DEL: %v", err)
			}
			if !deleted {
				t.Fatal("DEL expected true")
			}

			// GET after DEL
			val2, err := client.KV().Get(ctx, key)
			if err != nil {
				t.Fatalf("GET after DEL: %v", err)
			}
			if val2 != "" {
				t.Fatalf("GET after DEL expected empty, got '%s'", val2)
			}

			// INCR
			counterKey := prefix + ":counter"
			_ = client.KV().Set(ctx, counterKey, "0", 0)
			v1, err := client.KV().Incr(ctx, counterKey)
			if err != nil {
				t.Fatalf("INCR: %v", err)
			}
			if v1 != 1 {
				t.Fatalf("INCR expected 1, got %d", v1)
			}
			v2, _ := client.KV().Incr(ctx, counterKey)
			if v2 != 2 {
				t.Fatalf("INCR expected 2, got %d", v2)
			}
			_, _ = client.KV().Delete(ctx, counterKey)
		})
	}
}

func TestIntegration_Hash(t *testing.T) {
	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			cfg := synap.NewConfig(tr.url).WithTimeout(5 * time.Second)
			client := synap.NewClient(cfg)
			ctx := context.Background()
			key := fmt.Sprintf("go-test-%s:hash", tr.name)

			// HSET
			created, err := client.Hash().Set(ctx, key, "name", "synap")
			if err != nil {
				t.Fatalf("HSET: %v", err)
			}
			_ = created

			// HGET
			val, err := client.Hash().Get(ctx, key, "name")
			if err != nil {
				t.Fatalf("HGET: %v", err)
			}
			if val != "synap" {
				t.Fatalf("HGET expected 'synap', got '%s'", val)
			}

			// HEXISTS
			exists, err := client.Hash().Exists(ctx, key, "name")
			if err != nil {
				t.Fatalf("HEXISTS: %v", err)
			}
			if !exists {
				t.Fatal("HEXISTS expected true")
			}

			// HDEL
			_, err = client.Hash().Del(ctx, key, "name")
			if err != nil {
				t.Fatalf("HDEL: %v", err)
			}
		})
	}
}

func TestIntegration_List(t *testing.T) {
	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			cfg := synap.NewConfig(tr.url).WithTimeout(5 * time.Second)
			client := synap.NewClient(cfg)
			ctx := context.Background()
			key := fmt.Sprintf("go-test-%s:list", tr.name)

			// Cleanup
			for {
				items, _ := client.List().LPop(ctx, key, 100)
				if len(items) == 0 {
					break
				}
			}

			// LPUSH
			length, err := client.List().LPush(ctx, key, []string{"c", "b", "a"})
			if err != nil {
				t.Fatalf("LPUSH: %v", err)
			}
			if length != 3 {
				t.Fatalf("LPUSH expected 3, got %d", length)
			}

			// LRANGE
			items, err := client.List().Range(ctx, key, 0, -1)
			if err != nil {
				t.Fatalf("LRANGE: %v", err)
			}
			if len(items) != 3 {
				t.Fatalf("LRANGE expected 3 items, got %d", len(items))
			}

			// LLEN
			l, err := client.List().Len(ctx, key)
			if err != nil {
				t.Fatalf("LLEN: %v", err)
			}
			if l != 3 {
				t.Fatalf("LLEN expected 3, got %d", l)
			}

			// Cleanup
			client.List().LPop(ctx, key, 100)
		})
	}
}

func TestIntegration_Set(t *testing.T) {
	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			cfg := synap.NewConfig(tr.url).WithTimeout(5 * time.Second)
			client := synap.NewClient(cfg)
			ctx := context.Background()
			key := fmt.Sprintf("go-test-%s:set", tr.name)

			// Cleanup from previous run
			_, _ = client.Set().Remove(ctx, key, []string{"a", "b", "c"})

			// SADD
			added, err := client.Set().Add(ctx, key, []string{"a", "b", "c"})
			if err != nil {
				t.Fatalf("SADD: %v", err)
			}
			if added < 1 {
				t.Fatalf("SADD expected >= 1, got %d", added)
			}

			// SCARD
			card, err := client.Set().Card(ctx, key)
			if err != nil {
				t.Fatalf("SCARD: %v", err)
			}
			if card != 3 {
				t.Fatalf("SCARD expected 3, got %d", card)
			}

			// SISMEMBER
			isMember, err := client.Set().IsMember(ctx, key, "a")
			if err != nil {
				t.Fatalf("SISMEMBER: %v", err)
			}
			if !isMember {
				t.Fatal("SISMEMBER expected true")
			}

			// SMEMBERS
			members, err := client.Set().Members(ctx, key)
			if err != nil {
				t.Fatalf("SMEMBERS: %v", err)
			}
			if len(members) != 3 {
				t.Fatalf("SMEMBERS expected 3, got %d", len(members))
			}

			// SREM
			removed, err := client.Set().Remove(ctx, key, []string{"a", "b", "c"})
			if err != nil {
				t.Fatalf("SREM: %v", err)
			}
			if removed != 3 {
				t.Fatalf("SREM expected 3, got %d", removed)
			}
		})
	}
}

func TestIntegration_Queue_HTTP(t *testing.T) {
	// Queue operations only on HTTP (QCREATE/QPUBLISH/QCONSUME are Synap-specific)
	cfg := synap.NewConfig("http://127.0.0.1:15500").WithTimeout(5 * time.Second)
	client := synap.NewClient(cfg)
	ctx := context.Background()
	qname := "go-test-queue"

	// Cleanup
	_ = client.Queue().Delete(ctx, qname)

	// Create
	err := client.Queue().Create(ctx, qname, 1000, 30)
	if err != nil {
		t.Fatalf("Queue Create: %v", err)
	}

	// List
	queues, err := client.Queue().List(ctx)
	if err != nil {
		t.Fatalf("Queue List: %v", err)
	}
	found := false
	for _, q := range queues {
		if q == qname {
			found = true
		}
	}
	if !found {
		t.Fatalf("Queue not in list after create")
	}

	// Publish
	msgID, err := client.Queue().Publish(ctx, qname, []byte("test-payload"), 5, 3)
	if err != nil {
		t.Fatalf("Queue Publish: %v", err)
	}
	if msgID == "" {
		t.Fatal("Queue Publish returned empty ID")
	}

	// Consume
	msg, err := client.Queue().Consume(ctx, qname, "go-worker")
	if err != nil {
		t.Fatalf("Queue Consume: %v", err)
	}
	if msg == nil {
		t.Fatal("Queue Consume returned nil")
	}
	if msg.ID != msgID {
		t.Fatalf("Queue Consume ID mismatch: got %s, want %s", msg.ID, msgID)
	}

	// Ack
	err = client.Queue().Ack(ctx, qname, msg.ID)
	if err != nil {
		t.Fatalf("Queue Ack: %v", err)
	}

	// Delete
	err = client.Queue().Delete(ctx, qname)
	if err != nil {
		t.Fatalf("Queue Delete: %v", err)
	}
}

func TestIntegration_MultipleHTTPClients(t *testing.T) {
	client1 := synap.NewClient(synap.NewConfig("http://127.0.0.1:15500"))
	client2 := synap.NewClient(synap.NewConfig("http://127.0.0.1:15500"))
	ctx := context.Background()
	key := "go-multi-client-test"

	// Write via client1, read via client2
	_ = client1.KV().Set(ctx, key, "shared-value", 0)
	val, _ := client2.KV().Get(ctx, key)
	if val != "shared-value" {
		t.Fatalf("client2 sees '%s', expected 'shared-value'", val)
	}

	// Cleanup
	_, _ = client1.KV().Delete(ctx, key)
}

// A transaction must apply every queued write at EXEC and none before it, on
// each transport: the writes travel as TXQUEUE on the native wires and as a
// client_id-carrying payload over HTTP, so a regression on one wire is
// invisible from the others.
func TestIntegration_Transaction(t *testing.T) {
	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			cfg := synap.NewConfig(tr.url).WithTimeout(5 * time.Second)
			client := synap.NewClient(cfg)
			ctx := context.Background()
			prefix := fmt.Sprintf("go-tx-%s", tr.name)
			keyA, keyB := prefix+":a", prefix+":b"

			// A reader outside the transaction proves the writes are queued
			// rather than applied as they are issued.
			reader := synap.NewClient(synap.NewConfig("http://127.0.0.1:15500"))
			_, _ = reader.KV().Delete(ctx, keyA)
			_, _ = reader.KV().Delete(ctx, keyB)

			tx, err := client.Transaction().Multi(ctx)
			if err != nil {
				t.Fatalf("Multi: %v", err)
			}
			if err := tx.KV().Set(ctx, keyA, "1", 0); err != nil {
				t.Fatalf("queued Set: %v", err)
			}
			if err := tx.KV().Set(ctx, keyB, "2", 0); err != nil {
				t.Fatalf("queued Set: %v", err)
			}

			if v, _ := reader.KV().Get(ctx, keyA); v != "" {
				t.Fatalf("key was written before EXEC (value %q)", v)
			}

			result, err := tx.Exec(ctx)
			if err != nil {
				t.Fatalf("Exec: %v", err)
			}
			if !result.Success || result.Aborted {
				t.Fatalf("transaction did not commit: %+v", result)
			}

			if v, _ := reader.KV().Get(ctx, keyA); v != "1" {
				t.Errorf("after EXEC keyA is %q, want 1", v)
			}
			if v, _ := reader.KV().Get(ctx, keyB); v != "2" {
				t.Errorf("after EXEC keyB is %q, want 2", v)
			}

			_, _ = reader.KV().Delete(ctx, keyA)
			_, _ = reader.KV().Delete(ctx, keyB)
		})
	}
}

// DISCARD drops the queued writes: nothing reaches the store.
func TestIntegration_TransactionDiscard(t *testing.T) {
	client := synap.NewClient(synap.NewConfig("http://127.0.0.1:15500"))
	ctx := context.Background()
	key := "go-tx-discard:a"

	_, _ = client.KV().Delete(ctx, key)

	tx, err := client.Transaction().Multi(ctx)
	if err != nil {
		t.Fatalf("Multi: %v", err)
	}
	if err := tx.KV().Set(ctx, key, "queued", 0); err != nil {
		t.Fatalf("queued Set: %v", err)
	}
	if err := tx.Discard(ctx); err != nil {
		t.Fatalf("Discard: %v", err)
	}

	if v, _ := client.KV().Get(ctx, key); v != "" {
		t.Errorf("after DISCARD the key holds %q, want nothing", v)
	}
}

// Streams over every transport: this is the path that was broken, because
// stream.consume mapped to a command the server does not have, so a native
// consume failed while HTTP kept working.
func TestIntegration_Stream(t *testing.T) {
	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			cfg := synap.NewConfig(tr.url).WithTimeout(5 * time.Second)
			client := synap.NewClient(cfg)
			ctx := context.Background()
			room := fmt.Sprintf("go-stream-%s", tr.name)

			_ = client.Stream().Delete(ctx, room)

			created, err := client.Stream().GetOrCreate(ctx, room, 0)
			if err != nil {
				t.Fatalf("GetOrCreate: %v", err)
			}
			if !created {
				t.Errorf("a freshly deleted room should report created=true")
			}

			offset, err := client.Stream().Publish(ctx, room, "tick", map[string]int{"n": 1})
			if err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if offset != 0 {
				t.Errorf("first publish got offset %d, want 0", offset)
			}

			events, err := client.Stream().Consume(ctx, room, 0, 10)
			if err != nil {
				t.Fatalf("Consume: %v", err)
			}
			if len(events) != 1 {
				t.Fatalf("got %d events, want 1", len(events))
			}
			if events[0].Event != "tick" {
				t.Errorf("event name is %q, want tick", events[0].Event)
			}

			stats, err := client.Stream().Stats(ctx, room)
			if err != nil {
				t.Fatalf("Stats: %v", err)
			}
			if stats.Generation == 0 || stats.CreatedAt == 0 {
				t.Errorf("stats carry no wipe discriminator: %+v", stats)
			}

			// Recreating the room must move the generation even though every
			// counter resets to the same values (hivellm/synap#257).
			before := stats.Generation
			if err := client.Stream().Delete(ctx, room); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if _, err := client.Stream().GetOrCreate(ctx, room, 0); err != nil {
				t.Fatalf("GetOrCreate after delete: %v", err)
			}
			if _, err := client.Stream().Publish(ctx, room, "tick", map[string]int{"n": 1}); err != nil {
				t.Fatalf("Publish after recreate: %v", err)
			}
			after, err := client.Stream().Stats(ctx, room)
			if err != nil {
				t.Fatalf("Stats after recreate: %v", err)
			}
			if after.MessageCount != stats.MessageCount {
				t.Fatalf("the coincidence this test relies on no longer holds: %d vs %d",
					after.MessageCount, stats.MessageCount)
			}
			if after.Generation <= before {
				t.Errorf("generation %d did not move past %d after a wipe", after.Generation, before)
			}

			_ = client.Stream().Delete(ctx, room)
		})
	}
}

// Pub/Sub over every transport. Topic listing is the interesting one: the two
// native wires spell it differently, so a single mapping breaks one of them.
func TestIntegration_PubSub(t *testing.T) {
	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			cfg := synap.NewConfig(tr.url).WithTimeout(5 * time.Second)
			client := synap.NewClient(cfg)
			ctx := context.Background()
			topic := fmt.Sprintf("go.pubsub.%s", tr.name)

			if _, err := client.PubSub().Publish(ctx, topic, map[string]int{"n": 1}, 0); err != nil {
				t.Fatalf("Publish: %v", err)
			}

			if _, err := client.PubSub().ListTopics(ctx); err != nil {
				t.Fatalf("ListTopics: %v", err)
			}
		})
	}
}

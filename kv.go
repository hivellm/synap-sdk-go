package synap

import (
	"context"
	"sync"
	"time"
)

// KVStore provides key-value store operations against the Synap server.
type KVStore struct {
	client *SynapClient
}

// Set stores a key-value pair. ttl is the optional expiry duration; pass 0 for
// no expiry. The server expects a TTL expressed in seconds (uint64).
func (k *KVStore) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	type payload struct {
		Key   string  `json:"key"`
		Value string  `json:"value"`
		TTL   *uint64 `json:"ttl"`
	}
	p := payload{Key: key, Value: value}
	if ttl > 0 {
		secs := uint64(ttl.Seconds())
		p.TTL = &secs
	}
	_, err := k.client.sendCommand(ctx, "kv.set", p)
	return err
}

// Get retrieves the value for key. Returns ("", nil) when the key does not
// exist (server returns a null payload).
func (k *KVStore) Get(ctx context.Context, key string) (string, error) {
	type payload struct {
		Key string `json:"key"`
	}
	raw, err := k.client.sendCommand(ctx, "kv.get", payload{Key: key})
	if err != nil {
		return "", err
	}
	// Server returns null when key is absent.
	if raw.IsNull() {
		return "", nil
	}
	var value string
	if err := raw.Decode(&value); err != nil {
		return "", newInvalidResponseError("kv.get: " + err.Error())
	}
	return value, nil
}

// Delete removes a key. Returns true if the key existed and was deleted.
func (k *KVStore) Delete(ctx context.Context, key string) (bool, error) {
	type payload struct {
		Key string `json:"key"`
	}
	raw, err := k.client.sendCommand(ctx, "kv.del", payload{Key: key})
	if err != nil {
		return false, err
	}
	var result struct {
		Deleted bool `json:"deleted"`
	}
	if err := raw.Decode(&result); err != nil {
		return false, newInvalidResponseError("kv.del: " + err.Error())
	}
	return result.Deleted, nil
}

// Exists reports whether key is present in the store.
func (k *KVStore) Exists(ctx context.Context, key string) (bool, error) {
	type payload struct {
		Key string `json:"key"`
	}
	raw, err := k.client.sendCommand(ctx, "kv.exists", payload{Key: key})
	if err != nil {
		return false, err
	}
	var result struct {
		Exists bool `json:"exists"`
	}
	if err := raw.Decode(&result); err != nil {
		return false, newInvalidResponseError("kv.exists: " + err.Error())
	}
	return result.Exists, nil
}

// Incr atomically increments the integer value stored at key by one and
// returns the new value.
func (k *KVStore) Incr(ctx context.Context, key string) (int64, error) {
	type payload struct {
		Key string `json:"key"`
	}
	raw, err := k.client.sendCommand(ctx, "kv.incr", payload{Key: key})
	if err != nil {
		return 0, err
	}
	var result struct {
		Value int64 `json:"value"`
	}
	if err := raw.Decode(&result); err != nil {
		return 0, newInvalidResponseError("kv.incr: " + err.Error())
	}
	return result.Value, nil
}

// Decr atomically decrements the integer value stored at key by one and
// returns the new value.
func (k *KVStore) Decr(ctx context.Context, key string) (int64, error) {
	type payload struct {
		Key string `json:"key"`
	}
	raw, err := k.client.sendCommand(ctx, "kv.decr", payload{Key: key})
	if err != nil {
		return 0, err
	}
	var result struct {
		Value int64 `json:"value"`
	}
	if err := raw.Decode(&result); err != nil {
		return 0, newInvalidResponseError("kv.decr: " + err.Error())
	}
	return result.Value, nil
}

// Stats returns aggregate statistics for the KV store.
func (k *KVStore) Stats(ctx context.Context) (KVStats, error) {
	raw, err := k.client.sendCommand(ctx, "kv.stats", struct{}{})
	if err != nil {
		return KVStats{}, err
	}
	var stats KVStats
	if err := raw.Decode(&stats); err != nil {
		return KVStats{}, newInvalidResponseError("kv.stats: " + err.Error())
	}
	return stats, nil
}

// WatchEvent is one KV watch envelope (docs/features/kv-watch.md in the server
// repository).
//
// Value is the post-mutation value and is empty for terminal events (del,
// expired, evicted), TTL-only events (expire, persist), and envelopes degraded
// to notify-only (Truncated is true).
type WatchEvent struct {
	// Key is the key that changed.
	Key string `json:"key"`
	// Event is what happened: set, del, expired, evicted, expire, persist, ...
	Event string `json:"event"`
	// Version is a per-key counter for gap detection. It resets when the key
	// is deleted, expires or is evicted — version 1 marks a new incarnation.
	Version uint64 `json:"version"`
	// Value is the post-mutation value, when inlined.
	Value string `json:"value"`
	// Truncated is true when the value was withheld (over the inline cap, or
	// not UTF-8).
	Truncated bool `json:"truncated"`
}

// WatchOption configures a Watch subscription.
type WatchOption func(*watchOptions)

type watchOptions struct {
	mode string
}

// WithNotifyMode asks the server to strip values per subscription: envelopes
// carry key/event/version only, so a watcher that only wants change signals
// pays no value bandwidth.
func WithNotifyMode() WatchOption {
	return func(o *watchOptions) { o.mode = "notify" }
}

// Watch opens a dedicated push subscription driven by KV.WATCH and streams
// change events for the given key or wildcard pattern (e.g. "user:*").
//
// SynapRPC only (`synap://` URL). Delivery is best-effort, latest-value: a
// watcher that cannot keep up is disconnected by the server and must re-Get
// and re-watch; use WatchEvent.Version to detect gaps. Cancel the context, or
// call the returned stop, to end the subscription — teardown issues KV.UNWATCH
// and closes the channel.
func (k *KVStore) Watch(
	ctx context.Context,
	pattern string,
	opts ...WatchOption,
) (events <-chan WatchEvent, stop func(), err error) {
	if k.client.rpc == nil {
		return nil, nil, newServerError(
			"kv watch requires the synap:// transport; " +
				"over HTTP use the /kv/ws WebSocket endpoint")
	}

	options := watchOptions{mode: "value"}
	for _, opt := range opts {
		opt(&options)
	}

	// Buffered so a slow consumer cannot block the connection's reader.
	out := make(chan WatchEvent, 64)
	var once sync.Once

	_, cancel, err := k.client.rpc.WatchPush(ctx, pattern, options.mode,
		func(envelope map[string]interface{}) {
			select {
			case out <- watchEventFromEnvelope(envelope):
			default:
				// Dropping is better than stalling the shared reader; the
				// server-side slow-consumer policy is the authority.
			}
		})
	if err != nil {
		return nil, nil, err
	}

	shutdown := func() {
		once.Do(func() {
			cancel()
			close(out)
		})
	}

	// Cancelling the caller's context tears the subscription down too.
	go func() {
		<-ctx.Done()
		shutdown()
	}()

	return out, shutdown, nil
}

// watchEventFromEnvelope decodes one watch envelope map; omitted optional
// fields take their zero values.
func watchEventFromEnvelope(envelope map[string]interface{}) WatchEvent {
	event := WatchEvent{
		Key:   asString(envelope["key"]),
		Event: asString(envelope["event"]),
		Value: asString(envelope["value"]),
	}
	if version, ok := envelope["version"].(float64); ok {
		event.Version = uint64(version)
	}
	if truncated, ok := envelope["truncated"].(bool); ok {
		event.Truncated = truncated
	}
	return event
}

package synap

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	thunder "github.com/hivellm/thunder-go/client"
	"github.com/hivellm/thunder-go/wire"
)

// MaxFrameBytes is the frame-body cap, matching the server's
// `protocol::synap_rpc::config::MAX_FRAME_BYTES`. Thunder validates it against
// the length prefix before allocating the body, so a hostile peer cannot drive
// an unbounded allocation from four bytes.
const MaxFrameBytes = 512 * 1024 * 1024

// DefaultRpcPort is the default SynapRPC port.
const DefaultRpcPort = 15501

// synapConfig describes how Synap uses the Thunder wire, mirroring the
// server's `synap_config()` and the other SDKs.
//
// Thunder ships one standard and zero product knowledge, so this description
// lives in Synap's own repository. Every divergence from the standard is
// deliberate: Synap authenticates with AUTH rather than a mandatory HELLO, it
// ships a push-producing command (SUBSCRIBE), its errors use the
// Redis-compatible prefixes it shares with its RESP3 port, and its frame cap is
// 512 MiB rather than 64.
func synapConfig() thunder.Config {
	c := thunder.Standard()
	c.Scheme = "synap"
	c.DefaultPort = DefaultRpcPort
	c.Handshake = thunder.HandshakeAuthCommand
	c.HelloStyle = thunder.HelloStyleNotUsed
	c.Push = thunder.PushEnabled
	c.ErrorCodes = thunder.ErrorResp3Prefixes
	c.MaxFrameBytes = MaxFrameBytes
	return c
}

// toWire converts a plain Go value to a Thunder wire value.
//
// A Go string is an arbitrary byte sequence, not necessarily UTF-8, and the
// SDK's surface is string-typed — so a string is the only way a caller can hand
// over binary. Strings that are not valid UTF-8 travel as `bin`, which
// round-trips exactly; sending them as `str` made the server decode them
// lossily.
func toWire(v interface{}) wire.Value {
	switch val := v.(type) {
	case nil:
		return wire.Null()
	case bool:
		return wire.Bool(val)
	case string:
		if !utf8.ValidString(val) {
			return wire.Bytes([]byte(val))
		}
		return wire.Str(val)
	case []byte:
		return wire.Bytes(val)
	case int:
		return wire.Int(int64(val))
	case int64:
		return wire.Int(val)
	case uint64:
		return wire.Int(int64(val))
	case float64:
		return wire.Float(val)
	case []interface{}:
		items := make([]wire.Value, len(val))
		for i, item := range val {
			items[i] = toWire(item)
		}
		return wire.Array(items)
	case map[string]interface{}:
		pairs := make([]wire.MapEntry, 0, len(val))
		for k, item := range val {
			pairs = append(pairs, wire.MapEntry{Key: wire.Str(k), Val: toWire(item)})
		}
		return wire.Map(pairs)
	default:
		return wire.Str(fmt.Sprintf("%v", val))
	}
}

// fromWire converts a Thunder wire value back to a plain Go value.
//
// `Bytes` decode to string: the SDK's surface is string-typed, and a Go string
// holds arbitrary bytes, so binary survives. Thunder handles both the canonical
// MessagePack `bin` the server emits from 1.1.0 and the legacy
// array-of-integers form, so a pre-1.1.0 server still interoperates — the dual
// tolerance this SDK used to implement by hand is now Thunder's.
func fromWire(v wire.Value) interface{} {
	switch v.Kind() {
	case wire.KindNull:
		return nil
	case wire.KindBool:
		b, _ := v.AsBool()
		return b
	case wire.KindInt:
		i, _ := v.AsInt()
		return i
	case wire.KindFloat:
		f, _ := v.AsFloat()
		return f
	case wire.KindStr:
		s, _ := v.AsStr()
		return s
	case wire.KindBytes:
		b, _ := v.AsBytes()
		return string(b)
	case wire.KindArray:
		items, _ := v.AsArray()
		out := make([]interface{}, len(items))
		for i, item := range items {
			out[i] = fromWire(item)
		}
		return out
	case wire.KindMap:
		pairs, _ := v.AsMap()
		out := make(map[string]interface{}, len(pairs))
		for _, p := range pairs {
			out[fmt.Sprintf("%v", fromWire(p.Key))] = fromWire(p.Val)
		}
		return out
	default:
		return nil
	}
}

// ── SynapRPC transport ───────────────────────────────────────────────────────

// SynapRpcTransport is a persistent connection to the SynapRPC listener,
// backed by Thunder's client.
//
// Concurrent commands multiplex over the one connection and are demultiplexed
// by frame id, so they genuinely overlap. The previous implementation held a
// mutex across each request/response round trip, so commands serialized and the
// request id it incremented was decorative.
type SynapRpcTransport struct {
	endpoint     string
	clientConfig thunder.ClientConfig

	mu     sync.Mutex
	client *thunder.Client
}

func newSynapRpcTransport(host string, port int, timeout time.Duration) *SynapRpcTransport {
	return &SynapRpcTransport{
		endpoint: fmt.Sprintf("synap://%s:%d", host, port),
		clientConfig: thunder.ClientConfig{
			ConnectTimeout: timeout,
			CallTimeout:    timeout,
			ClientName:     "synap-go-sdk",
		},
	}
}

// withCredentials attaches handshake credentials, sent as AUTH on connect —
// including the reconnects Thunder performs, so an authenticated session is not
// silently downgraded when the socket is replaced.
//
// This transport never authenticated, so it could not reach a require_auth
// deployment on 15501 at all: every command came back NOAUTH.
func (t *SynapRpcTransport) withCredentials(token, username, password string) *SynapRpcTransport {
	switch {
	case username != "" && password != "":
		t.clientConfig.Credentials = thunder.UserPassCredentials(username, password)
	case token != "":
		t.clientConfig.Credentials = thunder.APIKeyCredentials(token)
	}
	return t
}

// dial opens a fresh Thunder client against the configured endpoint.
func (t *SynapRpcTransport) dial() (*thunder.Client, error) {
	cfg := t.clientConfig
	c, err := thunder.Connect(t.endpoint, synapConfig(), &cfg)
	if err != nil {
		return nil, newServerError(fmt.Sprintf("SynapRPC connect %s: %v", t.endpoint, err))
	}
	return c, nil
}

// ensureConnected returns the shared client, dialing it on first use.
func (t *SynapRpcTransport) ensureConnected() (*thunder.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.client != nil && t.client.IsAlive() {
		return t.client, nil
	}
	if t.client != nil {
		t.client.Close()
		t.client = nil
	}
	c, err := t.dial()
	if err != nil {
		return nil, err
	}
	t.client = c
	return c, nil
}

// Execute sends a command and waits for its response.
func (t *SynapRpcTransport) Execute(ctx context.Context, cmd string, args []interface{}) (interface{}, error) {
	c, err := t.ensureConnected()
	if err != nil {
		return nil, err
	}

	wireArgs := make([]wire.Value, len(args))
	for i, a := range args {
		wireArgs[i] = toWire(a)
	}

	result, err := c.Call(ctx, cmd, wireArgs...)
	if err != nil {
		return nil, newServerError(err.Error())
	}
	return fromWire(result), nil
}

// SubscribePush opens a dedicated push connection, sends SUBSCRIBE, and
// delivers each push frame to onMessage.
//
// The push hook is registered before SUBSCRIBE is sent, so a message published
// between the server's acknowledgement and the reader starting cannot be lost.
// The returned cancel closes the dedicated connection.
func (t *SynapRpcTransport) SubscribePush(
	ctx context.Context,
	topics []string,
	onMessage func(map[string]interface{}),
) (subscriberID string, cancel func(), err error) {
	c, err := t.dial()
	if err != nil {
		return "", nil, err
	}

	c.OnPush(func(v wire.Value) {
		frame, ok := fromWire(v).(map[string]interface{})
		if !ok {
			return
		}
		onMessage(frame)
	})

	topicValues := make([]wire.Value, len(topics))
	for i, topic := range topics {
		topicValues[i] = wire.Str(topic)
	}

	ack, err := c.Call(ctx, "SUBSCRIBE", topicValues...)
	if err != nil {
		c.Close()
		return "", nil, newServerError(err.Error())
	}

	if idValue, ok := ack.MapGet("subscriber_id"); ok {
		if s, ok := idValue.AsStr(); ok {
			subscriberID = s
		}
	}

	return subscriberID, func() { c.Close() }, nil
}

// WatchPush opens a dedicated push connection driven by KV.WATCH and delivers
// each decoded watch envelope to onEvent — the watch twin of SubscribePush.
//
// The push hook is registered before KV.WATCH is sent, so an event published
// between the server's acknowledgement and the reader starting cannot be lost.
// The returned cancel issues KV.UNWATCH (best-effort) before closing the
// dedicated connection.
func (t *SynapRpcTransport) WatchPush(
	ctx context.Context,
	pattern string,
	mode string,
	onEvent func(map[string]interface{}),
) (subscriberID string, cancel func(), err error) {
	c, err := t.dial()
	if err != nil {
		return "", nil, err
	}

	c.OnPush(func(v wire.Value) {
		frame, ok := fromWire(v).(map[string]interface{})
		if !ok {
			return
		}
		// The bridge encodes the envelope as a JSON string.
		payload, ok := frame["payload"].(string)
		if !ok {
			return
		}
		var envelope map[string]interface{}
		if json.Unmarshal([]byte(payload), &envelope) != nil {
			return
		}
		onEvent(envelope)
	})

	args := []wire.Value{wire.Str(pattern)}
	if mode != "" && mode != "value" {
		args = append(args, wire.Str(mode))
	}

	ack, err := c.Call(ctx, "KV.WATCH", args...)
	if err != nil {
		c.Close()
		return "", nil, newServerError(err.Error())
	}

	if idValue, ok := ack.MapGet("subscriber_id"); ok {
		if s, ok := idValue.AsStr(); ok {
			subscriberID = s
		}
	}

	sid := subscriberID
	return subscriberID, func() {
		// Teardown issues KV.UNWATCH so the server drops the routing entry
		// promptly; closing the connection unwinds it anyway, so a failure
		// here is ignored. A fresh context: the caller's is likely cancelled.
		if sid != "" {
			unwatchCtx, done := context.WithTimeout(context.Background(), 2*time.Second)
			_, _ = c.Call(unwatchCtx, "KV.UNWATCH", wire.Str(sid))
			done()
		}
		c.Close()
	}, nil
}

// Close tears down the connection and fails anything still in flight.
func (t *SynapRpcTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.client != nil {
		t.client.Close()
		t.client = nil
	}
}

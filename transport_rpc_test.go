package synap

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hivellm/thunder-go/wire"
)

// The transport is Thunder's now, so these tests pin the contract Synap
// depends on — the protocol config, the value mapping, and the behaviour a
// caller can observe — rather than re-testing framing Thunder already covers.

func TestSynapConfigMatchesWhatTheServerDeclares(t *testing.T) {
	// The server's `synap_config()` and five SDKs hard-code these values. A
	// silent change on either side desynchronises the wire.
	c := synapConfig()

	if c.Scheme != "synap" {
		t.Errorf("scheme = %q, want synap", c.Scheme)
	}
	if c.DefaultPort != 15501 {
		t.Errorf("default port = %d, want 15501", c.DefaultPort)
	}
	if c.MaxFrameBytes != 512*1024*1024 {
		t.Errorf("frame cap = %d, want 512 MiB", c.MaxFrameBytes)
	}
	if c.Handshake.String() != "auth_command" {
		t.Errorf("handshake = %s, want auth_command", c.Handshake)
	}
	if c.HelloStyle.String() != "not_used" {
		t.Errorf("hello style = %s, want not_used", c.HelloStyle)
	}
	if c.Push.String() != "enabled" {
		t.Errorf("push = %s, want enabled", c.Push)
	}
	if c.ErrorCodes.String() != "resp3_prefixes" {
		t.Errorf("error codes = %s, want resp3_prefixes", c.ErrorCodes)
	}
}

func TestFrameCapIsNotBelowTheLegacyCap(t *testing.T) {
	// This client capped at 64 MiB while the server accepted 512 MiB, so it
	// rejected frames the server considered legitimate.
	if MaxFrameBytes < 512*1024*1024 {
		t.Fatalf("frame cap %d is below the server's 512 MiB", MaxFrameBytes)
	}
}

func TestNonUTF8StringTravelsAsBytes(t *testing.T) {
	// A Go string is an arbitrary byte sequence and the SDK's surface is
	// string-typed, so this is the only way a caller can hand over binary.
	// Sent as `str` the server decoded it lossily: deadbeef came back as
	// deadefbfbdefbfbd.
	payload := string([]byte{0xDE, 0xAD, 0xBE, 0xEF})

	got := toWire(payload)

	if got.Kind() != wire.KindBytes {
		t.Fatalf("kind = %v, want Bytes", got.Kind())
	}
	b, _ := got.AsBytes()
	if string(b) != payload {
		t.Errorf("bytes = %x, want %x", b, payload)
	}
}

func TestValidUTF8StringStaysAString(t *testing.T) {
	got := toWire("hello")

	if got.Kind() != wire.KindStr {
		t.Fatalf("kind = %v, want Str", got.Kind())
	}
	s, _ := got.AsStr()
	if s != "hello" {
		t.Errorf("str = %q, want hello", s)
	}
}

func TestBytesDecodeBackToAStringByteExact(t *testing.T) {
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	got := fromWire(wire.Bytes(payload))

	s, ok := got.(string)
	if !ok {
		t.Fatalf("got %T, want string", got)
	}
	if s != string(payload) {
		t.Errorf("decoded %x, want %x", s, payload)
	}
}

func TestFromWireCoversEveryScalarKind(t *testing.T) {
	cases := []struct {
		name string
		in   wire.Value
		want interface{}
	}{
		{"null", wire.Null(), nil},
		{"bool", wire.Bool(true), true},
		{"int", wire.Int(42), int64(42)},
		{"float", wire.Float(2.5), 2.5},
		{"str", wire.Str("PONG"), "PONG"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := fromWire(tc.in); got != tc.want {
				t.Errorf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

// ── Behaviour against a socket ───────────────────────────────────────────────

// echoServer answers every request with its first argument, so a test can
// assert what actually reached the wire.
func echoServer(t *testing.T, onRequest func(wire.Request)) (host string, port int, stop func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().(*net.TCPAddr)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveEcho(conn, onRequest)
		}
	}()

	return "127.0.0.1", addr.Port, func() { listener.Close() }
}

func serveEcho(conn net.Conn, onRequest func(wire.Request)) {
	defer conn.Close()
	for {
		header := make([]byte, 4)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		body := make([]byte, binary.LittleEndian.Uint32(header))
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}

		req, err := wire.DecodeRequestBody(body)
		if err != nil {
			return
		}
		if onRequest != nil {
			onRequest(req)
		}

		reply := wire.Str("OK")
		if len(req.Args) > 0 {
			reply = req.Args[0]
		}
		out, err := wire.EncodeFrame(wire.ResponseOK(req.ID, reply))
		if err != nil {
			return
		}
		if _, err := conn.Write(out); err != nil {
			return
		}
	}
}

func TestExecuteRoundTripsBinaryOverASocket(t *testing.T) {
	host, port, stop := echoServer(t, nil)
	defer stop()

	transport := newSynapRpcTransport(host, port, 5*time.Second)
	defer transport.Close()

	payload := string([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	got, err := transport.Execute(context.Background(), "SET", []interface{}{payload})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if got != payload {
		t.Errorf("round trip gave %x, want %x", got, payload)
	}
}

func TestConcurrentCommandsShareOneConnection(t *testing.T) {
	// The old transport held a mutex across each request/response round trip,
	// so commands serialized and the request id it incremented was decorative.
	// Thunder demultiplexes by id, so these genuinely overlap — and each
	// response must still match its own request.
	var mu sync.Mutex
	seen := map[uint32]bool{}

	host, port, stop := echoServer(t, func(req wire.Request) {
		mu.Lock()
		seen[req.ID] = true
		mu.Unlock()
	})
	defer stop()

	transport := newSynapRpcTransport(host, port, 5*time.Second)
	defer transport.Close()

	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	results := make([]interface{}, n)

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			want := fmt.Sprintf("value-%d", i)
			results[i], errs[i] = transport.Execute(
				context.Background(), "GET", []interface{}{want})
		}(i)
	}
	wg.Wait()

	for i := range n {
		if errs[i] != nil {
			t.Fatalf("call %d: %v", i, errs[i])
		}
		want := fmt.Sprintf("value-%d", i)
		if results[i] != want {
			t.Errorf("call %d got %v, want %s — a response was mismatched",
				i, results[i], want)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Errorf("server saw %d distinct request ids, want %d", len(seen), n)
	}
}

func TestExecuteRefusesOverCapLengthPrefix(t *testing.T) {
	// A hostile peer must not be able to drive an unbounded allocation from a
	// four-byte prefix: the cap is checked before the body is read.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	addr := listener.Addr().(*net.TCPAddr)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		header := make([]byte, 4)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		body := make([]byte, binary.LittleEndian.Uint32(header))
		_, _ = io.ReadFull(conn, body)

		// Claim a body far larger than the cap, and send nothing after it.
		oversized := make([]byte, 4)
		binary.LittleEndian.PutUint32(oversized, uint32(MaxFrameBytes)+1)
		_, _ = conn.Write(oversized)
		time.Sleep(300 * time.Millisecond)
	}()

	transport := newSynapRpcTransport("127.0.0.1", addr.Port, 2*time.Second)
	defer transport.Close()

	_, err = transport.Execute(context.Background(), "GET", []interface{}{"k"})
	if err == nil {
		t.Fatal("an over-cap length prefix must be refused")
	}
}

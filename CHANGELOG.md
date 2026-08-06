# Changelog

All notable changes to the Synap Go SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.3.1] - 2026-08-06

Catches the Go SDK up with the server: it was still on 1.2.3 while the server
reached 1.3.1, so it carried neither the 1.3.0 transaction work nor the 1.3.1
stream fixes. Two of the gaps were not "missing features" but broken wires —
consuming a stream over a native transport, and reading any RESP3 map — which
had shipped that way because nothing tested the command mapping. 1.3.0 is
skipped so the number matches the server release this SDK is verified against.

### Added
- **Transactions.** `client.Transaction()` opens a Redis-compatible
  MULTI/EXEC scope — the last SDK to get one. `Multi(ctx)` returns a `*Tx`
  whose `KV()`, `Hash()`, `List()` and `Set()` accessors issue their commands
  inside the transaction, so the client id that keys it server-side is threaded
  through for you instead of being passed to every call. `Begin()` opens the
  scope without contacting the server (for `Watch` before `Multi`), `Resume()`
  rebinds a transaction by id, and `Exec` reports commit or abort through
  `ExecResult`. Writes are queued by the server — they travel as
  `TXQUEUE <client_id> <CMD> <args...>` on the native transports (ADR 005) —
  and a command the server cannot queue is refused rather than silently
  executed outside the transaction. The parent client stays non-transactional
  while a `Tx` is open. Verified against a live server on HTTP, SynapRPC and
  RESP3, including that nothing reaches the store before `Exec`.

### Fixed
- **RESP3 maps decode at all.** The reader stopped at the `%` type byte with
  `unknown type byte "%"`, and the server answers `SSTATS`, `SUBSCRIBE` and
  every `SREAD` entry with a map — so streams were unusable over RESP3 even
  after the command names were fixed. Maps, sets (`~`), booleans (`#`),
  doubles (`,`), big numbers (`(`), verbatim strings (`=`) and blob errors
  (`!`) all decode now.
- **Consuming a stream over a native transport works at all.**
  `stream.consume` mapped to `SCONSUME`, a command the Synap server has never
  implemented, so every SynapRPC/RESP3 consume failed as an unknown command; it
  now maps to `SREAD` and carries room, subscriber, offset and limit. The rest
  of the family was mapped but had no reply translation, so `stream.publish`,
  `stream.list`, `stream.create`, `stream.delete` and `stream.get_or_create`
  now translate their raw wire replies into the shapes the stream module
  decodes, and `stream.get_or_create` reaches the native transports at all
  (it had no `SGETORCREATE` mapping). Covered by a new `command_map_test.go`
  that pins each command to the name the server dispatches.

### Added
- **Room wipe discriminator** (hivellm/synap#257, server 1.3.1): `StreamStats`
  carries `CreatedAt` and `Generation`. Generation identifies this incarnation
  of the room and is strictly increasing across every room creation, including
  server restarts — remember it alongside your offset cursor and rewind when it
  changes, because every counter in the payload resets on a wipe and can
  coincide with a pre-wipe value. `MinOffset` is now decoded too.

### Changed
- `github.com/hivellm/thunder-go` 0.2.1 → 0.2.2, matching the version every
  other end of the wire runs.
- The `go` directive is pinned at 1.25.12: `govulncheck` reported 13 standard
  library advisories reachable from this module under 1.25.4 (crypto/tls,
  crypto/x509, net/http, net/url, net/textproto), and 1.25.12 clears all of
  them. Scan is clean.

## [1.2.3] - 2026-07-20

Realigns the Go SDK onto the shared Synap SDK version — all six SDKs now sit at
1.2.3. 1.2.1 and 1.2.2 are skipped to reach a single common number.

### Added
- **KV watch.** `client.KV().Watch(ctx, pattern, opts...)` streams
  value-carrying change envelopes as a channel of the new `WatchEvent` struct,
  over a dedicated `KV.WATCH` push connection. `synap.WithNotifyMode()` asks
  the server to strip values per subscription. Cancelling the context (or the
  returned stop) issues `KV.UNWATCH` and closes the channel. SynapRPC only;
  HTTP clients get a clear error pointing at the `/kv/ws` endpoint.

## [1.2.0] - 2026-07-19

Released alongside Synap 1.2.0. No functional change since 1.1.1 — the version
moves to keep the SDK aligned with the server release it is verified against.

## [1.1.1] - 2026-07-19

### Fixed
- **A binary value survives a full round trip.** 1.1.0 fixed the outbound path
  and the transport, but replies were still re-encoded as JSON to reach the
  module methods, and Go's JSON encoder replaces every invalid UTF-8 sequence
  with U+FFFD — so `deadbeef` came back as `deadefbfbdefbfbd`. Replies on
  `synap://` now travel to the caller as typed values. HTTP and RESP3, which
  genuinely speak JSON, are unchanged.

## [1.1.0] - 2026-07-19

### Changed
- **The `synap://` transport now runs on [Thunder](https://github.com/hivellm/thunder)**
  (`github.com/hivellm/thunder-go`), the family's shared binary RPC client — the
  same protocol implementation the Synap server runs, so the two ends of the wire
  cannot drift. The hand-written framing, socket handling, reconnect loop and
  request-id bookkeeping are gone.
- **The module path is now `github.com/hivellm/synap-sdk-go`**, matching this
  repository, which is where the Go SDK now lives. The previous module,
  `github.com/hivellm/synap/sdks/go`, stays resolvable at its published versions
  (through v0.11.1) — the Go proxy is immutable — but receives no further
  releases. Migrating is a one-line import change:

  ```go
  // before
  import synap "github.com/hivellm/synap/sdks/go"
  // after
  import synap "github.com/hivellm/synap-sdk-go"
  ```
- **Minimum Go is now 1.25**, raised from 1.22 by the Thunder client.

### Added
- `PubSubManager.Observe` streams server-push messages over a dedicated
  subscription. This SDK previously had no push support of any kind — pub/sub was
  request/response only.

### Fixed
- **The SDK can reach an authenticated server over `synap://`.** The RPC
  transport never sent `AUTH`, so against a `require_auth` deployment on 15501
  every command came back `NOAUTH` — the transport was unusable, not degraded.
- **Concurrent commands genuinely overlap.** The previous transport held a mutex
  across each request/response round trip, so they serialized.
- **A binary payload is no longer destroyed on the way out.** The client marshalled
  each command payload to JSON purely to reach its fields by name, and Go's JSON
  encoder replaces every invalid UTF-8 sequence with U+FFFD. Payload fields are
  now read by reflection, so strings stay byte-exact.

- **Known limitation, fixed in 1.1.1:** a binary value does not survive a full
  round trip in this version — replies still travel to the module methods as
  JSON.

## [1.0.0] - 2026-07-11

### Added
- First tagged release of the Go SDK: KV, Hash, List, Set, Sorted Set, Queue,
  Stream and Pub/Sub modules over three transports (SynapRPC default, RESP3,
  HTTP), selected by URL scheme.

### Changed
- Version aligned with the Synap server 1.0.0 release. SynapRPC (`synap://host:15501`) is the default transport; RESP3 and HTTP remain available via URL scheme. Test suite verified against the official `hivehub/synap:1.0.0` image.
- `stretchr/testify` 1.6 → 1.11.

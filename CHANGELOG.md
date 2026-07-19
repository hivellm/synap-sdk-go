# Changelog

All notable changes to the Synap Go SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

- **A binary value survives a full round trip.** Responses used to be re-encoded
  as JSON to reach the module methods, which re-introduced the same U+FFFD
  substitution inbound. The binary transport now carries typed Go values all the
  way to the caller; HTTP and RESP3, which genuinely speak JSON, are unchanged.
  `Set`/`Get` of `deadbeef` returns `deadbeef`.

## [1.0.0] - 2026-07-11

### Added
- First tagged release of the Go SDK: KV, Hash, List, Set, Sorted Set, Queue,
  Stream and Pub/Sub modules over three transports (SynapRPC default, RESP3,
  HTTP), selected by URL scheme.

### Changed
- Version aligned with the Synap server 1.0.0 release. SynapRPC (`synap://host:15501`) is the default transport; RESP3 and HTTP remain available via URL scheme. Test suite verified against the official `hivehub/synap:1.0.0` image.
- `stretchr/testify` 1.6 → 1.11.

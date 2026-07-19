# Changelog

All notable changes to the Synap Go SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **The `synap://` transport now runs on [Thunder](https://github.com/hivellm/thunder)**
  (`github.com/hivellm/thunder-go`), the family's shared binary RPC client — the
  same protocol implementation the Synap server runs, so the two ends of the wire
  cannot drift. The hand-written framing, socket handling, reconnect loop and
  request-id bookkeeping are gone.
- **The module path is now `github.com/hivellm/synap-sdk-go`**, matching this
  repository. It declared `github.com/hivellm/synap/sdks/go` while living at the
  new URL, so `go get` could not resolve it at all.
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

### Known limitations
- **A binary value still does not survive a round trip.** The outbound path and
  the transport are byte-exact now, but responses are still shuttled from the
  transport to the module methods as JSON, and that re-introduces the same U+FFFD
  substitution on the way back. Fixing it means replacing that internal plumbing
  across every module. Tracked as the remaining work from
  `phase20_thunder-go-sdk-swap`.

## [1.0.0] - 2026-07-11

### Added
- First tagged release of the Go SDK: KV, Hash, List, Set, Sorted Set, Queue,
  Stream and Pub/Sub modules over three transports (SynapRPC default, RESP3,
  HTTP), selected by URL scheme.

### Changed
- Version aligned with the Synap server 1.0.0 release. SynapRPC (`synap://host:15501`) is the default transport; RESP3 and HTTP remain available via URL scheme. Test suite verified against the official `hivehub/synap:1.0.0` image.
- `stretchr/testify` 1.6 → 1.11.

# easybus-sdk-rust

The **Rust** implementation of the easybus protocol SDK — a multi-tenant
message bus between an agent and its extensions.

## Source of truth

The wire contract is `proto/easybus/v1/protocol.proto`, **copied** from the Go
reference (`easydev-org/easybus-sdk`); `build.rs` (prost + a vendored protoc)
generates the messages, so a plain `cargo build` is self-contained.
`buf generate` produces the same code.

Runtime conventions (subject layout `easybus.<tenant>.<...>`, KV-key escaping,
golden derivations) live in `src/protocol.rs` and MUST match Go / TypeScript /
Python byte-for-byte.

## Layout (mirrors the Go SDK)

```
proto/easybus/v1/protocol.proto   the wire contract (copied, never edited here)
src/gen (OUT_DIR)                 generated messages (prost)
src/protocol.rs                   subjects, KV keys, escaping, golden
src/wire.rs                       the Go-compatible JSON codec
src/identity.rs                   HMAC pre-shared identity
src/bus.rs                        transport-agnostic Bus
src/transport/nats.rs             the NATS transport
src/extension.rs                  the extension role
src/agent.rs                      the agent role
```

## Tests

```
cargo t          # the full suite (serial; see .cargo/config.toml)
```

One nats-server serves the whole suite. Tests run **serially** because
`easybus.discover` is a global control-plane subject: two extensions answering
at once would see each other. Set `EASYBUS_NATS_SERVER_BIN` to point at a
nats-server binary (defaults to `nats-server` on `$PATH`).

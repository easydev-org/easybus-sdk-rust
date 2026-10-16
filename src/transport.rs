//! The NATS transport — the Rust twin of the Go `easybus-sdk/transport/nats`
//! package. The wire envelope is Go-compatible JSON (snake_case, `kind` a short
//! string). Stream topology (mailbox/events/dlq) and the object/KV buckets
//! match the Go side.

pub mod nats;

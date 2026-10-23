//! The Rust implementation of the easybus protocol SDK — a multi-tenant message
//! bus between an agent and its extensions.
//!
//! The wire contract is `proto/easybus/v1/protocol.proto` (copied from the Go
//! reference, the single source of truth); `build.rs` (prost + vendored protoc)
//! generates the messages. Runtime conventions (subject layout
//! `easybus.<tenant>.<...>`, KV-key escaping, golden derivations) live in
//! [`protocol`] and MUST match Go/TypeScript/Python byte-for-byte.

pub mod gen {
    pub mod easybus {
        pub mod v1 {
            // COMMITTED, not generated at build time.
            //
            // prost's default writes into OUT_DIR, which nothing can review: a contract change
            // that altered a message produced no diff, so a stale binding was indistinguishable
            // from a fresh one — and a field this build did not know about was silently absent
            // on the wire. Committing the output puts it in `git diff`, in review, and within
            // reach of the CI bot that regenerates it.
            include!("gen/easybus.v1.rs");
        }
    }
}

pub mod agent;
pub mod bus;
pub mod channel;
pub mod extension;
pub mod i18n;
pub mod identity;
pub mod manifest;
pub mod permission;
pub mod protocol;
pub mod transport;
pub mod wire;

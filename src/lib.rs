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
            include!(concat!(env!("OUT_DIR"), "/easybus/easybus.v1.rs"));
        }
    }
}

pub mod agent;
pub mod bus;
pub mod extension;
pub mod i18n;
pub mod identity;
pub mod manifest;
pub mod protocol;
pub mod transport;
pub mod wire;

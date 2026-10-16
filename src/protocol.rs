//! Runtime protocol conventions — the Rust twin of the Go `easybus-sdk/protocol`
//! package. Every derivation here MUST match the Go implementation byte-for-byte
//! (see [`golden`] for the frozen vectors).
//!
//! The wire MESSAGES come from proto/easybus/v1/protocol.proto; this module owns
//! the SUBJECT layout, KV-key layout, tenant rules and small arg helpers that
//! proto cannot express.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use sha2::{Digest, Sha256};

/// Derive the transport-safe token for a session name.
pub fn session_token(session_name: &str) -> String {
    let mut h = Sha256::new();
    h.update(session_name.as_bytes());
    let sum = h.finalize();
    // base64url WITHOUT padding, truncated to 22 chars (matches Go's
    // base64.RawURLEncoding.EncodeToString(sum)[:22]).
    let s = URL_SAFE_NO_PAD.encode(sum);
    s.chars().take(22).collect()
}

/// The reserved tenant for GLOBAL control-plane messages.
pub const GLOBAL_TENANT: &str = "global";

/// A tenant is an opaque isolation key matching `[A-Za-z0-9_-]{1,64}`.
pub fn is_valid_tenant(raw: &str) -> bool {
    !raw.is_empty()
        && raw.len() <= 64
        && raw.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// The data-plane subject prefix for a tenant: `easybus.<tenant>.`
pub fn tenant_prefix(tenant: &str) -> String {
    format!("easybus.{tenant}.")
}

/// Extract the tenant segment, or "" when not namespaced.
pub fn subject_tenant(ch: &str) -> String {
    let p = "easybus.";
    let Some(rest) = ch.strip_prefix(p) else {
        return String::new();
    };
    match rest.find('.') {
        Some(i) if i > 0 => {
            let t = &rest[..i];
            if is_valid_tenant(t) {
                t.to_string()
            } else {
                String::new()
            }
        }
        _ => String::new(),
    }
}

/// A tenant-scoped KV key in a shared bucket: `t.<tenant>.<rest>`.
pub fn tenant_kv_key(tenant: &str, rest: &str) -> String {
    format!("t.{tenant}.{rest}")
}

/// A tenant-scoped object-store name: `t.<tenant>.<name>`.
pub fn tenant_object_name(tenant: &str, name: &str) -> String {
    format!("t.{tenant}.{name}")
}

pub const CH_DISCOVER: &str = "easybus.discover";
pub const MAILBOX_WILDCARD: &str = "easybus.";
pub const MAILBOX_WILDCARD_ALL: &str = "easybus.*.mailbox.";
pub const LIFECYCLE_WILDCARD: &str = "easybus.*.session.lifecycle.";
pub const VARS_BUCKET: &str = "vars";
pub const CONFIG_KV_BUCKET: &str = "cfg";

pub fn ch_tool_call(tenant: &str, ext_id: &str, tool: &str) -> String {
    format!("{}tool.call.{ext_id}.{tool}", tenant_prefix(tenant))
}
pub fn ch_tool_progress(tenant: &str, call_id: &str) -> String {
    format!("{}tool.progress.{call_id}", tenant_prefix(tenant))
}
pub fn ch_variable(tenant: &str, ext_id: &str, name: &str) -> String {
    format!("{}var.{ext_id}.{name}", tenant_prefix(tenant))
}
pub fn ch_mailbox(tenant: &str, session: &str) -> String {
    format!("{}mailbox.{}", tenant_prefix(tenant), session_token(session))
}
pub fn ch_session_events(tenant: &str, session: &str) -> String {
    format!("{}session.events.{}", tenant_prefix(tenant), session_token(session))
}
pub fn ch_session_changed(tenant: &str) -> String {
    format!("{}session.changed", tenant_prefix(tenant))
}
pub fn ch_interrupt(tenant: &str, ext_id: &str) -> String {
    format!("{}ctl.interrupt.{ext_id}", tenant_prefix(tenant))
}
pub fn ch_interrupt_all(tenant: &str) -> String {
    format!("{}ctl.interrupt.>", tenant_prefix(tenant))
}
pub fn ch_hook_call(tenant: &str, ext_id: &str, hook: &str) -> String {
    format!("{}hook.call.{ext_id}.{hook}", tenant_prefix(tenant))
}
pub fn ch_hook_event(tenant: &str, hook: &str) -> String {
    format!("{}hook.event.{hook}", tenant_prefix(tenant))
}
pub fn ch_lifecycle(tenant: &str, kind: &str) -> String {
    format!("{}session.lifecycle.{kind}", tenant_prefix(tenant))
}
pub fn ch_config(tenant: &str, ext_id: &str) -> String {
    format!("{}config.{ext_id}", tenant_prefix(tenant))
}
pub fn var_key(tenant: &str, ext_id: &str, name: &str) -> String {
    tenant_kv_key(tenant, &format!("{ext_id}.{name}"))
}
pub fn session_var_key(tenant: &str, ext_id: &str, session_name: &str, name: &str) -> String {
    tenant_kv_key(tenant, &format!("{ext_id}.{}.{name}", session_token(session_name)))
}

/// A fresh random UUID-v4-shaped id (matches Go's protocol.NewID).
pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

const HEX: &[u8; 16] = b"0123456789ABCDEF";

fn kv_safe(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '/'
}

/// Encode a string into a NATS KV-safe key segment using only
/// `[a-zA-Z0-9-/_=]`. The escape char `=` introduces a two-hex-digit byte
/// (`.` -> "=2E", `=` -> "=3D"). Never produces the `.` separator.
pub fn escape_kv_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if kv_safe(c) {
            out.push(c);
        } else {
            let b = c as u32;
            out.push('=');
            out.push(HEX[((b >> 4) & 0xF) as usize] as char);
            out.push(HEX[(b & 0xF) as usize] as char);
        }
    }
    out
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'A'..=b'F' => Some(c - b'A' + 10),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    }
}

/// Reverse [`escape_kv_segment`].
pub fn unescape_kv_segment(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = String::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'=' && i + 2 < b.len() {
            if let (Some(hi), Some(lo)) = (hex_val(b[i + 1]), hex_val(b[i + 2])) {
                out.push(((hi << 4) | lo) as char);
                i += 3;
                continue;
            }
        }
        out.push(b[i] as char);
        i += 1;
    }
    out
}

/// The frozen cross-language derivations — MUST equal the Go vectors in
/// easybus-sdk/conformance/golden_test.go, the TS vectors in
/// easybus-sdk-typescript/src/protocol/golden.ts and the Python vectors in
/// easybus-sdk-python/src/easybus/protocol/golden.py.
pub mod golden {
    use super::*;

    pub const GOLDEN_TENANT: &str = "alice";

    /// name -> (got, want). Every entry must hold exactly.
    pub fn vectors() -> Vec<(&'static str, String, &'static str)> {
        let t = GOLDEN_TENANT;
        vec![
            ("session_token", session_token("sess-1"), "q-Yz86R6J1gXTqvpFg2vNs"),
            ("discover", CH_DISCOVER.to_string(), "easybus.discover"),
            ("tool_call", ch_tool_call(t, "ops", "echo"), "easybus.alice.tool.call.ops.echo"),
            ("tool_progress", ch_tool_progress(t, "c1"), "easybus.alice.tool.progress.c1"),
            ("variable", ch_variable(t, "ops", "base-url"), "easybus.alice.var.ops.base-url"),
            ("interrupt", ch_interrupt(t, "ops"), "easybus.alice.ctl.interrupt.ops"),
            (
                "hook_call",
                ch_hook_call(t, "ops", "session.before_create"),
                "easybus.alice.hook.call.ops.session.before_create",
            ),
            ("hook_event", ch_hook_event(t, "session.created"), "easybus.alice.hook.event.session.created"),
            ("mailbox_wildcard", format!("{MAILBOX_WILDCARD_ALL}>"), "easybus.*.mailbox.>"),
            ("mailbox_sess1", ch_mailbox(t, "sess-1"), "easybus.alice.mailbox.q-Yz86R6J1gXTqvpFg2vNs"),
            ("session_events_s1", ch_session_events(t, "sess-1"), "easybus.alice.session.events.q-Yz86R6J1gXTqvpFg2vNs"),
            ("config", ch_config(t, "ops"), "easybus.alice.config.ops"),
            ("var_key", var_key(t, "ops", "base-url"), "t.alice.ops.base-url"),
            ("session_var_key", session_var_key(t, "ops", "sess-1", "ws"), "t.alice.ops.q-Yz86R6J1gXTqvpFg2vNs.ws"),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn golden_vectors() {
        for (name, got, want) in golden::vectors() {
            assert_eq!(got, want, "golden vector {name}");
        }
    }
}

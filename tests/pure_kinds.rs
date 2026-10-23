//! Runners for the pure-computation shared kinds.
//!
//! Included by shared_cases.rs so the same loop covers every kind, and kept in its own module so
//! the broker-dependent cases stay readable. No broker here: these compare this SDK's functions
//! to values pinned by the reference implementation, because a subject or matching-rule
//! divergence is otherwise invisible until a message fails to arrive.

use serde_json::Value;

use easybus_sdk::channel::{ch_channel, match_subject, parse_channel_subject};
use easybus_sdk::permission::{matches, GrantParams};

fn s(v: &Value, k: &str) -> String {
    v[k].as_str().unwrap_or_default().to_string()
}

fn params(v: &Value) -> GrantParams {
    let mut out = GrantParams::new();
    if let Some(o) = v.as_object() {
        for (k, val) in o {
            out.insert(
                k.clone(),
                match val {
                    Value::String(x) => x.clone(),
                    other => other.to_string(),
                },
            );
        }
    }
    out
}

pub fn run_channel_matching(case: &Value, id: &str) {
    let cases = case["cases"]
        .as_array()
        .unwrap_or_else(|| panic!("{id}: no cases"));
    assert!(!cases.is_empty(), "{id}: empty");
    for (i, c) in cases.iter().enumerate() {
        let pattern = c["pattern"].as_str().unwrap_or_default();
        let topic = c["topic"].as_str().unwrap_or_default();
        let want = c["want"].as_bool().expect("want");
        let why = c["why"].as_str().unwrap_or_default();
        assert_eq!(
            match_subject(pattern, topic),
            want,
            "{id} case {i}: {why} ({pattern} vs {topic})"
        );
    }
}

pub fn run_channel_subjects(case: &Value, id: &str) {
    let cases = case["cases"]
        .as_array()
        .unwrap_or_else(|| panic!("{id}: no cases"));
    assert!(!cases.is_empty(), "{id}: empty");
    for (i, c) in cases.iter().enumerate() {
        let why = c["why"].as_str().unwrap_or_default();
        if let Some(b) = c.get("build") {
            let got = ch_channel(&s(b, "tenant"), &s(b, "channel"), &s(b, "topic"))
                .unwrap_or_else(|e| panic!("{id} case {i} ({why}): {e}"));
            assert_eq!(got, c["want"].as_str().unwrap(), "{id} case {i}: {why}");
        } else if let Some(p) = c.get("parse").and_then(|v| v.as_str()) {
            let got = parse_channel_subject(p);
            if c["want"].is_null() {
                assert!(got.is_none(), "{id} case {i}: {why} — expected None for {p}");
            } else {
                let (t, ch, top) =
                    got.unwrap_or_else(|| panic!("{id} case {i} ({why}): None for {p}"));
                let want = c["want"].as_array().unwrap();
                assert_eq!(t, want[0].as_str().unwrap(), "{id} case {i}: {why}");
                assert_eq!(ch, want[1].as_str().unwrap(), "{id} case {i}: {why}");
                assert_eq!(top, want[2].as_str().unwrap(), "{id} case {i}: {why}");
            }
        } else if let Some(r) = c.get("reject") {
            // A wildcard or empty segment must be refused, not published as a broadcast.
            assert!(
                ch_channel(&s(r, "tenant"), &s(r, "channel"), &s(r, "topic")).is_err(),
                "{id} case {i}: {why} — should be refused"
            );
        }
    }
}

pub fn run_protocol_subjects(case: &Value, id: &str) {
    use easybus_sdk::protocol::*;

    let cases = case["cases"]
        .as_array()
        .unwrap_or_else(|| panic!("{id}: no cases"));
    assert!(!cases.is_empty(), "{id}: empty");
    for (i, c) in cases.iter().enumerate() {
        let fn_name = c["fn"].as_str().unwrap_or_default();
        let a = &c["args"];
        let why = c["why"].as_str().unwrap_or_default();
        let got: String = match fn_name {
            "chMailbox" => ch_mailbox(&s(a, "tenant"), &s(a, "session")),
            "chToolCall" => ch_tool_call(&s(a, "tenant"), &s(a, "ext"), &s(a, "tool")),
            "chToolProgress" => ch_tool_progress(&s(a, "tenant"), &s(a, "callId")),
            "chVariable" => ch_variable(&s(a, "tenant"), &s(a, "ext"), &s(a, "name")),
            "chSessionEvents" => ch_session_events(&s(a, "tenant"), &s(a, "session")),
            "chSessionChanged" => ch_session_changed(&s(a, "tenant")),
            "chInterrupt" => ch_interrupt(&s(a, "tenant"), &s(a, "ext")),
            "chInterruptAll" => ch_interrupt_all(&s(a, "tenant")),
            "chLifecycle" => ch_lifecycle(&s(a, "tenant"), &s(a, "kind")),
            "chHookCall" => ch_hook_call(&s(a, "tenant"), &s(a, "ext"), &s(a, "hook")),
            "chHookEvent" => ch_hook_event(&s(a, "tenant"), &s(a, "hook")),
            "chConfig" => ch_config(&s(a, "tenant"), &s(a, "ext")),
            "chFileGet" => ch_file_get(&s(a, "tenant")),
            "chFileIngest" => ch_file_ingest(&s(a, "tenant")),
            "chChannel" => ch_channel(&s(a, "tenant"), &s(a, "channel"), &s(a, "topic"))
                .unwrap_or_else(|_| "REJECTED".to_string()),
            "subjectTenant" => subject_tenant(&s(a, "subject")),
            other => panic!("{id} case {i}: no mapping for {other}"),
        };
        assert_eq!(got, c["want"].as_str().unwrap(), "{id} case {i} ({fn_name}): {why}");
    }
}

/// Mirrors the permission-matching runner so both live next to their siblings.
pub fn run_permission_table(case: &Value, id: &str) {
    let cases = case["cases"]
        .as_array()
        .unwrap_or_else(|| panic!("{id}: no cases"));
    assert!(!cases.is_empty(), "{id}: empty");
    for (i, c) in cases.iter().enumerate() {
        let grant = params(&c["grant"]);
        let target = params(&c["target"]);
        let want = c["want"].as_bool().expect("want");
        let why = c["why"].as_str().unwrap_or_default();
        assert_eq!(matches(&grant, &target), want, "{id} case {i}: {why}");
    }
}

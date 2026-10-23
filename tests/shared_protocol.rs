//! The shared cross-language tables: channel matching, channel subjects, every subject string.
//!
//! Each describes behaviour implemented once per language. They are read from the SAME files in
//! every SDK, so a divergence fails a test here instead of surfacing as a message that silently
//! never arrives — a wrong subject or a wrong `>` rule produces no error anywhere.

use serde_json::Value;

use easybus_sdk::channel::{ch_channel, match_subject, parse_channel_subject};
use easybus_sdk::protocol::{
    ch_config, ch_file_get, ch_file_ingest, ch_hook_call, ch_hook_event, ch_interrupt,
    ch_interrupt_all, ch_lifecycle, ch_mailbox, ch_session_changed, ch_session_events,
    ch_tool_call, ch_tool_progress, ch_variable, subject_tenant,
};

fn table(name: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("conformance-cases")
        .join(name);
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("parse table")
}

fn s(args: &Value, k: &str) -> String {
    args[k].as_str().unwrap_or_default().to_string()
}

#[test]
fn shared_channel_matching() {
    let doc = table("channel-matching.json");
    let cases = doc["cases"].as_array().expect("cases");
    assert!(!cases.is_empty());
    for (i, c) in cases.iter().enumerate() {
        let pattern = c["pattern"].as_str().unwrap_or_default();
        let topic = c["topic"].as_str().unwrap_or_default();
        let want = c["want"].as_bool().expect("want");
        let why = c["why"].as_str().unwrap_or_default();
        assert_eq!(
            match_subject(pattern, topic),
            want,
            "case {i}: {why} ({pattern} vs {topic})"
        );
    }
}

#[test]
fn shared_channel_subjects() {
    let doc = table("channel-subjects.json");
    let cases = doc["cases"].as_array().expect("cases");
    assert!(!cases.is_empty());
    for (i, c) in cases.iter().enumerate() {
        let why = c["why"].as_str().unwrap_or_default();
        if let Some(b) = c.get("build") {
            let got = ch_channel(
                &s(b, "tenant"),
                &s(b, "channel"),
                &s(b, "topic"),
            )
            .unwrap_or_else(|e| panic!("case {i} ({why}): {e}"));
            assert_eq!(got, c["want"].as_str().unwrap(), "case {i}: {why}");
        } else if let Some(p) = c.get("parse").and_then(|v| v.as_str()) {
            let got = parse_channel_subject(p);
            if c["want"].is_null() {
                assert!(got.is_none(), "case {i}: {why} — expected None for {p}");
            } else {
                let (t, ch, top) = got.unwrap_or_else(|| panic!("case {i}: {why} — None"));
                let want = c["want"].as_array().unwrap();
                assert_eq!(t, want[0].as_str().unwrap(), "case {i}: {why}");
                assert_eq!(ch, want[1].as_str().unwrap(), "case {i}: {why}");
                assert_eq!(top, want[2].as_str().unwrap(), "case {i}: {why}");
            }
        } else if let Some(r) = c.get("reject") {
            // A wildcard or empty segment must be refused, not published as a broadcast.
            assert!(
                ch_channel(&s(r, "tenant"), &s(r, "channel"), &s(r, "topic")).is_err(),
                "case {i}: {why} — should be refused"
            );
        }
    }
}

#[test]
fn shared_protocol_subjects() {
    let doc = table("protocol-subjects.json");
    let cases = doc["cases"].as_array().expect("cases");
    assert!(!cases.is_empty());
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
            other => panic!("case {i}: no mapping for {other}"),
        };
        assert_eq!(got, c["want"].as_str().unwrap(), "case {i} ({fn_name}): {why}");
    }
}

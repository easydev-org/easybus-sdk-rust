//! The shared matching table, from conformance-cases/permission-matching.json.
//!
//! WHY a shared file rather than a test written once here: the rules are implemented four times,
//! and they are the security boundary of every extension. Two SDKs that disagree do not fail
//! loudly — they authorise different things for the same grant, silently, depending on which
//! language the extension happens to be written in. Every SDK reads this same table.

use std::collections::HashMap;

use easybus_sdk::permission::{matches, GrantParams};
use serde_json::Value;

#[test]
fn shared_matching_table() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("conformance-cases/permission-matching.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let table: Value = serde_json::from_str(&raw).expect("parse shared table");

    let cases = table["cases"].as_array().expect("cases array");
    assert!(!cases.is_empty(), "the shared table is empty");

    for (i, c) in cases.iter().enumerate() {
        let grant = to_params(&c["grant"]);
        let target = to_params(&c["target"]);
        let want = c["want"].as_bool().expect("want");
        let why = c["why"].as_str().unwrap_or("");
        assert_eq!(matches(&grant, &target), want, "case {i}: {why}");
    }
}

fn to_params(v: &Value) -> GrantParams {
    let mut out = HashMap::new();
    if let Some(obj) = v.as_object() {
        for (k, val) in obj {
            let s = match val {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            out.insert(k.clone(), s);
        }
    }
    out
}

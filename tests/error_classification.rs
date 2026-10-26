//! Pin how a transport error is CATEGORISED.
//!
//! The shared half of the KV fix. Every SDK used to collapse every failure into a zero value
//! (`Err(_) => 0`), which made a lost race and a broken connection indistinguishable. The zero
//! value is CORRECT for a lost race and catastrophic for anything else, so the category is the
//! contract.
//!
//! No broker: this is pure text matching, so it runs everywhere.

use easybus_sdk::protocol::{is_absent_key, is_consumer_gone, is_lost_race};
use serde_json::Value;

fn cases_path() -> std::path::PathBuf {
    // Resolved from this file rather than the working directory: the runner is also called from
    // other crates' tests, whose cwd is their own.
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("conformance-cases")
        .join("kv-error-classification.json")
}

#[test]
fn shared_error_classification_case() {
    let raw = std::fs::read(cases_path()).expect("read kv-error-classification.json");
    let case: Value = serde_json::from_slice(&raw).expect("parse");
    let id = case["id"].as_str().unwrap_or("?");
    let entries = case["cases"].as_array().expect("cases array");
    assert!(!entries.is_empty(), "{id}: no cases");

    for (i, mc) in entries.iter().enumerate() {
        let msg = mc["err"].as_str().unwrap_or("");
        let why = mc["why"].as_str().unwrap_or("");
        // An empty message means "no error at all".
        let err: Option<&str> = if msg.is_empty() { None } else { Some(msg) };

        assert_eq!(
            is_lost_race(err),
            mc["lostRace"].as_bool().unwrap_or(false),
            "{id} case {i} is_lost_race({msg:?}): {why}"
        );
        assert_eq!(
            is_absent_key(err),
            mc["absent"].as_bool().unwrap_or(false),
            "{id} case {i} is_absent_key({msg:?}): {why}"
        );
        assert_eq!(
            is_consumer_gone(err),
            mc["consumerGone"].as_bool().unwrap_or(false),
            "{id} case {i} is_consumer_gone({msg:?}): {why}"
        );

        // The categories must be mutually exclusive: a fault belonging to two would make
        // behaviour depend on clause order rather than on the fault.
        let hits = [is_lost_race(err), is_absent_key(err), is_consumer_gone(err)]
            .iter()
            .filter(|b| **b)
            .count();
        assert!(hits <= 1, "{id} case {i} ({msg:?}): matches {hits} categories");
    }
}

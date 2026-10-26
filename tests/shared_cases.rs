//! Run the shared, language-neutral conformance cases.
//!
//! The cases live in `conformance-cases/` and describe behaviour every SDK must
//! implement identically. Reading the same files in every SDK is what stops four
//! hand-written test suites from drifting apart: the config decoder had the same
//! defect in all four languages, every suite passed, and nothing compared them.
//!
//! The copy is pinned by `cases.lock`. A mismatch means this SDK has not been
//! refreshed after the contract changed — which would otherwise keep passing while
//! asserting older behaviour.

use std::path::{Path, PathBuf};

use easybus_sdk::bus::Bus;
use easybus_sdk::extension::{Config, ConfigSpec, Extension};
use easybus_sdk::protocol::CONFIG_KV_BUCKET;
use easybus_sdk::transport::nats::{NatsBus, Options};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;

// The pure-computation kinds live in their own module: same loop, but they need no broker.
#[path = "pure_kinds.rs"]
mod pure_kinds;
use pure_kinds::{run_channel_matching, run_channel_subjects, run_protocol_subjects};

async fn connect(url: &str) -> Arc<NatsBus> {
    NatsBus::connect(url, Options::default()).await.unwrap()
}

fn cases_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("conformance-cases")
}

fn case_files() -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(cases_dir())
        .expect("conformance-cases/ missing")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("json"))
        .collect();
    v.sort();
    v
}

/// The copy must match the lock, or the assertions below are testing an older
/// contract than the SDKs believe they implement.
#[test]
fn shared_cases_are_current() {
    let want = std::fs::read_to_string(cases_dir().join("cases.lock"))
        .expect("cases.lock missing")
        .trim()
        .to_string();
    let mut h = Sha256::new();
    for p in case_files() {
        h.update(std::fs::read(&p).unwrap());
    }
    let got = format!("{:x}", h.finalize());
    assert_eq!(
        got, want,
        "conformance-cases/ does not match cases.lock — run conformance/cases/gen-lock.sh \
         in the SDK repo that owns the change, then copy conformance-cases/ into this one"
    );
}

#[test]
fn shared_cases_exist() {
    assert!(!case_files().is_empty(), "no shared cases found");
}

/// One case: seed a bucket key, start an extension, assert the knob resolves.
///
/// `#[tokio::test]` per case would need a static case list; a single test that
/// iterates keeps the cases data-driven, which is the point of sharing them.
#[tokio::test(flavor = "multi_thread")]
async fn shared_cases() {
    for path in case_files() {
        let case: Value = serde_json::from_slice(&std::fs::read(&path).unwrap())
            .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
        let id = case["id"].as_str().expect("case has no id").to_string();
        let kind = case["kind"].as_str().expect("case has no kind").to_string();
        match kind.as_str() {
            "constant" => run_constant(&case, &id),
            "permission-matching" => run_permission_matching(&case, &id),
            "channel-matching" => run_channel_matching(&case, &id),
            "channel-subjects" => run_channel_subjects(&case, &id),
            "protocol-subjects" => run_protocol_subjects(&case, &id),
            "kv" => run_kv(&case, &id).await,
            "error-classification" => run_error_classification(&case, &id),
            // Not a case: the INPUTS to the broker suite (the extension it builds, the calls it
            // makes). They live in the shared directory so every SDK reads the same bytes, and
            // are consumed by the broker runner — running them through this loop made them
            // demand a broker they never needed.
            "broker-fixture" | "broker-cases" => {}
            other => panic!("{id}: unknown case kind {other}"),
        }
    }
}

/// The grant-matching rules, from the shared table.
///
/// Pure computation, so no broker: this runs in the unit job, which means a rule that drifts in
/// this SDK fails immediately rather than at the next integration run.
///
/// These rules are the security boundary of every extension, implemented once per language. Two
/// SDKs that disagree do not fail loudly — they authorise different things for the same grant,
/// depending on which language the extension happens to be written in.
fn run_permission_matching(case: &Value, id: &str) {
    use easybus_sdk::permission::{matches, GrantParams};
    use std::collections::HashMap;

    let cases = case["cases"]
        .as_array()
        .unwrap_or_else(|| panic!("{id}: no cases"));
    assert!(!cases.is_empty(), "{id}: empty case list");
    for (i, mc) in cases.iter().enumerate() {
        let to_params = |v: &Value| -> GrantParams {
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
        };
        let grant = to_params(&mc["grant"]);
        let target = to_params(&mc["target"]);
        let want = mc["want"].as_bool().expect("want");
        let why = mc["why"].as_str().unwrap_or("");
        assert_eq!(matches(&grant, &target), want, "{id} case {i}: {why}");
    }
}

fn run_constant(case: &Value, id: &str) {
    let name = case["constant"]["name"].as_str().unwrap_or_default();
    let want = case["constant"]["value"].as_str().unwrap_or_default();
    match name {
        "configKVBucket" => assert_eq!(CONFIG_KV_BUCKET, want, "{}: {}", id, why(case)),
        other => panic!("{id}: unknown constant {other}"),
    }
}

async fn run_kv(case: &Value, id: &str) {
    let tenant = "shared-t1";
    let ext_id = "shared-ext";
    let key = case["kv"]["key"]
        .as_str()
        .unwrap()
        .replace("{tenant}", tenant)
        .replace("{ext}", ext_id);
    let value = case["kv"]["value"].as_str().unwrap();

    let mut specs = std::collections::HashMap::new();
    for (name, spec) in case["knobs"].as_object().expect("knobs must be an object") {
        specs.insert(
            name.clone(),
            ConfigSpec {
                r#type: spec["type"].as_str().unwrap_or("string").to_string(),
                default: None,
                ..Default::default()
            },
        );
    }

    let url = std::env::var("EASYBUS_NATS_URL").unwrap_or_else(|_| String::new());
    // FAIL, not skip. A silent skip means `cargo test` (no broker) reports success
    // while never touching the KV path — the path where every config defect in this
    // SDK was found. CI has a dedicated job with a pinned broker; a developer
    // without one gets told how to get it.
    assert!(
        !url.is_empty(),
        "{id}: EASYBUS_NATS_URL is required — these cases cover the KV recovery path. \
         Run `docker compose up --build --exit-code-from conformance`, or start \
         nats-server -js -p 4222 and export EASYBUS_NATS_URL=nats://127.0.0.1:4222"
    );

    let a: Arc<NatsBus> = connect(&url).await;
    let e: Arc<NatsBus> = connect(&url).await;
    a.kv_put(CONFIG_KV_BUCKET, &key, value, 0).await;

    let ext = Extension::new(
        e,
        Config {
            id: ext_id.to_string(),
            version: "1".to_string(),
            config: specs,
            ..Default::default()
        },
    );
    ext.serve().await;

    let knob = case["expect"]["knob"].as_str().unwrap();
    let want = &case["expect"]["value"];
    let value_type = case["expect"]["valueType"].as_str().unwrap_or("string");
    let mut ok = false;
    for _ in 0..100 {
        if matches_value(&ext.get_config(tenant, knob, ""), want, value_type) {
            ok = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let got = ext.get_config(tenant, knob, "");
    ext.close().await;
    assert!(
        ok,
        "{id}: {knob} = {got} , want {want} ({value_type}). {}",
        why(case)
    );
}

fn why(case: &Value) -> &str {
    case["why"].as_str().unwrap_or("")
}

/// Compare by the TYPE the case declares, not the language's representation: a
/// number is f64 here, float64 in Go, number in TypeScript, and the case states the
/// intent rather than the encoding.
fn matches_value(got: &Value, want: &Value, value_type: &str) -> bool {
    match value_type {
        "string" => got.as_str() == want.as_str(),
        "number" => match (got.as_f64(), want.as_f64()) {
            (Some(g), Some(w)) => g == w,
            _ => false,
        },
        "bool" => got.as_bool() == want.as_bool(),
        "null" => got.is_null(),
        _ => false,
    }
}

/// Pin how a transport error is CATEGORISED.
///
/// The shared half of the KV fix. Every SDK used to collapse every failure into a zero value
/// (`Err(_) => 0`), which made a lost race and a broken connection indistinguishable. The zero
/// value is CORRECT for a lost race and catastrophic for anything else, so the category is the
/// contract.
///
/// An empty `err` means "no error at all": it must match nothing. A classifier that reported nil
/// as a fault would send every success path down a fault branch and misread its own result.
fn run_error_classification(case: &Value, id: &str) {
    let entries = case["cases"].as_array().expect("cases array");
    assert!(!entries.is_empty(), "{id}: no cases");

    for (i, mc) in entries.iter().enumerate() {
        let msg = mc["err"].as_str().unwrap_or("");
        let why = mc["why"].as_str().unwrap_or("");
        let err: Option<&str> = if msg.is_empty() { None } else { Some(msg) };

        assert_eq!(
            easybus_sdk::protocol::is_lost_race(err),
            mc["lostRace"].as_bool().unwrap_or(false),
            "{id} case {i} is_lost_race({msg:?}): {why}"
        );
        assert_eq!(
            easybus_sdk::protocol::is_absent_key(err),
            mc["absent"].as_bool().unwrap_or(false),
            "{id} case {i} is_absent_key({msg:?}): {why}"
        );
        assert_eq!(
            easybus_sdk::protocol::is_consumer_gone(err),
            mc["consumerGone"].as_bool().unwrap_or(false),
            "{id} case {i} is_consumer_gone({msg:?}): {why}"
        );

        // The categories must be mutually exclusive: a fault belonging to two would make
        // behaviour depend on clause order rather than on the fault.
        let hits = [
            easybus_sdk::protocol::is_lost_race(err),
            easybus_sdk::protocol::is_absent_key(err),
            easybus_sdk::protocol::is_consumer_gone(err),
        ]
        .iter()
        .filter(|b| **b)
        .count();
        assert!(hits <= 1, "{id} case {i} ({msg:?}): matches {hits} categories");
    }
}

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
            "kv" => run_kv(&case, &id).await,
            other => panic!("{id}: unknown case kind {other}"),
        }
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

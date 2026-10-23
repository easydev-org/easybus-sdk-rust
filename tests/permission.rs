//! The permission rules, which must agree with the Go, TypeScript and Python SDKs.
//!
//! These are the rules an extension uses to decide whether a grant covers a call. If they
//! disagree across SDKs, the same grant authorises different things depending on which language
//! the extension happens to be written in — the one thing a shared contract must not allow.

use std::collections::BTreeMap;

use easybus_sdk::gen::easybus::v1 as pb;
use easybus_sdk::permission::{allowed, grants_for, matches, params, require, with_grants};

fn grant(key: &str, pairs: &[(&str, &str)]) -> pb::Grant {
    let mut fields = BTreeMap::new();
    for (k, v) in pairs {
        fields.insert(
            (*k).to_string(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue((*v).to_string())),
            },
        );
    }
    pb::Grant {
        key: key.to_string(),
        params: Some(prost_types::Struct { fields }),
    }
}

#[test]
fn matches_exact() {
    assert!(matches(
        &params([("repo", "agentdev/mrflow")]),
        &params([("repo", "agentdev/mrflow")])
    ));
    assert!(!matches(
        &params([("repo", "agentdev/mrflow")]),
        &params([("repo", "agentdev/other")])
    ));
}

#[test]
fn matches_glob() {
    let cases = [
        ("agentdev/*", "agentdev/mrflow", true),
        ("agentdev/*", "easydev-org/x", false),
        ("*", "anything/at/all", true),
        ("agentdev/*.go", "agentdev/main.go", true),
        ("agentdev/*.go", "agentdev/main.md", false),
        // A wildcard must not match the parent it was anchored to.
        ("agentdev/*", "agentdev", false),
    ];
    for (pattern, value, want) in cases {
        assert_eq!(
            matches(&params([("repo", pattern)]), &params([("repo", value)])),
            want,
            "{pattern} vs {value}"
        );
    }
}

#[test]
fn matches_requires_every_param() {
    let g = params([("namespace", "dev"), ("service", "api")]);
    assert!(matches(
        &g,
        &params([("namespace", "dev"), ("service", "api")])
    ));
    assert!(!matches(
        &g,
        &params([("namespace", "dev"), ("service", "web")])
    ));
    assert!(!matches(&g, &params([("namespace", "dev")])));
}

#[test]
fn matches_ignores_undeclared_target_params() {
    assert!(matches(
        &params([("repo", "agentdev/*")]),
        &params([("repo", "agentdev/mrflow"), ("branch", "x")])
    ));
}

#[tokio::test]
async fn allowed_with_multiple_instances() {
    let gs = vec![
        grant("mr.merge", &[("repo", "agentdev/mrflow")]),
        grant("mr.merge", &[("repo", "scratch/*")]),
    ];
    with_grants(gs, async {
        assert!(allowed("mr.merge", &params([("repo", "agentdev/mrflow")])));
        assert!(allowed("mr.merge", &params([("repo", "scratch/anything")])));
        assert!(!allowed(
            "mr.merge",
            &params([("repo", "easydev-org/easyagent")])
        ));
    })
    .await;
}

#[tokio::test]
async fn keys_do_not_leak() {
    with_grants(vec![grant("mr.merge", &[("repo", "*")])], async {
        assert!(!allowed("repo.delete", &params([("repo", "anything")])));
    })
    .await;
}

#[tokio::test]
async fn nothing_without_grants() {
    with_grants(Vec::new(), async {
        assert!(!allowed("anything", &params([])));
    })
    .await;
}

/// A task-local, not a global: a grant bound for one call must be gone once that call returns,
/// or the next call would inherit it.
#[tokio::test]
async fn grants_do_not_leak_after_the_call() {
    with_grants(vec![grant("mr.merge", &[("repo", "*")])], async {
        assert!(allowed("mr.merge", &params([("repo", "x/y")])));
    })
    .await;
    assert!(!allowed("mr.merge", &params([("repo", "x/y")])));
}

#[tokio::test]
async fn require_passes_when_covered() {
    with_grants(vec![grant("mr.merge", &[("repo", "agentdev/*")])], async {
        assert!(require("mr.merge", &params([("repo", "agentdev/mrflow")])).is_ok());
    })
    .await;
}

/// The message has to name BOTH what was wanted and what is held, or a caller cannot tell "no
/// permission" from "wrong repository".
#[tokio::test]
async fn require_names_the_gap() {
    with_grants(
        vec![grant("mr.merge", &[("repo", "agentdev/mrflow")])],
        async {
            let err = require("mr.merge", &params([("repo", "easydev-org/easyagent")]))
                .expect_err("a non-covering target must be refused");
            let msg = err.to_string();
            assert!(msg.contains("easydev-org/easyagent"), "{msg}");
            assert!(msg.contains("agentdev/mrflow"), "{msg}");
        },
    )
    .await;
}

#[test]
fn require_distinguishes_none_from_not_covering() {
    // Outside any with_grants scope: nothing is held.
    let err = require("mr.merge", &params([("repo", "x/y")])).expect_err("must refuse");
    assert!(err.to_string().contains("no grant held"), "{err}");
}

#[tokio::test]
async fn grants_for_returns_every_instance() {
    with_grants(
        vec![
            grant("mr.merge", &[("repo", "a/one")]),
            grant("mr.merge", &[("repo", "a/two")]),
            grant("exec", &[("sandbox", "s")]),
        ],
        async {
            assert_eq!(grants_for("mr.merge").len(), 2);
            assert_eq!(grants_for("exec").len(), 1);
        },
    )
    .await;
}

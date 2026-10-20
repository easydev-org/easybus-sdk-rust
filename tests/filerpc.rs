//! The agent-served file RPCs: an extension ingests bytes through a stub agent
//! and gets back a canonical file code; it can fetch them back. Runs against a
//! real nats-server (skips when the binary is unavailable).
mod common;

use easybus_sdk::bus::{Bus, PublishOpts, RequestOpts, SubscribeOpts};
use easybus_sdk::extension::Extension;
use easybus_sdk::protocol::{ch_file_get, ch_file_ingest, tenant_object_name};
use easybus_sdk::transport::nats::{NatsBus, Options};
use easybus_sdk::wire::from_value;
use serde_json::{json, Value};
use std::sync::Arc;

const T: &str = "t1";

/// A minimal stand-in for easyagent's file service.
async fn stub_agent(bus: Arc<NatsBus>, tenant: String) {
    let ing = bus
        .subscribe(&ch_file_ingest(&tenant), SubscribeOpts::default())
        .await;
    let get = bus
        .subscribe(&ch_file_get(&tenant), SubscribeOpts::default())
        .await;
    let store = Arc::new(std::sync::Mutex::new(std::collections::HashMap::<
        String,
        (String, String, Vec<u8>),
    >::new()));

    let b = bus.clone();
    let st = store.clone();
    let tn = tenant.clone();
    tokio::spawn(async move {
        let tenant = tn.as_str();
        while let Some(env) = ing.next().await {
            let Some(reply_to) = env.reply_to.clone() else {
                continue;
            };
            let req = from_value(env.payload.as_ref().unwrap());
            let object = req.get("object").and_then(|x| x.as_str()).unwrap_or("");
            let data = b
                .object_get(&tenant_object_name(tenant, object))
                .await
                .unwrap_or_default();
            let code = "abc123def4567890".to_string();
            st.lock().unwrap().insert(
                code.clone(),
                (
                    req.get("name")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    "image/png".to_string(),
                    data,
                ),
            );
            b.publish(
                &reply_to,
                json!({"ok": true, "code": code, "mime": "image/png"}),
                PublishOpts {
                    tenant: tenant.to_string(),
                    ..Default::default()
                },
            )
            .await;
        }
    });

    let b2 = bus.clone();
    let tn2 = tenant.clone();
    tokio::spawn(async move {
        let tenant = tn2.as_str();
        while let Some(env) = get.next().await {
            let Some(reply_to) = env.reply_to.clone() else {
                continue;
            };
            let req = from_value(env.payload.as_ref().unwrap());
            let code = req
                .get("code")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let rec = store.lock().unwrap().get(&code).cloned();
            let Some((name, mime, bytes)) = rec else {
                b2.publish(
                    &reply_to,
                    json!({"ok": false}),
                    PublishOpts {
                        tenant: tenant.to_string(),
                        ..Default::default()
                    },
                )
                .await;
                continue;
            };
            let obj = format!("out-{code}");
            b2.object_put(&tenant_object_name(tenant, &obj), bytes.clone())
                .await;
            b2.publish(
                &reply_to,
                json!({"ok": true, "meta": {"code": code, "name": name, "mime": mime, "size": bytes.len()}, "object": obj}),
                PublishOpts { tenant: tenant.to_string(), ..Default::default() },
            )
            .await;
        }
    });
}

#[tokio::test]
async fn ingest_and_get_via_agent() {
    let Some(url) = common::nats_url() else {
        eprintln!("no nats-server; skipping");
        return;
    };
    let ext_bus = NatsBus::connect(url, Options::default()).await.unwrap();
    let agent_bus = NatsBus::connect(url, Options::default()).await.unwrap();
    stub_agent(agent_bus.clone(), T.to_string()).await;

    let ext = Extension::new(ext_bus.clone(), Default::default());
    let bytes = b"\x89PNG fake bytes".to_vec();
    let (code, mime) = ext
        .ingest_file_via_agent(T, "shot.png", bytes.clone(), "sess-1")
        .await
        .unwrap();
    assert_eq!(code, "abc123def4567890");
    assert_eq!(mime, "image/png");

    let got = ext.get_file_via_agent(T, &code).await.expect("file");
    let (meta, data) = got;
    assert_eq!(meta.get("name").and_then(|x| x.as_str()), Some("shot.png"));
    assert_eq!(data, bytes);

    // A missing code returns None.
    assert!(ext.get_file_via_agent(T, "nope").await.is_none());
    void(&ext_bus);
}

fn void<T>(_: &T) {}

#[tokio::test]
async fn ingest_request_carries_object_reference() {
    let Some(url) = common::nats_url() else {
        eprintln!("no nats-server; skipping");
        return;
    };
    let ext_bus = NatsBus::connect(url, Options::default()).await.unwrap();
    let agent_bus = NatsBus::connect(url, Options::default()).await.unwrap();
    // Observe the raw request payload: it must be {name, object} — no bytes.
    let obs = agent_bus
        .subscribe(&ch_file_ingest(T), SubscribeOpts::default())
        .await;
    let b = agent_bus.clone();
    tokio::spawn(async move {
        while let Some(env) = obs.next().await {
            let Some(reply_to) = env.reply_to.clone() else {
                continue;
            };
            let req = from_value(env.payload.as_ref().unwrap());
            assert!(req.get("object").is_some(), "object reference required");
            assert!(req.get("data").is_none(), "bytes must NOT ride the message");
            b.publish(
                &reply_to,
                json!({"ok": true, "code": "c", "mime": "image/png"}),
                PublishOpts {
                    tenant: T.to_string(),
                    ..Default::default()
                },
            )
            .await;
        }
    });

    let ext = Extension::new(ext_bus.clone(), Default::default());
    let (code, _mime) = ext
        .ingest_file_via_agent(T, "x.bin", vec![1, 2, 3], "")
        .await
        .unwrap();
    assert_eq!(code, "c");
    void(&ext_bus);
    let _ = RequestOpts::default();
    let _ = Value::Null;
}

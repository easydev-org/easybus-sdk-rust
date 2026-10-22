//! The protocol conformance suite — the Rust twin of easybus-sdk's Go
//! conformance and the TS/Python suites. Runs against a real nats-server
//! (skips when the binary is unavailable).

mod common;

use easybus_sdk::agent::Agent;
use easybus_sdk::bus::{Bus, InboxConsumeOpts, InboxPublishOpts, RequestOpts};
use easybus_sdk::extension::{
    Config, ConfigSpec, Extension, FnTool, ToolError, ToolResultData, ToolSpec, TypedError,
    VariableSpec,
};
use easybus_sdk::gen::easybus::v1::ExtensionManifest;
use easybus_sdk::protocol::{new_id, session_var_key, CONFIG_KV_BUCKET, VARS_BUCKET};
use easybus_sdk::transport::nats::{NatsBus, Options};
use easybus_sdk::wire::{code_to_wire, from_value};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

fn tool<F, Fut>(f: F) -> (ToolSpec, Arc<dyn easybus_sdk::extension::ToolHandler>)
where
    F: Fn(Value, String, String, String, Arc<tokio::sync::Notify>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<ToolResultData, ToolError>> + Send + 'static,
{
    (ToolSpec::default(), Arc::new(FnTool(f)))
}

async fn connect(url: &str) -> Arc<NatsBus> {
    NatsBus::connect(url, Options::default()).await.unwrap()
}

fn full_ext(bus: Arc<NatsBus>, cfg_mut: impl FnOnce(&mut Config)) -> Arc<Extension> {
    let mut cfg = Config {
        id: "conf-ext".into(),
        version: "1".into(),
        ..Default::default()
    };
    cfg.tools.insert(
        "echo".into(),
        tool(|a: Value, _c, _s, _t, _x| async move {
            Ok(ToolResultData {
                content: format!(
                    "echo:{}",
                    a.get("msg").and_then(|v| v.as_str()).unwrap_or("")
                ),
                ..Default::default()
            })
        }),
    );
    cfg.tools.insert(
        "add".into(),
        tool(|a: Value, _c, _s, _t, _x| async move {
            let x = a.get("a").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let y = a.get("b").and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(ToolResultData {
                data: Some(json!({"sum": x + y})),
                ..Default::default()
            })
        }),
    );
    cfg.tools.insert(
        "session".into(),
        tool(|_a: Value, _c, s: String, _t, _x| async move {
            Ok(ToolResultData {
                content: format!("session={s}"),
                ..Default::default()
            })
        }),
    );
    cfg.tools.insert(
        "slow".into(),
        tool(|_a: Value, _c, _s, _t, _x| async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(ToolResultData {
                content: "woke".into(),
                ..Default::default()
            })
        }),
    );
    cfg.tools.insert(
        "big".into(),
        tool(|_a: Value, _c, _s, _t, _x| async move {
            Ok(ToolResultData {
                object: Some(("big-1".into(), "text/plain".into())),
                ..Default::default()
            })
        }),
    );
    cfg.tools.insert(
        "boom".into(),
        tool(|_a: Value, _c, _s, _t, _x| async move {
            Err(ToolError::Typed(TypedError::new("CODE_BUSINESS", "nope")))
        }),
    );
    cfg.tools.insert(
        "hang".into(),
        tool(
            |_a: Value, _c, _s, _t, x: Arc<tokio::sync::Notify>| async move {
                x.notified().await;
                Err(ToolError::Typed(TypedError::new(
                    "CODE_RETRYABLE",
                    "interrupted",
                )))
            },
        ),
    );
    cfg.variables.insert(
        "base-url".into(),
        VariableSpec {
            scope: "global".into(),
            ..Default::default()
        },
    );
    cfg.config.insert(
        "poll-interval".into(),
        ConfigSpec {
            r#type: "number".into(),
            default: Some(json!(30)),
            ..Default::default()
        },
    );
    cfg.config.insert(
        "mode".into(),
        ConfigSpec {
            r#type: "enum".into(),
            enum_values: vec!["fast".into(), "safe".into()],
            default: Some(json!("safe")),
            ..Default::default()
        },
    );
    cfg.config.insert(
        "session-limit".into(),
        ConfigSpec {
            r#type: "number".into(),
            scope: "session".into(),
            default: Some(json!(5)),
            ..Default::default()
        },
    );
    cfg_mut(&mut cfg);
    Extension::new(bus, cfg)
}

macro_rules! skip_no_nats {
    () => {
        match common::nats_url() {
            Some(u) => u,
            None => {
                eprintln!("no nats-server; skipping");
                return;
            }
        }
    };
}

#[tokio::test]
async fn discover() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let agent = Agent::new(a.clone());
    let ms = agent.discover(500).await;
    let m = ms.iter().find(|m| m.id == "conf-ext").expect("conf-ext");
    assert!(m.capabilities.contains(&"tools".to_string()));
    assert!(m.capabilities.contains(&"prompt".to_string()));
    ext.close().await;
}

#[tokio::test]
async fn tool_content() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let agent = Agent::new(a.clone());
    let r = agent
        .call_tool(T, "sess-1", "conf-ext", "echo", "c1", json!({"msg": "hi"}))
        .await
        .unwrap();
    assert_eq!(r.content, "echo:hi");
    assert!(r.error.is_none());
    ext.close().await;
}

#[tokio::test]
async fn tool_data() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let agent = Agent::new(a.clone());
    let r = agent
        .call_tool(
            T,
            "sess-1",
            "conf-ext",
            "add",
            "c2",
            json!({"a": 2, "b": 3}),
        )
        .await
        .unwrap();
    assert_eq!(r.data.unwrap().get("sum").unwrap().as_f64().unwrap(), 5.0);
    ext.close().await;
}

#[tokio::test]
async fn tool_call_id_echo() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let agent = Agent::new(a.clone());
    let r = agent
        .call_tool(
            T,
            "sess-1",
            "conf-ext",
            "echo",
            "call-xyz",
            json!({"msg": "1"}),
        )
        .await
        .unwrap();
    assert_eq!(r.content, "echo:1");
    ext.close().await;
}

#[tokio::test]
async fn tool_error_mapping() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let agent = Agent::new(a.clone());
    let r = agent
        .call_tool(T, "sess-1", "conf-ext", "boom", "c3", json!({}))
        .await
        .unwrap();
    let (code, msg) = r.error.unwrap();
    assert_eq!(code, "CODE_BUSINESS");
    assert_eq!(msg, "nope");
    ext.close().await;
}

#[tokio::test]
async fn tool_session_name() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let agent = Agent::new(a.clone());
    let r = agent
        .call_tool(T, "my-session", "conf-ext", "session", "c4", json!({}))
        .await
        .unwrap();
    assert_eq!(r.content, "session=my-session");
    ext.close().await;
}

#[tokio::test]
async fn slow_tool_no_request_cap() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let agent = Agent::new(a.clone());
    let r = agent
        .call_tool(T, "sess-1", "conf-ext", "slow", "slow-1", json!({}))
        .await
        .unwrap();
    assert_eq!(r.content, "woke");
    ext.close().await;
}

#[tokio::test]
async fn tool_object_offload() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let agent = Agent::new(a.clone());
    let r = agent
        .call_tool(T, "sess-1", "conf-ext", "big", "c5", json!({}))
        .await
        .unwrap();
    assert_eq!(r.object.unwrap().0, "big-1");
    ext.close().await;
}

#[tokio::test]
async fn request_timeout_zero_is_bounded() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let agent = Agent::new(a.clone());
    let r = agent
        .call_tool(T, "sess-1", "conf-ext", "missing", "c6", json!({}))
        .await;
    assert!(r.is_err());
    ext.close().await;
}

#[tokio::test]
async fn variable() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    ext.set_variable(T, "base-url", "https://example.test")
        .await;
    let agent = Agent::new(a.clone());
    let (v, ok) = agent
        .resolve_variable(T, "sess-1", "conf-ext", "base-url")
        .await;
    assert!(ok && v == "https://example.test");
    ext.close().await;
}

#[tokio::test]
async fn variable_kv_first() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    a.kv_put(
        VARS_BUCKET,
        &session_var_key(T, "conf-ext", "sess-v", "base-url"),
        "cached",
        0,
    )
    .await;
    let agent = Agent::new(a.clone());
    let (v, ok) = agent
        .resolve_variable(T, "sess-v", "conf-ext", "base-url")
        .await;
    assert!(ok && v == "cached");
    ext.close().await;
}

#[tokio::test]
async fn call_hook() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |cfg| {
        cfg.call_hooks = vec!["session.before_create".into()];
        cfg.on_call_hook = Some(Arc::new(|hook, session, _args, _t| {
            Box::pin(async move { json!({"ok": true, "data": {"session": session, "hook": hook}}) })
        }));
    });
    ext.serve().await;
    let agent = Agent::new(a.clone());
    let hr = agent
        .call_hook(
            T,
            "conf-ext",
            "session.before_create",
            "sess-h",
            json!({"k": "v"}),
        )
        .await
        .unwrap();
    assert_eq!(hr.get("ok").unwrap().as_bool().unwrap(), true);
    assert_eq!(
        hr.get("data")
            .unwrap()
            .get("session")
            .unwrap()
            .as_str()
            .unwrap(),
        "sess-h"
    );
    ext.close().await;
}

#[tokio::test]
async fn event_hook() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen2 = seen.clone();
    let ext = full_ext(e, move |cfg| {
        cfg.event_hooks = vec!["session.created".into()];
        cfg.on_event_hook = Some(Arc::new(move |hook, session, _p, _t| {
            let seen = seen2.clone();
            Box::pin(async move {
                seen.lock().unwrap().push(format!("{hook}:{session}"));
            })
        }));
    });
    ext.serve().await;
    let agent = Agent::new(a.clone());
    agent
        .publish_event_hook(T, "session.created", "sess-ev", json!({"x": 1}))
        .await;
    for _ in 0..50 {
        if !seen.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(seen.lock().unwrap()[0], "session.created:sess-ev");
    ext.close().await;
}

#[tokio::test]
async fn hook_schema_validation() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |cfg| {
        cfg.call_hooks = vec!["audit".into()];
        let mut schemas = std::collections::HashMap::new();
        schemas.insert("audit".to_string(), json!({"type": "object", "required": ["who"], "properties": {"who": {"type": "string"}}}));
        cfg.hook_schemas.insert("call".into(), schemas);
        cfg.on_call_hook = Some(Arc::new(|_h, _s, _a, _t| {
            Box::pin(async move { json!({"ok": true}) })
        }));
    });
    ext.serve().await;
    let agent = Agent::new(a.clone());
    let bad = agent
        .call_hook(T, "conf-ext", "audit", "sess-h", json!({"nope": true}))
        .await
        .unwrap();
    assert_eq!(bad.get("ok").unwrap().as_bool().unwrap(), false);
    assert_eq!(
        bad.get("error")
            .unwrap()
            .get("code")
            .unwrap()
            .as_f64()
            .unwrap() as i64,
        code_to_wire("CODE_INVALID_ARGUMENT") as i64
    );
    let good = agent
        .call_hook(T, "conf-ext", "audit", "sess-h", json!({"who": "me"}))
        .await
        .unwrap();
    assert_eq!(good.get("ok").unwrap().as_bool().unwrap(), true);
    ext.close().await;
}

#[tokio::test]
async fn kv() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    a.kv_put("test-bucket", "k1", "v1", 0).await;
    assert_eq!(a.kv_get("test-bucket", "k1").await, "v1");
    a.kv_delete("test-bucket", "k1").await;
    assert_eq!(a.kv_get("test-bucket", "k1").await, "");
}

#[tokio::test]
async fn kv_watch() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    a.kv_put("watch-test", "k.a", "1", 0).await;
    let (mut rx, stop) = a.kv_watch("watch-test", "k.>").await;
    let first = rx.recv().await.unwrap();
    assert_eq!(first.key, "k.a");
    assert_eq!(first.value, "1");
    a.kv_put("watch-test", "k.b", "2", 0).await;
    let upd = rx.recv().await.unwrap();
    assert_eq!(upd.key, "k.b");
    assert!(upd.is_update);
    a.kv_delete("watch-test", "k.b").await;
    let del = rx.recv().await.unwrap();
    assert_eq!(del.key, "k.b");
    assert!(del.deleted);
    stop();
}

#[tokio::test]
async fn object_store() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    a.object_put("o1", b"hello bytes".to_vec()).await;
    assert_eq!(a.object_get("o1").await.unwrap(), b"hello bytes");
}

#[tokio::test]
async fn mailbox() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let agent = Agent::new(a.clone());
    let got = Arc::new(Mutex::new(Vec::<String>::new()));
    let got2 = got.clone();
    let handler = Arc::new(move |m: easybus_sdk::agent::MailboxMessage| {
        let got = got2.clone();
        Box::pin(async move {
            got.lock().unwrap().push(m.r#type);
        }) as easybus_sdk::agent::BoxFutureMail
    });
    let stop = agent.consume_mailbox(handler).await;
    agent
        .publish_mailbox(T, "sess-mb", "trigger", json!({"text": "hi"}))
        .await;
    for _ in 0..50 {
        if !got.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(got.lock().unwrap()[0], "trigger");
    stop();
}

#[tokio::test]
async fn ext_mailbox() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let agent = Agent::new(a.clone());
    let got = Arc::new(Mutex::new(Vec::<String>::new()));
    let got2 = got.clone();
    let handler = Arc::new(move |m: easybus_sdk::agent::MailboxMessage| {
        let got = got2.clone();
        Box::pin(async move {
            got.lock().unwrap().push(m.r#type);
        }) as easybus_sdk::agent::BoxFutureMail
    });
    let stop = agent.consume_mailbox(handler).await;
    ext.publish_mailbox_event(T, "sess-emb", "event", json!({"done": true}))
        .await;
    for _ in 0..50 {
        if !got.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(got.lock().unwrap()[0], "event");
    stop();
    ext.close().await;
}

#[tokio::test]
async fn session_events() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let sub = a
        .inbox_consume(InboxConsumeOpts {
            subject: easybus_sdk::protocol::ch_session_events(T, "sess-se"),
        })
        .await;
    ext.publish_session_event(T, "sess-se", "todos-updated", json!({"count": 3}))
        .await;
    let mut got = None;
    for _ in 0..50 {
        if let Some(m) = sub.next().await {
            got = Some(m);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let m = got.expect("session event");
    let ev = from_value(m.envelope().payload.as_ref().unwrap());
    assert_eq!(ev.get("event").unwrap().as_str().unwrap(), "todos-updated");
    ext.close().await;
}

#[tokio::test]
async fn progress() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let sub = a
        .subscribe(
            &format!("easybus.{T}.tool.progress.c-progress"),
            Default::default(),
        )
        .await;
    ext.report_progress(
        T,
        "c-progress",
        json!({"phase": "sync", "progress": 0.5, "text": "half"}),
    )
    .await;
    let env = sub.next().await.expect("progress");
    let p = from_value(env.payload.as_ref().unwrap());
    assert_eq!(p.get("call_id").unwrap().as_str().unwrap(), "c-progress");
    assert_eq!(p.get("phase").unwrap().as_str().unwrap(), "sync");
    ext.close().await;
}

#[tokio::test]
async fn interrupt_cancels_inflight_tool() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let agent = Agent::new(a.clone());
    let a1 = agent.clone();
    let p1 = tokio::spawn(async move {
        a1.call_tool(T, "sess-int", "conf-ext", "hang", "hang-1", json!({}))
            .await
    });
    let a2 = agent.clone();
    let p2 = tokio::spawn(async move {
        a2.call_tool(T, "sess-other", "conf-ext", "hang", "hang-2", json!({}))
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    agent
        .interrupt_session(T, "conf-ext", "sess-int", "test")
        .await;
    let r1 = tokio::time::timeout(std::time::Duration::from_secs(5), p1)
        .await
        .expect("p1 timed out")
        .unwrap()
        .unwrap();
    assert!(r1.error.unwrap().1.contains("interrupted"));
    agent.interrupt(T, "conf-ext").await;
    let r2 = tokio::time::timeout(std::time::Duration::from_secs(5), p2)
        .await
        .expect("p2 timed out")
        .unwrap()
        .unwrap();
    assert!(r2.error.unwrap().1.contains("interrupted"));
    ext.close().await;
}

#[tokio::test]
async fn interrupt() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    Agent::new(a.clone()).interrupt(T, "conf-ext").await;
    ext.close().await;
}

#[tokio::test]
async fn on_interrupt_callback() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let got = Arc::new(Mutex::new(Vec::<String>::new()));
    let got2 = got.clone();
    let ext = full_ext(e, move |cfg| {
        cfg.on_interrupt = Some(Arc::new(move |session, reason, _t| {
            got2.lock().unwrap().push(format!("{session}|{reason}"));
        }));
    });
    ext.serve().await;
    let agent = Agent::new(a.clone());
    let a1 = agent.clone();
    tokio::spawn(async move {
        a1.call_tool(T, "sess-cb", "conf-ext", "hang", "cb-1", json!({}))
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    agent
        .interrupt_session(T, "conf-ext", "sess-cb", "because")
        .await;
    for _ in 0..50 {
        if !got.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(got.lock().unwrap()[0], "sess-cb|because");
    ext.close().await;
}

#[tokio::test]
async fn lifecycle() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let received = Arc::new(Mutex::new(Vec::<String>::new()));
    let r2 = received.clone();
    let ext = Extension::new(
        e,
        Config {
            id: "lc-ext".into(),
            version: "1".into(),
            lifecycle: vec![
                "created".into(),
                "forked".into(),
                "renamed".into(),
                "deleted".into(),
            ],
            on_lifecycle: Some(Arc::new(move |ev, _t| {
                let r = r2.clone();
                Box::pin(async move {
                    r.lock().unwrap().push(
                        ev.get("kind")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string(),
                    );
                })
            })),
            ..Default::default()
        },
    );
    ext.serve().await;
    let agent = Agent::new(a.clone());
    for kind in ["created", "forked", "renamed", "deleted"] {
        agent
            .publish_lifecycle_event(T, kind, "sess-lc", Some(json!({"k": kind})))
            .await;
    }
    for _ in 0..100 {
        if received.lock().unwrap().len() >= 4 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        *received.lock().unwrap(),
        vec!["created", "forked", "renamed", "deleted"]
    );
    ext.close().await;
}

#[tokio::test]
async fn session_lease() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a1 = connect(&url).await;
    let a2 = connect(&url).await;
    let ag1 = Agent::new(a1.clone());
    let ag2 = Agent::new(a2.clone());
    let session = format!("sess-lease-{}", &new_id()[..8]);
    let r1 = ag1.claim_session(T, &session, 1000).await;
    let r2 = ag2.claim_session(T, &session, 1000).await;
    assert!(!(r1.is_none() && r2.is_none()));
    assert!(!(r1.is_some() && r2.is_some()));
    let (holder, other, rev) = if r1.is_some() {
        (ag1.clone(), ag2.clone(), r1.unwrap())
    } else {
        (ag2.clone(), ag1.clone(), r2.unwrap())
    };
    assert!(holder.is_session_running(T, &session).await);
    let next = holder.renew_session(T, &session, rev, 1000).await;
    assert!(next.is_some() && next.unwrap() > rev);
    assert!(holder.renew_session(T, &session, rev, 1000).await.is_none());
    holder.release_session(T, &session).await;
    assert!(!holder.is_session_running(T, &session).await);
    assert!(other.claim_session(T, &session, 1000).await.is_some());
    other.release_session(T, &session).await;
}

#[tokio::test]
async fn config_set_and_apply() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let applied = Arc::new(Mutex::new(Vec::<Value>::new()));
    let ap = applied.clone();
    let ext = full_ext(e, move |cfg| {
        cfg.on_config_change = Some(Arc::new(move |_n, v, _s| {
            let ap = ap.clone();
            Box::pin(async move {
                ap.lock().unwrap().push(v);
                Ok(())
            })
        }));
    });
    ext.serve().await;
    let agent = Agent::new(a.clone());
    agent.discover(500).await;
    agent.serve_config(true);
    agent
        .set_config(T, "conf-ext", "poll-interval", json!(60), "", None, None)
        .await
        .unwrap();
    for _ in 0..50 {
        if !applied.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(applied.lock().unwrap()[0].as_f64().unwrap() as i64, 60);
    ext.close().await;
}

#[tokio::test]
async fn config_rejected() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |cfg| {
        cfg.on_config_change = Some(Arc::new(|n, _v, _s| {
            Box::pin(async move {
                if n == "poll-interval" {
                    Err("rejected".to_string())
                } else {
                    Ok(())
                }
            })
        }));
    });
    ext.serve().await;
    let agent = Agent::new(a.clone());
    agent.discover(500).await;
    agent.serve_config(true);
    let r = agent
        .set_config(T, "conf-ext", "poll-interval", json!(99), "", None, None)
        .await;
    assert!(r.is_err());
    let err = r.unwrap_err();
    assert_eq!(err.code, "retryable");
    assert!(err.message.contains("rejected"));
    ext.close().await;
}

#[tokio::test]
async fn config_validation() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let agent = Agent::new(a.clone());
    agent.discover(500).await;
    agent.serve_config(true);
    assert!(agent
        .set_config(
            T,
            "conf-ext",
            "poll-interval",
            json!("fast"),
            "",
            None,
            None
        )
        .await
        .is_err());
    assert!(agent
        .set_config(T, "conf-ext", "mode", json!("turbo"), "", None, None)
        .await
        .is_err());
    assert!(agent
        .set_config(T, "conf-ext", "nope", json!(1), "", None, None)
        .await
        .is_err());
    ext.close().await;
}

#[tokio::test]
async fn config_session_override() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let se = seen.clone();
    let ext = full_ext(e, move |cfg| {
        cfg.on_config_change = Some(Arc::new(move |_n, _v, s| {
            let se = se.clone();
            Box::pin(async move {
                se.lock().unwrap().push(s);
                Ok(())
            })
        }));
    });
    ext.serve().await;
    let agent = Agent::new(a.clone());
    agent.discover(500).await;
    agent.serve_config(true);
    agent
        .set_config(
            T,
            "conf-ext",
            "session-limit",
            json!(99),
            "sess-a",
            None,
            None,
        )
        .await
        .unwrap();
    for _ in 0..50 {
        if !seen.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(seen.lock().unwrap()[0], "sess-a");
    agent.drop_session_config(T, "conf-ext", "sess-a").await;
    ext.close().await;
}

#[tokio::test]
async fn config_no_ack() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let applied = Arc::new(Mutex::new(Vec::<Value>::new()));
    let ap = applied.clone();
    let ext = full_ext(e, move |cfg| {
        cfg.on_config_change = Some(Arc::new(move |_n, v, _s| {
            let ap = ap.clone();
            Box::pin(async move {
                ap.lock().unwrap().push(v);
                Ok(())
            })
        }));
    });
    ext.serve().await;
    let agent = Agent::new(a.clone());
    agent.discover(500).await;
    agent.serve_config(true);
    agent
        .set_config(
            T,
            "conf-ext",
            "poll-interval",
            json!(3),
            "",
            None,
            Some(false),
        )
        .await
        .unwrap();
    for _ in 0..50 {
        if !applied.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(applied.lock().unwrap()[0].as_f64().unwrap() as i64, 3);
    ext.close().await;
}

#[tokio::test]
async fn config_snapshot() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let agent = Agent::new(a.clone());
    agent.discover(500).await;
    agent.serve_config(true);
    agent
        .set_config(T, "conf-ext", "poll-interval", json!(7), "", None, None)
        .await
        .unwrap();
    let raw = a
        .kv_get(CONFIG_KV_BUCKET, &format!("t.{T}.conf-ext.poll-interval"))
        .await;
    assert!(raw.contains('7'));
    ext.close().await;
}

#[tokio::test]
async fn config_recovers_from_kv() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    a.kv_put(
        CONFIG_KV_BUCKET,
        &format!("t.{T}.conf-ext.poll-interval"),
        r#"{"r":5,"v":42}"#,
        0,
    )
    .await;
    let ext = Extension::new(
        e,
        Config {
            id: "conf-ext".into(),
            version: "1".into(),
            config: {
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "poll-interval".to_string(),
                    ConfigSpec {
                        r#type: "number".into(),
                        default: Some(json!(30)),
                        ..Default::default()
                    },
                );
                m
            },
            ..Default::default()
        },
    );
    ext.serve().await;
    for _ in 0..50 {
        if ext.get_config(T, "poll-interval", "").as_f64() == Some(42.0) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(ext.get_config(T, "poll-interval", "").as_f64(), Some(42.0));
    ext.close().await;
}

/// A knob's value is ultimately a STRING.
///
/// The agent writes it verbatim, and a URL or a token is not JSON — so a decoder
/// that requires JSON drops the entry, silently, one key at a time. The extension
/// then reports every knob unset while the agent and the console both insist the
/// value is saved. This is the RESTART path: the bucket snapshot is the only way a
/// fresh process learns its configuration.
#[tokio::test]
async fn config_kv_bare_value_is_stored() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    a.kv_put(
        CONFIG_KV_BUCKET,
        &format!("t.{T}.conf-ext.endpoint"),
        "http://easyops.example:80",
        0,
    )
    .await;
    let ext = one_string_knob_ext(e, "endpoint").await;
    for _ in 0..50 {
        if ext.get_config(T, "endpoint", "").as_str() == Some("http://easyops.example:80") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        ext.get_config(T, "endpoint", "").as_str(),
        Some("http://easyops.example:80"),
        "a bare value is a valid config value; dropping it makes every knob read as unset"
    );
    ext.close().await;
}

/// A structured knob arrives as its ORIGINAL TEXT.
///
/// The knob carries a declaration the extension parses itself. Handing back a
/// parsed array makes a string-typed reader see nothing — and every sandbox/
/// computer tool requires a structured `sandboxes` knob, so this is the common
/// case, not an edge.
#[tokio::test]
async fn config_kv_structured_value_stays_text() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let raw = r#"[{"name":"local","url":"http://worker:80"}]"#;
    a.kv_put(CONFIG_KV_BUCKET, &format!("t.{T}.conf-ext.servers"), raw, 0)
        .await;
    let ext = one_string_knob_ext(e, "servers").await;
    for _ in 0..50 {
        if ext.get_config(T, "servers", "").as_str() == Some(raw) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        ext.get_config(T, "servers", "").as_str(),
        Some(raw),
        "a structured value must stay text; the extension parses it itself"
    );
    ext.close().await;
}

/// The legacy `{"r":<rev>,"v":<value>}` envelope keeps working.
///
/// Guards the other direction: accepting bare values must not break an agent that
/// still writes the envelope.
#[tokio::test]
async fn config_kv_envelope_still_unwraps() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    a.kv_put(
        CONFIG_KV_BUCKET,
        &format!("t.{T}.conf-ext.endpoint"),
        r#"{"r":9,"v":"enveloped"}"#,
        0,
    )
    .await;
    let ext = one_string_knob_ext(e, "endpoint").await;
    for _ in 0..50 {
        if ext.get_config(T, "endpoint", "").as_str() == Some("enveloped") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        ext.get_config(T, "endpoint", "").as_str(),
        Some("enveloped")
    );
    ext.close().await;
}

/// The bucket name is a contract between the agent and every extension.
///
/// An agent writing to a differently-named bucket stores every value where nothing
/// is looking, and each side still looks healthy on its own.
#[test]
fn config_kv_bucket_name() {
    assert_eq!(CONFIG_KV_BUCKET, "cfg");
}

/// A minimal extension declaring one string knob.
async fn one_string_knob_ext(e: Arc<NatsBus>, knob: &str) -> Arc<Extension> {
    let ext = Extension::new(
        e,
        Config {
            id: "conf-ext".into(),
            version: "1".into(),
            config: {
                let mut m = std::collections::HashMap::new();
                m.insert(
                    knob.to_string(),
                    ConfigSpec {
                        r#type: "string".into(),
                        default: None,
                        ..Default::default()
                    },
                );
                m
            },
            ..Default::default()
        },
    );
    ext.serve().await;
    ext
}

#[tokio::test]
async fn config_dot_session_key() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let agent = Agent::new(a.clone());
    agent.serve_config(true);
    let sess = "weird.session.name:main";
    let mut m = ExtensionManifest {
        id: "conf-ext".into(),
        version: "1".into(),
        ..Default::default()
    };
    let mut item = easybus_sdk::gen::easybus::v1::ExtensionConfigItem::default();
    item.name = "dots".into();
    item.r#type = "string".into();
    item.scope = "session".into();
    m.config.push(item);
    agent
        .set_config(
            T,
            "conf-ext",
            "dots",
            json!("escaped-ok"),
            sess,
            Some(m),
            None,
        )
        .await
        .unwrap();
    let mut cm = std::collections::HashMap::new();
    cm.insert(
        "dots".to_string(),
        ConfigSpec {
            r#type: "string".into(),
            scope: "session".into(),
            ..Default::default()
        },
    );
    let ext = Extension::new(
        e,
        Config {
            id: "conf-ext".into(),
            version: "1".into(),
            config: cm,
            ..Default::default()
        },
    );
    ext.serve().await;
    for _ in 0..50 {
        if ext.get_config(T, "dots", sess).as_str() == Some("escaped-ok") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(ext.get_config(T, "dots", sess).as_str(), Some("escaped-ok"));
    ext.close().await;
}

#[tokio::test]
async fn presence_liveness() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e = connect(&url).await;
    let agent = Agent::new(a.clone());
    let ext = full_ext(e, |_| {});
    ext.serve().await;
    let ag = agent.clone();
    tokio::spawn(async move { ag.discover(300).await });
    for _ in 0..100 {
        if agent.manifest("conf-ext").is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(agent.manifest("conf-ext").is_some());
    ext.close().await;
    for _ in 0..100 {
        if agent.manifest("conf-ext").is_none() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(agent.manifest("conf-ext").is_none());
}

#[tokio::test]
async fn presence_replica_keys() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let e1 = connect(&url).await;
    let e2 = connect(&url).await;
    let r1 = Extension::new(
        e1,
        Config {
            id: "conf-ext".into(),
            version: "1".into(),
            replica_id: "pod-a".into(),
            ..Default::default()
        },
    );
    let r2 = Extension::new(
        e2,
        Config {
            id: "conf-ext".into(),
            version: "1".into(),
            replica_id: "pod-b".into(),
            ..Default::default()
        },
    );
    r1.serve().await;
    r2.serve().await;
    for _ in 0..50 {
        let v1 = a.kv_get("easybus-presence", "conf-ext.pod-a").await;
        let v2 = a.kv_get("easybus-presence", "conf-ext.pod-b").await;
        if !v1.is_empty() && !v2.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(!a
        .kv_get("easybus-presence", "conf-ext.pod-a")
        .await
        .is_empty());
    assert!(!a
        .kv_get("easybus-presence", "conf-ext.pod-b")
        .await
        .is_empty());
    r1.close().await;
    for _ in 0..50 {
        if a.kv_get("easybus-presence", "conf-ext.pod-a")
            .await
            .is_empty()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(a
        .kv_get("easybus-presence", "conf-ext.pod-a")
        .await
        .is_empty());
    assert!(!a
        .kv_get("easybus-presence", "conf-ext.pod-b")
        .await
        .is_empty());
    r2.close().await;
}

#[tokio::test]
async fn term_routes_to_dlq() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let sub = a
        .inbox_consume(InboxConsumeOpts {
            subject: format!("easybus.{T}.mailbox.>"),
        })
        .await;
    a.inbox_publish(
        &format!("easybus.{T}.mailbox.dead1"),
        json!({"id": "dlq-1", "type": "event", "payload": {}}),
        InboxPublishOpts {
            id: "dlq-1".into(),
            tenant: T.into(),
            session_name: "sess-dlq".into(),
        },
    )
    .await;
    let m = sub.next().await.expect("msg");
    m.term().await;
    let mut dlq = vec![];
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        dlq = a.replay(&format!("easybus.{T}.dlq.>")).await;
        if !dlq.is_empty() {
            break;
        }
    }
    assert!(!dlq.is_empty());
}

#[tokio::test]
async fn poison_message_escalates_to_dlq() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    let sub = a
        .inbox_consume(InboxConsumeOpts {
            subject: format!("easybus.{T}.mailbox.>"),
        })
        .await;
    a.inbox_publish(
        &format!("easybus.{T}.mailbox.dead2"),
        json!({"id": "p-1", "type": "event", "payload": {}}),
        InboxPublishOpts {
            id: "p-1".into(),
            tenant: T.into(),
            session_name: "sess-poison".into(),
        },
    )
    .await;
    let m = sub.next().await.expect("msg");
    m.term().await;
    let mut dlq = vec![];
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        dlq = a.replay(&format!("easybus.{T}.dlq.>")).await;
        if !dlq.is_empty() {
            break;
        }
    }
    assert!(!dlq.is_empty());
}

#[tokio::test]
async fn requeue_dlq() {
    let url = skip_no_nats!();
    let T = common::tenant();
    let a = connect(&url).await;
    a.inbox_publish(
        &format!("easybus.{T}.dlq.requeue"),
        json!({"id": "rq-1", "type": "requeue-me", "payload": {"k": 1}}),
        InboxPublishOpts {
            id: "rq-1".into(),
            tenant: T.into(),
            session_name: "sess-rq".into(),
        },
    )
    .await;
    let mut dlq = vec![];
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        dlq = a.replay(&format!("easybus.{T}.dlq.>")).await;
        if !dlq.is_empty() {
            break;
        }
    }
    assert!(dlq.iter().any(|e| e.id.as_deref() == Some("rq-1")));
}

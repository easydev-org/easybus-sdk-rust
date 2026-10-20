//! A self-contained smoke test: a Rust extension + a Rust agent over a real
//! nats-server.
use easybus_sdk::agent::Agent;
use easybus_sdk::extension::{Config, Extension, FnTool, ToolError, ToolResultData, ToolSpec};
use easybus_sdk::transport::nats::{NatsBus, Options};
use serde_json::{json, Value};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let a = NatsBus::connect(&url, Options::default()).await.unwrap();
    let e = NatsBus::connect(&url, Options::default()).await.unwrap();

    let echo = FnTool(
        |args: Value, _cid: String, _sess: String, _ten: String, _c: Arc<tokio::sync::Notify>| async move {
            let msg = args.get("msg").and_then(|x| x.as_str()).unwrap_or("");
            Ok(ToolResultData {
                content: format!("echo:{msg}"),
                ..Default::default()
            })
        },
    );
    let boom = FnTool(
        |_a: Value, _c: String, _s: String, _t: String, _cc: Arc<tokio::sync::Notify>| async move {
            Err(ToolError::Typed(easybus_sdk::extension::TypedError::new(
                "CODE_BUSINESS",
                "nope",
            )))
        },
    );

    let mut cfg = Config {
        id: "conf-ext".into(),
        version: "1".into(),
        ..Default::default()
    };
    cfg.tools
        .insert("echo".into(), (ToolSpec::default(), Arc::new(echo)));
    cfg.tools
        .insert("boom".into(), (ToolSpec::default(), Arc::new(boom)));

    let ext = Extension::new(e.clone(), cfg);
    ext.serve().await;

    let agent = Agent::new(a.clone());
    let ms = agent.discover(800).await;
    println!(
        "discover: {:?}",
        ms.iter().map(|m| m.id.clone()).collect::<Vec<_>>()
    );

    let r = agent
        .call_tool(
            "t1",
            "sess-1",
            "conf-ext",
            "echo",
            "c1",
            json!({"msg": "hi"}),
        )
        .await
        .unwrap();
    println!("echo content: {}", r.content);

    let r = agent
        .call_tool("t1", "sess-1", "conf-ext", "boom", "c2", json!({}))
        .await
        .unwrap();
    println!("boom error: {:?}", r.error);

    ext.close().await;
}

//! Rust-side interop agent: discovers a Go extension (go-ext) and asserts the
//! cross-language wire contract.
use easybus_sdk::agent::Agent;
use easybus_sdk::transport::nats::{NatsBus, Options};
use serde_json::json;

fn expect(cond: bool, msg: &str) {
    if !cond {
        println!("[rs-agent] FAIL: {msg}");
        std::process::exit(1);
    }
    println!("[rs-agent] ok: {msg}");
}

#[tokio::main]
async fn main() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let bus = NatsBus::connect(&url, Options::default()).await.unwrap();
    let agent = Agent::new(bus);
    println!("[rs-agent] connected to {url}");

    let ms = agent.discover(1200).await;
    expect(ms.iter().any(|m| m.id == "go-ext"), "discover sees go-ext");

    let tr = agent.call_tool("i1", "sess-x", "go-ext", "echo", "c1", json!({"msg": "hello-from-rs"})).await.unwrap();
    expect(tr.content == "go-ext echo: hello-from-rs", "tool echo content");

    let s = agent.call_tool("i1", "sess-x", "go-ext", "session", "c2", json!({})).await.unwrap();
    expect(s.content == "session=sess-x", "session name");

    let e = agent.call_tool("i1", "sess-x", "go-ext", "fail", "c3", json!({})).await.unwrap();
    let (code, msg) = e.error.clone().unwrap_or_default();
    expect(code == "CODE_BUSINESS" && msg == "deliberate failure", "business error mapping");

    agent.publish_event_hook("i1", "interop.event", "sess-rs", json!({"from": "rs-agent"})).await;
    println!("[rs-agent] ok: event hook published (check go-ext stdout)");
    println!("[rs-agent] ALL INTEROP CHECKS PASSED");
    agent.close().await;
}

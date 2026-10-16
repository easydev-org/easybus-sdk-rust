//! Rust-side interop extension: serves echo/add/session/fail + a variable, a
//! call hook and an event hook over NATS, so a Go/TS/Python agent can drive it.
use easybus_sdk::extension::{Config, ConfigSpec, Extension, FnTool, ToolError, ToolResultData, ToolSpec, TypedError, VariableSpec};
use easybus_sdk::transport::nats::{NatsBus, Options};
use serde_json::{json, Value};
use std::sync::Arc;

fn tool<F, Fut>(f: F) -> (ToolSpec, Arc<dyn easybus_sdk::extension::ToolHandler>)
where
    F: Fn(Value, String, String, String, Arc<tokio::sync::Notify>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<ToolResultData, ToolError>> + Send + 'static,
{
    (ToolSpec::default(), Arc::new(FnTool(f)))
}

#[tokio::main]
async fn main() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let bus = NatsBus::connect(&url, Options::default()).await.unwrap();

    let mut cfg = Config {
        id: "rs-ext".into(),
        version: "1.0".into(),
        call_hooks: vec!["interop.before".into()],
        event_hooks: vec!["interop.event".into()],
        ..Default::default()
    };
    cfg.tools.insert("echo".into(), tool(|args: Value, _c, _s, _t, _x| async move {
        Ok(ToolResultData { content: format!("rs-ext echo: {}", args.get("msg").and_then(|v| v.as_str()).unwrap_or("")), ..Default::default() })
    }));
    cfg.tools.insert("add".into(), tool(|args: Value, _c, _s, _t, _x| async move {
        let a = args.get("a").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let b = args.get("b").and_then(|v| v.as_f64()).unwrap_or(0.0);
        Ok(ToolResultData { data: Some(json!({"sum": a + b})), ..Default::default() })
    }));
    cfg.tools.insert("session".into(), tool(|_a: Value, _c, s: String, _t, _x| async move {
        Ok(ToolResultData { content: format!("session={s}"), ..Default::default() })
    }));
    cfg.tools.insert("fail".into(), tool(|_a: Value, _c, _s, _t, _x| async move {
        Err(ToolError::Typed(TypedError::new("CODE_BUSINESS", "deliberate failure")))
    }));
    cfg.variables.insert("rs-var".into(), VariableSpec { description: "rs var".into(), scope: "global".into() });
    cfg.config.insert("poll-interval".into(), ConfigSpec { r#type: "number".into(), default: Some(json!(30)), ..Default::default() });
    cfg.on_config_change = Some(Arc::new(|name, value, _sess| Box::pin(async move {
        println!("[rs-ext] config applied: {name}={value}");
        Ok(())
    })));
    cfg.on_call_hook = Some(Arc::new(|hook, session, _args, _tenant| Box::pin(async move {
        json!({"ok": true, "data": {"session": session, "hook": hook}})
    })));
    cfg.on_event_hook = Some(Arc::new(|hook, session, payload, _tenant| Box::pin(async move {
        println!("[rs-ext] event hook={hook} session={session} payload={payload}");
    })));

    let ext = Extension::new(bus.clone(), cfg);
    ext.serve().await;
    ext.set_variable("i1", "rs-var", "from-rs-ext").await;
    println!("[rs-ext] serving rs-ext over {url}");
    // Serve until killed (the interop harness sends SIGKILL).
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
    #[allow(unreachable_code)]
    ext.close().await;
}

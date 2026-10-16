//! The extension role — the Rust twin of the Go `easybus-sdk/extension`
//! package. An Extension registers on the bus (discovery + tool/variable/config/
//! hook/lifecycle/interrupt channels), heartbeats a per-replica presence key,
//! and executes tool calls.

use crate::bus::{Bus, InboxPublishOpts, PublishOpts, SubscribeOpts};
use crate::gen::easybus::v1 as pb;
use crate::protocol::{
    new_id, session_var_key, subject_tenant, tenant_object_name, var_key, CONFIG_KV_BUCKET,
    GLOBAL_TENANT, LIFECYCLE_WILDCARD, CH_DISCOVER, VARS_BUCKET,
};
use crate::wire::{
    code_to_wire, from_value, lifecycle_kind_string, to_value,
};
use async_trait::async_trait;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const MAX_TOOL_CONTENT_BYTES: usize = 128 * 1024;
pub const PRESENCE_BUCKET: &str = "easybus-presence";
pub const PRESENCE_TTL_MS: u64 = 15_000;
pub const PRESENCE_INTERVAL_MS: u64 = 5_000;

pub const EASYBUS_FEATURES: &[&str] = &[
    "dlq",
    "config-kv",
    "presence",
    "kv-escaping",
    "interrupt-abort",
    "progress",
    "multitenant",
];

/// Carries a standard protocol error code from a tool execution.
#[derive(Debug, Clone)]
pub struct TypedError {
    pub code: String,
    pub message: String,
}

impl TypedError {
    pub fn new(code: &str, message: &str) -> Self {
        TypedError {
            code: code.to_string(),
            message: message.to_string(),
        }
    }
}

/// The data a tool handler returns.
#[derive(Debug, Default, Clone)]
pub struct ToolResultData {
    pub content: String,
    pub data: Option<Value>,
    pub object: Option<(String, String)>, // (id, content_type)
}

/// A tool handler: (args, call_id, session, tenant, cancel) -> result.
#[async_trait]
pub trait ToolHandler: Send + Sync {
    async fn execute(
        &self,
        args: Value,
        call_id: &str,
        session_name: &str,
        tenant: &str,
        cancel: Arc<tokio::sync::Notify>,
    ) -> Result<ToolResultData, ToolError>;
}

/// A tool error (TypedError or internal).
#[derive(Debug, Clone)]
pub enum ToolError {
    Typed(TypedError),
    Internal(String),
}

impl ToolError {
    pub fn internal(msg: impl Into<String>) -> Self {
        ToolError::Internal(msg.into())
    }
}

#[derive(Clone, Default)]
pub struct ToolSpec {
    pub description: String,
    pub descriptions: HashMap<String, String>,
    pub input_schema: Value,
    pub required_config: Vec<String>,
}

#[derive(Clone, Default)]
pub struct VariableSpec {
    pub description: String,
    pub scope: String,
}

#[derive(Clone, Default)]
pub struct ConfigSpec {
    pub r#type: String,
    pub kind: String,
    pub capability: String,
    pub enum_values: Vec<String>,
    pub default: Option<Value>,
    pub description: String,
    pub scope: String,
}

/// The extension configuration + handlers.
#[derive(Default)]
pub struct Config {
    pub id: String,
    pub version: String,
    pub replica_id: String,
    pub tools: HashMap<String, (ToolSpec, Arc<dyn ToolHandler>)>,
    pub variables: HashMap<String, VariableSpec>,
    pub config: HashMap<String, ConfigSpec>,
    pub call_hooks: Vec<String>,
    pub event_hooks: Vec<String>,
    pub lifecycle: Vec<String>,
    pub hook_schemas: HashMap<String, HashMap<String, Value>>,
    pub on_call_hook: Option<CallHookFn>,
    pub on_event_hook: Option<EventHookFn>,
    pub on_config_change: Option<ConfigChangeFn>,
    pub on_lifecycle: Option<LifecycleFn>,
    pub on_interrupt: Option<InterruptFn>,
}

pub type CallHookFn = Arc<dyn Fn(String, String, Value, String) -> BoxFutureHook + Send + Sync>;
pub type EventHookFn = Arc<dyn Fn(String, String, Value, String) -> BoxFutureEvent + Send + Sync>;
pub type ConfigChangeFn = Arc<dyn Fn(String, Value, String) -> BoxFutureConfig + Send + Sync>;
pub type LifecycleFn = Arc<dyn Fn(Value, String) -> BoxFutureLife + Send + Sync>;
pub type InterruptFn = Arc<dyn Fn(String, String, String) + Send + Sync>;

pub type BoxFutureHook = std::pin::Pin<Box<dyn std::future::Future<Output = Value> + Send>>;
pub type BoxFutureEvent = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;
pub type BoxFutureConfig = std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>;
pub type BoxFutureLife = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// The extension-side role.
pub struct Extension {
    bus: Arc<dyn Bus>,
    cfg: Arc<Config>,
    replica_id: String,
    manifest: pb::ExtensionManifest,
    stopped: Arc<AtomicBool>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// tenant\x00session -> call_id -> cancel
    inflight: Arc<Mutex<HashMap<String, HashMap<String, Arc<tokio::sync::Notify>>>>>,
    /// tenant -> name -> value (global) and tenant -> session -> name -> value
    config_store: Arc<Mutex<ConfigStore>>,
}

#[derive(Default)]
pub struct ConfigStore {
    pub global: HashMap<String, HashMap<String, Value>>,
    pub session: HashMap<String, HashMap<String, HashMap<String, Value>>>,
}

impl ConfigStore {
    pub fn get(&self, cfg: &HashMap<String, ConfigSpec>, tenant: &str, name: &str, session_name: &str) -> Value {
        if !session_name.is_empty() {
            if let Some(v) = self.session.get(tenant).and_then(|s| s.get(session_name)).and_then(|n| n.get(name)) {
                return v.clone();
            }
        }
        if let Some(v) = self.global.get(tenant).and_then(|n| n.get(name)) {
            return v.clone();
        }
        cfg.get(name).and_then(|s| s.default.clone()).unwrap_or(Value::Null)
    }
    pub fn apply_set(&mut self, tenant: &str, name: &str, value: Value, session_name: Option<&str>) {
        match session_name {
            Some(s) => {
                self.session
                    .entry(tenant.to_string())
                    .or_default()
                    .entry(s.to_string())
                    .or_default()
                    .insert(name.to_string(), value);
            }
            None => {
                self.global.entry(tenant.to_string()).or_default().insert(name.to_string(), value);
            }
        }
    }
    pub fn rollback(&mut self, tenant: &str, name: &str, session_name: Option<&str>) {
        match session_name {
            Some(s) => {
                if let Some(m) = self.session.get_mut(tenant).and_then(|x| x.get_mut(s)) {
                    m.remove(name);
                }
            }
            None => {
                if let Some(m) = self.global.get_mut(tenant) {
                    m.remove(name);
                }
            }
        }
    }
}

fn resolve_replica_id(explicit: &str) -> String {
    let raw = if !explicit.is_empty() {
        explicit.to_string()
    } else {
        std::env::var("EASYBUS_REPLICA_ID")
            .or_else(|_| std::env::var("POD_NAME"))
            .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string())
    };
    let s: String = raw
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
        .collect();
    if s.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        s
    }
}

/// The small declarative JSON-schema subset (returns "" when valid).
pub fn validate_hook_schema(schema: Option<&Value>, value: &Value, path: &str) -> String {
    let Some(s) = schema else { return String::new() };
    let Some(o) = s.as_object() else { return String::new() };
    if let Some(t) = o.get("type").and_then(|x| x.as_str()) {
        let ok = match t {
            "string" => value.is_string(),
            "number" => value.is_number(),
            "boolean" => value.is_boolean(),
            "object" => value.is_object(),
            _ => true,
        };
        if !ok {
            return format!("{path}: expected {t}");
        }
    }
    if o.get("type").and_then(|x| x.as_str()) == Some("object") {
        if let Some(v) = value.as_object() {
            if let Some(req) = o.get("required").and_then(|x| x.as_array()) {
                for r in req {
                    if let Some(name) = r.as_str() {
                        if !v.contains_key(name) {
                            return format!("{path}: missing required field \"{name}\"");
                        }
                    }
                }
            }
            if let Some(props) = o.get("properties").and_then(|x| x.as_object()) {
                for (name, p) in props {
                    if let Some(val) = v.get(name) {
                        let msg = validate_hook_schema(Some(p), val, &format!("{path}.{name}"));
                        if !msg.is_empty() {
                            return msg;
                        }
                    }
                }
            }
        }
    }
    if let Some(enum_vals) = o.get("enum").and_then(|x| x.as_array()) {
        let vs = value.to_string();
        if !enum_vals.iter().any(|e| e.to_string() == vs || e.as_str() == value.as_str()) {
            return format!("{path}: value not in the declared enum");
        }
    }
    String::new()
}

impl Extension {
    pub fn new(bus: Arc<dyn Bus>, cfg: Config) -> Arc<Self> {
        let replica_id = resolve_replica_id(&cfg.replica_id);
        let cfg = Arc::new(cfg);
        let manifest = build_manifest(&cfg);
        Arc::new(Extension {
            bus,
            cfg,
            replica_id,
            manifest,
            stopped: Arc::new(AtomicBool::new(false)),
            tasks: Mutex::new(vec![]),
            inflight: Arc::new(Mutex::new(HashMap::new())),
            config_store: Arc::new(Mutex::new(ConfigStore::default())),
        })
    }

    pub fn id(&self) -> &str {
        &self.cfg.id
    }
    pub fn replica_id(&self) -> &str {
        &self.replica_id
    }
    pub fn manifest(&self) -> &pb::ExtensionManifest {
        &self.manifest
    }

    fn presence_key(&self) -> String {
        format!("{}.{}", self.cfg.id, self.replica_id)
    }

    pub async fn serve(self: &Arc<Self>) {
        self.start_presence().await;
        self.serve_discovery().await;
        self.serve_config().await;
        self.serve_tools().await;
        self.serve_variables().await;
        self.serve_call_hooks().await;
        self.serve_event_hooks().await;
        self.serve_lifecycle().await;
        self.serve_interrupt().await;
    }

    pub async fn close(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
        self.bus.kv_delete(PRESENCE_BUCKET, &self.presence_key()).await;
        self.bus.close().await;
    }

    fn spawn(&self, fut: impl std::future::Future<Output = ()> + Send + 'static) {
        self.tasks.lock().unwrap().push(tokio::spawn(fut));
    }

    async fn start_presence(self: &Arc<Self>) {
        let me = self.clone();
        let put = {
            let me = me.clone();
            move || {
                let me = me.clone();
                async move {
                    let json = crate::wire::encode_manifest(&me.manifest);
                    me.bus.kv_put(PRESENCE_BUCKET, &me.presence_key(), &String::from_utf8_lossy(&json), PRESENCE_TTL_MS).await;
                }
            }
        };
        put().await;
        self.spawn(async move {
            while !me.stopped.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(PRESENCE_INTERVAL_MS)).await;
                put().await;
            }
        });
    }

    pub async fn report_progress(&self, tenant: &str, call_id: &str, progress: Value) {
        let mut body = progress;
        if let Some(o) = body.as_object_mut() {
            o.insert("call_id".into(), json!(call_id));
        }
        self.bus
            .publish(
                &crate::protocol::ch_tool_progress(tenant, call_id),
                body,
                PublishOpts {
                    tenant: tenant.to_string(),
                    ..Default::default()
                },
            )
            .await;
    }

    async fn serve_discovery(self: &Arc<Self>) {
        let sub = self
            .bus
            .subscribe(CH_DISCOVER, SubscribeOpts { queue: self.cfg.id.clone() })
            .await;
        let me = self.clone();
        self.spawn(async move {
            while !me.stopped.load(Ordering::Relaxed) {
                if let Some(env) = sub.next().await {
                    if let Some(reply) = &env.reply_to {
                        let payload: Value = serde_json::from_slice(&crate::wire::encode_manifest(&me.manifest)).unwrap_or(Value::Null);
                        me.bus
                            .publish(reply, payload, PublishOpts { tenant: GLOBAL_TENANT.to_string(), ..Default::default() })
                            .await;
                    }
                }
            }
        });
    }

    async fn serve_tools(self: &Arc<Self>) {
        for (name, (spec, handler)) in self.cfg.tools.clone() {
            let sub = self
                .bus
                .subscribe(&format!("easybus.*.tool.call.{}.{}", self.cfg.id, name), SubscribeOpts { queue: self.cfg.id.clone() })
                .await;
            let me = self.clone();
            let handler = handler.clone();
            self.spawn(async move {
                while !me.stopped.load(Ordering::Relaxed) {
                    let Some(env) = sub.next().await else { continue };
                    let Some(reply_to) = env.reply_to.clone() else { continue };
                    let spec = spec.clone();
                    let me2 = me.clone();
                    let handler = handler.clone();
                    let name = name.clone();
                    tokio::spawn(async move {
                        me2.handle_tool_call(&name, &spec, handler, env, &reply_to).await;
                    });
                }
            });
        }
    }

    async fn handle_tool_call(
        self: &Arc<Self>,
        name: &str,
        spec: &ToolSpec,
        handler: Arc<dyn ToolHandler>,
        env: pb::Envelope,
        reply_to: &str,
    ) {
        let call = from_value(env.payload.as_ref().unwrap_or(&prost_types::Value { kind: None }));
        let session_name = env.session_name.clone().unwrap_or_default();
        let tenant = if env.tenant.is_empty() { subject_tenant(&env.ch) } else { env.tenant.clone() };
        let call_id = call.get("call_id").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let cancel = Arc::new(tokio::sync::Notify::new());
        if !session_name.is_empty() {
            self.inflight
                .lock()
                .unwrap()
                .entry(format!("{tenant}\x00{session_name}"))
                .or_default()
                .insert(call_id.clone(), cancel.clone());
        }
        let mut res = pb::ToolResult {
            call_id: call_id.clone(),
            tool: name.to_string(),
            ..Default::default()
        };
        let args = call.get("arguments").cloned().unwrap_or(json!({}));
        match handler.execute(args, &call_id, &session_name, &tenant, cancel).await {
            Ok(data) => {
                if data.content.len() > MAX_TOOL_CONTENT_BYTES {
                    res.error = Some(pb::ToolError {
                        code: code_to_wire("CODE_INVALID_ARGUMENT"),
                        message: format!("tool '{name}' returned {} bytes of text, over the {MAX_TOOL_CONTENT_BYTES}-byte limit; return large data as a file (data.files) instead", data.content.len()),
                    });
                } else if !data.content.is_empty() {
                    res.content = Some(data.content);
                }
                if res.error.is_none() {
                    if let Some(d) = data.data {
                        res.data = Some(to_value(&d));
                    }
                }
                if res.error.is_none() {
                    if let Some((id, ct)) = data.object {
                        res.object = Some(pb::ObjectRef {
                            id,
                            content_type: if ct.is_empty() { None } else { Some(ct) },
                        });
                    }
                }
            }
            Err(ToolError::Typed(e)) => {
                res.error = Some(pb::ToolError {
                    code: code_to_wire(&e.code),
                    message: e.message,
                });
            }
            Err(ToolError::Internal(m)) => {
                res.error = Some(pb::ToolError {
                    code: code_to_wire("CODE_INTERNAL"),
                    message: m,
                });
            }
        }
        if !session_name.is_empty() {
            if let Some(m) = self.inflight.lock().unwrap().get_mut(&format!("{tenant}\x00{session_name}")) {
                m.remove(&call_id);
            }
        }
        let _ = spec;
        let payload: Value = serde_json::from_slice(&crate::wire::encode_tool_result(&res)).unwrap_or(Value::Null);
        self.bus.publish(reply_to, payload, PublishOpts { tenant: tenant.clone(), ..Default::default() }).await;
    }

    async fn serve_variables(self: &Arc<Self>) {
        for (name, spec) in self.cfg.variables.clone() {
            let sub = self
                .bus
                .subscribe(&format!("easybus.*.var.{}.{}", self.cfg.id, name), SubscribeOpts { queue: self.cfg.id.clone() })
                .await;
            let me = self.clone();
            self.spawn(async move {
                while !me.stopped.load(Ordering::Relaxed) {
                    let Some(env) = sub.next().await else { continue };
                    let Some(reply_to) = env.reply_to.clone() else { continue };
                    let tenant = if env.tenant.is_empty() { subject_tenant(&env.ch) } else { env.tenant.clone() };
                    // A resolver returns the value; here we echo the KV value.
                    let _ = &spec;
                    let v = me.bus.kv_get(VARS_BUCKET, &var_key(&tenant, &me.cfg.id, &name)).await;
                    me.bus
                        .publish(&reply_to, json!({"name": name, "value": v}), PublishOpts { tenant, ..Default::default() })
                        .await;
                }
            });
        }
    }

    async fn serve_call_hooks(self: &Arc<Self>) {
        for hook in self.cfg.call_hooks.clone() {
            let sub = self
                .bus
                .subscribe(&format!("easybus.*.hook.call.{}.{}", self.cfg.id, hook), SubscribeOpts { queue: self.cfg.id.clone() })
                .await;
            let me = self.clone();
            self.spawn(async move {
                while !me.stopped.load(Ordering::Relaxed) {
                    let Some(env) = sub.next().await else { continue };
                    let Some(reply_to) = env.reply_to.clone() else { continue };
                    let call = from_value(env.payload.as_ref().unwrap_or(&prost_types::Value { kind: None }));
                    let session_name = call.get("session_name").and_then(|x| x.as_str()).unwrap_or("").to_string();
                    let tenant = if env.tenant.is_empty() { subject_tenant(&env.ch) } else { env.tenant.clone() };
                    let args = call.get("arguments").cloned().unwrap_or(json!({}));
                    let schema = me.cfg.hook_schemas.get("call").and_then(|m| m.get(&hook));
                    let msg = validate_hook_schema(schema, &args, "arguments");
                    let res = if !msg.is_empty() {
                        json!({"ok": false, "error": {"code": code_to_wire("CODE_INVALID_ARGUMENT"), "message": msg}})
                    } else if let Some(f) = &me.cfg.on_call_hook {
                        f(hook.clone(), session_name.clone(), args, tenant.clone()).await
                    } else {
                        json!({"ok": false, "error": {"code": code_to_wire("CODE_NOT_FOUND"), "message": format!("no handler for hook {hook}")}})
                    };
                    me.bus.publish(&reply_to, res, PublishOpts { tenant, ..Default::default() }).await;
                }
            });
        }
    }

    async fn serve_event_hooks(self: &Arc<Self>) {
        for hook in self.cfg.event_hooks.clone() {
            let sub = self
                .bus
                .subscribe(&format!("easybus.*.hook.event.{}", hook), SubscribeOpts { queue: self.cfg.id.clone() })
                .await;
            let me = self.clone();
            self.spawn(async move {
                while !me.stopped.load(Ordering::Relaxed) {
                    let Some(env) = sub.next().await else { continue };
                    let ev = from_value(env.payload.as_ref().unwrap_or(&prost_types::Value { kind: None }));
                    let session_name = ev.get("session_name").and_then(|x| x.as_str()).unwrap_or("").to_string();
                    let tenant = if env.tenant.is_empty() { subject_tenant(&env.ch) } else { env.tenant.clone() };
                    let schema = me.cfg.hook_schemas.get("event").and_then(|m| m.get(&hook));
                    let payload = ev.get("payload").cloned().unwrap_or(Value::Null);
                    if !validate_hook_schema(schema, &payload, "payload").is_empty() {
                        continue;
                    }
                    if let Some(f) = &me.cfg.on_event_hook {
                        f(hook.clone(), session_name, payload, tenant).await;
                    }
                }
            });
        }
    }

    async fn serve_lifecycle(self: &Arc<Self>) {
        if self.cfg.lifecycle.is_empty() || self.cfg.on_lifecycle.is_none() {
            return;
        }
        let wanted: std::collections::HashSet<String> = self.cfg.lifecycle.iter().cloned().collect();
        let sub = self
            .bus
            .subscribe(&format!("{LIFECYCLE_WILDCARD}>"), SubscribeOpts { queue: self.cfg.id.clone() })
            .await;
        let me = self.clone();
        self.spawn(async move {
            while !me.stopped.load(Ordering::Relaxed) {
                let Some(env) = sub.next().await else { continue };
                let mut ev = from_value(env.payload.as_ref().unwrap_or(&prost_types::Value { kind: None }));
                let tenant = if env.tenant.is_empty() { subject_tenant(&env.ch) } else { env.tenant.clone() };
                let kind_num = ev.get("kind").and_then(|x| x.as_f64()).unwrap_or(0.0) as i32;
                let kind = lifecycle_kind_string(kind_num).to_string();
                if !wanted.contains(&kind) {
                    continue;
                }
                if kind == "deleted" {
                    let session_name = ev.get("session_name").and_then(|x| x.as_str()).unwrap_or("");
                    me.delete_session_variables(&tenant, session_name).await;
                }
                if let Some(o) = ev.as_object_mut() {
                    o.insert("kind".into(), json!(kind));
                }
                if let Some(f) = &me.cfg.on_lifecycle {
                    f(ev, tenant).await;
                }
            }
        });
    }

    async fn serve_interrupt(self: &Arc<Self>) {
        let sub = self
            .bus
            .subscribe(&format!("easybus.*.ctl.interrupt.{}", self.cfg.id), SubscribeOpts { queue: self.cfg.id.clone() })
            .await;
        let me = self.clone();
        self.spawn(async move {
            while !me.stopped.load(Ordering::Relaxed) {
                let Some(env) = sub.next().await else { continue };
                let sig = from_value(env.payload.as_ref().unwrap_or(&prost_types::Value { kind: None }));
                let session_name = sig.get("session_name").and_then(|x| x.as_str()).unwrap_or("").to_string();
                let tenant = if env.tenant.is_empty() { subject_tenant(&env.ch) } else { env.tenant.clone() };
                me.cancel_inflight(&tenant, &session_name);
                let reason = sig.get("reason").and_then(|x| x.as_str()).unwrap_or("").to_string();
                if let Some(f) = &me.cfg.on_interrupt {
                    f(session_name, reason, tenant);
                }
            }
        });
    }

    fn cancel_inflight(&self, tenant: &str, session: &str) {
        let mut m = self.inflight.lock().unwrap();
        if !session.is_empty() {
            if let Some(map) = m.remove(&format!("{tenant}\x00{session}")) {
                for c in map.values() {
                    c.notify_waiters();
                }
            }
        } else if !tenant.is_empty() {
            let keys: Vec<String> = m.keys().filter(|k| k.starts_with(&format!("{tenant}\x00"))).cloned().collect();
            for k in keys {
                if let Some(map) = m.remove(&k) {
                    for c in map.values() {
                        c.notify_waiters();
                    }
                }
            }
        } else {
            for (_, map) in m.drain() {
                for c in map.values() {
                    c.notify_waiters();
                }
            }
        }
    }

    // ---- session-facing helpers -------------------------------------------

    pub async fn publish_session_event(&self, tenant: &str, session_name: &str, event: &str, params: Value) {
        let id = new_id();
        self.bus
            .inbox_publish(
                &crate::protocol::ch_session_events(tenant, session_name),
                json!({"event": event, "params": params, "eid": id}),
                InboxPublishOpts {
                    id: id.clone(),
                    session_name: session_name.to_string(),
                    tenant: tenant.to_string(),
                },
            )
            .await;
    }

    pub async fn publish_mailbox_event(&self, tenant: &str, session_name: &str, typ: &str, payload: Value) {
        let id = new_id();
        self.bus
            .inbox_publish(
                &crate::protocol::ch_mailbox(tenant, session_name),
                json!({"id": id, "type": if typ.is_empty() { "event" } else { typ }, "payload": payload}),
                InboxPublishOpts {
                    id: id.clone(),
                    session_name: session_name.to_string(),
                    tenant: tenant.to_string(),
                },
            )
            .await;
    }

    pub async fn put_object(&self, tenant: &str, name: &str, data: Vec<u8>) {
        self.bus.object_put(&tenant_object_name(tenant, name), data).await;
    }
    pub async fn get_object(&self, tenant: &str, name: &str) -> Option<Vec<u8>> {
        self.bus.object_get(&tenant_object_name(tenant, name)).await
    }
    pub async fn set_variable(&self, tenant: &str, name: &str, value: &str) {
        self.bus.kv_put(VARS_BUCKET, &var_key(tenant, &self.cfg.id, name), value, 0).await;
    }
    pub async fn set_session_variable(&self, tenant: &str, session_name: &str, name: &str, value: &str) {
        self.bus
            .kv_put(VARS_BUCKET, &session_var_key(tenant, &self.cfg.id, session_name, name), value, 0)
            .await;
    }
    pub async fn delete_session_variables(&self, tenant: &str, session_name: &str) {
        for (name, spec) in &self.cfg.variables {
            if spec.scope == "session" {
                self.bus.kv_delete(VARS_BUCKET, &session_var_key(tenant, &self.cfg.id, session_name, name)).await;
            }
        }
    }
    pub fn get_config(&self, tenant: &str, name: &str, session_name: &str) -> Value {
        self.config_store.lock().unwrap().get(&self.cfg.config, tenant, name, session_name)
    }

    async fn serve_config(self: &Arc<Self>) {
        if self.cfg.config.is_empty() {
            return;
        }
        // KV recovery.
        let (mut rx, stop) = self.bus.kv_watch(CONFIG_KV_BUCKET, &format!("t.*.{}.>", self.cfg.id)).await;
        let me = self.clone();
        self.spawn(async move {
            while let Some(ev) = rx.recv().await {
                if ev.done {
                    continue;
                }
                me.apply_config_kv(&ev);
            }
        });
        let _ = stop;
        let sub = self
            .bus
            .subscribe(&format!("easybus.*.config.{}", self.cfg.id), SubscribeOpts { queue: self.cfg.id.clone() })
            .await;
        let me = self.clone();
        self.spawn(async move {
            while !me.stopped.load(Ordering::Relaxed) {
                let Some(env) = sub.next().await else { continue };
                let Some(reply_to) = env.reply_to.clone() else { continue };
                let raw = from_value(env.payload.as_ref().unwrap_or(&prost_types::Value { kind: None }));
                let name = raw.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string();
                let scope = raw.get("scope").and_then(|x| x.as_str()).unwrap_or("global").to_string();
                let session_name = raw
                    .get("session_name")
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| env.session_name.clone())
                    .unwrap_or_default();
                let value = raw.get("value").cloned().unwrap_or(Value::Null);
                let ack = raw.get("ack").and_then(|x| x.as_bool()).unwrap_or(false);
                let tenant = if env.tenant.is_empty() { subject_tenant(&env.ch) } else { env.tenant.clone() };
                let sess = if scope == "session" { Some(session_name.as_str()) } else { None };
                me.config_store.lock().unwrap().apply_set(&tenant, &name, value.clone(), sess);
                let mut reject: Option<String> = None;
                if let Some(f) = &me.cfg.on_config_change {
                    if let Err(e) = f(name.clone(), value, session_name.clone()).await {
                        reject = Some(e);
                    }
                }
                if reject.is_some() {
                    me.config_store.lock().unwrap().rollback(&tenant, &name, sess);
                }
                if ack {
                    let res = match reject {
                        Some(msg) => json!({"ok": false, "error": {"code": code_to_wire("CODE_INTERNAL"), "message": msg}}),
                        None => json!({"ok": true}),
                    };
                    me.bus.publish(&reply_to, res, PublishOpts { tenant, ..Default::default() }).await;
                }
            }
        });
    }

    fn apply_config_kv(&self, ev: &crate::bus::KvEvent) {
        let segs: Vec<&str> = ev.key.split('.').collect();
        if segs.len() < 3 || segs[0] != "t" {
            return;
        }
        let tenant = segs[1].to_string();
        let mut rest = segs[2..].join(".");
        let prefix = format!("{}.", self.cfg.id);
        if let Some(stripped) = rest.strip_prefix(&prefix) {
            rest = stripped.to_string();
        }
        let parts: Vec<&str> = rest.split('.').collect();
        let value: Value = if ev.deleted {
            Value::Null
        } else {
            match serde_json::from_str::<Value>(&ev.value) {
                Ok(parsed) => parsed.get("v").cloned().unwrap_or(parsed),
                Err(_) => return,
            }
        };
        let mut store = self.config_store.lock().unwrap();
        if parts.len() == 1 {
            let m = store.global.entry(tenant).or_default();
            if ev.deleted {
                m.remove(parts[0]);
            } else {
                m.insert(parts[0].to_string(), value);
            }
        } else if parts.len() >= 2 {
            let session = crate::protocol::unescape_kv_segment(parts[0]);
            let name = parts[1..].join(".");
            let m = store.session.entry(tenant).or_default().entry(session).or_default();
            if ev.deleted {
                m.remove(&name);
            } else {
                m.insert(name, value);
            }
        }
    }
}

fn build_manifest(cfg: &Config) -> pb::ExtensionManifest {
    let mut m = pb::ExtensionManifest {
        id: cfg.id.clone(),
        version: cfg.version.clone(),
        features: EASYBUS_FEATURES.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    };
    let mut caps: Vec<String> = vec![];
    for (name, (spec, _)) in &cfg.tools {
        m.tools.push(pb::ExtensionTool {
            name: name.clone(),
            description: spec.description.clone(),
            descriptions: spec.descriptions.clone(),
            required_config: spec.required_config.clone(),
            ..Default::default()
        });
    }
    if !cfg.tools.is_empty() {
        caps.push("tools".to_string());
    }
    for (name, spec) in &cfg.variables {
        m.prompt_variables.push(pb::ExtensionVariable {
            name: name.clone(),
            description: Some(spec.description.clone()),
            scope: if spec.scope.is_empty() { "global".to_string() } else { spec.scope.clone() },
            ..Default::default()
        });
    }
    if !cfg.variables.is_empty() {
        caps.push("prompt".to_string());
    }
    for (name, spec) in &cfg.config {
        let mut item = pb::ExtensionConfigItem {
            name: name.clone(),
            r#type: if spec.r#type.is_empty() { "string".to_string() } else { spec.r#type.clone() },
            kind: Some(if spec.kind.is_empty() { "value".to_string() } else { spec.kind.clone() }),
            scope: if spec.scope.is_empty() { "global".to_string() } else { spec.scope.clone() },
            ..Default::default()
        };
        if !spec.capability.is_empty() {
            item.capability = Some(spec.capability.clone());
        }
        if !spec.description.is_empty() {
            item.description = Some(spec.description.clone());
        }
        item.enum_values = spec.enum_values.clone();
        if let Some(d) = &spec.default {
            item.default = Some(to_value(d));
        }
        m.config.push(item);
    }
    m.lifecycle = cfg.lifecycle.clone();
    if !cfg.call_hooks.is_empty() || !cfg.event_hooks.is_empty() {
        let mut hs = pb::HookSpec {
            call: cfg.call_hooks.clone(),
            event: cfg.event_hooks.clone(),
            ..Default::default()
        };
        hs.call = cfg.call_hooks.clone();
        hs.event = cfg.event_hooks.clone();
        m.hooks = Some(hs);
    }
    m.capabilities = caps;
    m
}

/// A simple closure-backed tool handler.
pub struct FnTool<F>(pub F);

#[async_trait]
impl<F, Fut> ToolHandler for FnTool<F>
where
    F: Fn(Value, String, String, String, Arc<tokio::sync::Notify>) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<ToolResultData, ToolError>> + Send,
{
    async fn execute(
        &self,
        args: Value,
        call_id: &str,
        session_name: &str,
        tenant: &str,
        cancel: Arc<tokio::sync::Notify>,
    ) -> Result<ToolResultData, ToolError> {
        (self.0)(args, call_id.to_string(), session_name.to_string(), tenant.to_string(), cancel).await
    }
}

/// Convenience: build a `Value` object from a `Map`.
pub fn obj(m: Map<String, Value>) -> Value {
    Value::Object(m)
}

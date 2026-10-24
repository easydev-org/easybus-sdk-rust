//! The agent role — the Rust twin of the Go `easybus-sdk/agent` package.
//! Discovers extensions (presence-first, broadcast fallback), calls tools,
//! resolves variables, fires hooks, interrupts, drives the mailbox, and owns
//! the config authority + session lease.

use crate::bus::{
    Bus, InboxConsumeOpts, InboxPublishOpts, PublishOpts, RequestOpts, SubscribeOpts,
};
use crate::gen::easybus::v1 as pb;
use crate::protocol::{
    ch_config, ch_hook_call, ch_hook_event, ch_interrupt, ch_lifecycle, ch_mailbox,
    ch_session_events, ch_tool_call, escape_kv_segment, new_id, session_token, session_var_key,
    subject_tenant, tenant_kv_key, tenant_object_name, var_key, CH_DISCOVER, CONFIG_KV_BUCKET,
    GLOBAL_TENANT, VARS_BUCKET,
};
use crate::wire::{code_from_wire, decode_manifest, from_value, lifecycle_kind_enum};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const PRESENCE_BUCKET: &str = "easybus-presence";
pub const LEASE_BUCKET: &str = "easybus-session-state";
pub const LEASE_TTL_DEFAULT_MS: u64 = 30_000;

#[derive(Debug, Clone)]
pub struct ConfigError {
    pub code: String,
    pub message: String,
}

impl ConfigError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        ConfigError {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

#[derive(Debug, Default, Clone)]
pub struct ToolResult {
    pub content: String,
    pub data: Option<Value>,
    pub metadata: Option<Value>,
    pub object: Option<(String, String)>,
    pub error: Option<(String, String)>, // (code name, message)
}

#[derive(Debug, Clone)]
pub struct MailboxMessage {
    pub id: String,
    pub r#type: String,
    pub source: String,
    pub payload: Value,
    pub session_name: String,
    pub tenant: String,
}

#[derive(Default, Clone)]
struct ConfigValue {
    revision: i64,
    value: Value,
}

/// The agent-side role.
pub struct Agent {
    bus: Arc<dyn Bus>,
    manifest_cache: Arc<Mutex<HashMap<String, pb::ExtensionManifest>>>,
    live_presence: Arc<Mutex<HashMap<String, std::collections::HashSet<String>>>>,
    presence_started: AtomicBool,
    presence_stop: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    // config authority
    declarations: Arc<Mutex<HashMap<String, HashMap<String, Vec<pb::ExtensionConfigItem>>>>>,
    global: Arc<Mutex<HashMap<String, HashMap<String, HashMap<String, ConfigValue>>>>>,
    sessions:
        Arc<Mutex<HashMap<String, HashMap<String, HashMap<String, HashMap<String, ConfigValue>>>>>>,
    config_started: AtomicBool,
    default_ack: AtomicBool,
}

/// One resolved dead-letter message.
#[derive(Debug, Clone)]
pub struct DlqMessage {
    pub id: String,
    pub tenant: String,
    pub session_name: String,
    pub typ: String,
    pub payload: Value,
}

/// Stops the DLQ consumer when dropped.
///
/// A handle rather than a bare tokio::JoinHandle: dropping a JoinHandle detaches the task, so the
/// loop would keep consuming after the caller thought it had stopped.
pub struct DlqHandle {
    stop: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for DlqHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

impl Agent {
    pub fn new(bus: Arc<dyn Bus>) -> Arc<Self> {
        Arc::new(Agent {
            bus,
            manifest_cache: Arc::new(Mutex::new(HashMap::new())),
            live_presence: Arc::new(Mutex::new(HashMap::new())),
            presence_started: AtomicBool::new(false),
            presence_stop: Mutex::new(None),
            declarations: Arc::new(Mutex::new(HashMap::new())),
            global: Arc::new(Mutex::new(HashMap::new())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            config_started: AtomicBool::new(false),
            default_ack: AtomicBool::new(false),
        })
    }

    pub fn manifest(&self, ext_id: &str) -> Option<pb::ExtensionManifest> {
        self.manifest_cache.lock().unwrap().get(ext_id).cloned()
    }

    pub async fn discover(self: &Arc<Self>, max_wait_ms: u64) -> Vec<pb::ExtensionManifest> {
        self.ensure_presence().await;
        {
            let cache = self.manifest_cache.lock().unwrap();
            if !cache.is_empty() {
                return cache.values().cloned().collect();
            }
        }
        let replies = self
            .bus
            .request_many(
                CH_DISCOVER,
                json!({}),
                RequestOpts {
                    max_wait_ms,
                    tenant: GLOBAL_TENANT.to_string(),
                    ..Default::default()
                },
            )
            .await;
        let mut seen = std::collections::HashSet::new();
        let mut out = vec![];
        for r in replies {
            let payload = from_value(
                r.payload
                    .as_ref()
                    .unwrap_or(&prost_types::Value { kind: None }),
            );
            let bytes = serde_json::to_vec(&payload).unwrap_or_default();
            if let Ok(m) = decode_manifest(&bytes) {
                if !m.id.is_empty() && seen.insert(m.id.clone()) {
                    self.manifest_cache
                        .lock()
                        .unwrap()
                        .insert(m.id.clone(), m.clone());
                    out.push(m);
                }
            }
        }
        out
    }

    async fn ensure_presence(self: &Arc<Self>) {
        if self.presence_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let (mut rx, stop) = self.bus.kv_watch(PRESENCE_BUCKET, ">").await;
        *self.presence_stop.lock().unwrap() = Some(stop);
        let me = self.clone();
        tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                if ev.done || ev.key.is_empty() {
                    continue;
                }
                let ext_id = ev.key.split('.').next().unwrap_or("").to_string();
                if ev.deleted {
                    me.forget_presence(&ext_id, &ev.key);
                    continue;
                }
                if let Ok(m) = decode_manifest(ev.value.as_bytes()) {
                    if !m.id.is_empty() {
                        me.manifest_cache.lock().unwrap().insert(m.id.clone(), m);
                    }
                }
                me.live_presence
                    .lock()
                    .unwrap()
                    .entry(ext_id)
                    .or_default()
                    .insert(ev.key);
            }
        });
    }

    fn forget_presence(&self, ext_id: &str, key: &str) {
        let mut lp = self.live_presence.lock().unwrap();
        if let Some(s) = lp.get_mut(ext_id) {
            s.remove(key);
            if s.is_empty() {
                lp.remove(ext_id);
                self.manifest_cache.lock().unwrap().remove(ext_id);
            }
        }
    }

    pub async fn call_tool(
        &self,
        tenant: &str,
        session_name: &str,
        ext_id: &str,
        tool: &str,
        call_id: &str,
        args: Value,
    ) -> Result<ToolResult, String> {
        let reply = self
            .bus
            .request(
                &ch_tool_call(tenant, ext_id, tool),
                json!({"call_id": call_id, "arguments": args}),
                RequestOpts {
                    session_name: session_name.to_string(),
                    tenant: tenant.to_string(),
                    timeout_ms: 30_000,
                    ..Default::default()
                },
            )
            .await
            .ok_or_else(|| format!("tool \"{tool}\": no reply"))?;
        let tr = from_value(
            reply
                .payload
                .as_ref()
                .unwrap_or(&prost_types::Value { kind: None }),
        );
        let mut r = ToolResult {
            content: tr
                .get("content")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            data: tr.get("data").cloned(),
            metadata: tr.get("metadata").cloned(),
            ..Default::default()
        };
        if let Some(e) = tr.get("error").and_then(|x| x.as_object()) {
            // protobuf Value carries numbers as float, so accept f64.
            let code = e.get("code").and_then(|x| x.as_f64()).unwrap_or(0.0) as i64;
            r.error = Some((
                code_from_wire(code),
                e.get("message")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string(),
            ));
        }
        if let Some(o) = tr.get("object").and_then(|x| x.as_object()) {
            r.object = Some((
                o.get("id")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string(),
                o.get("content_type")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string(),
            ));
        }
        Ok(r)
    }

    pub async fn resolve_variable(
        &self,
        tenant: &str,
        session_name: &str,
        provider: &str,
        name: &str,
    ) -> (String, bool) {
        let cached = self
            .bus
            .kv_get(
                VARS_BUCKET,
                &session_var_key(tenant, provider, session_name, name),
            )
            .await;
        if !cached.is_empty() {
            return (cached, true);
        }
        let cg = self
            .bus
            .kv_get(VARS_BUCKET, &var_key(tenant, provider, name))
            .await;
        if !cg.is_empty() {
            return (cg, true);
        }
        let reply = self
            .bus
            .request(
                &format!("easybus.{tenant}.var.{provider}.{name}"),
                json!({}),
                RequestOpts {
                    session_name: session_name.to_string(),
                    tenant: tenant.to_string(),
                    timeout_ms: 5000,
                    ..Default::default()
                },
            )
            .await;
        match reply {
            None => (String::new(), false),
            Some(r) => {
                let v = from_value(
                    r.payload
                        .as_ref()
                        .unwrap_or(&prost_types::Value { kind: None }),
                );
                (
                    v.get("value")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    true,
                )
            }
        }
    }

    pub async fn call_hook(
        &self,
        tenant: &str,
        ext_id: &str,
        hook: &str,
        session_name: &str,
        args: Value,
    ) -> Option<Value> {
        let reply = self
            .bus
            .request(
                &ch_hook_call(tenant, ext_id, hook),
                json!({"hook": hook, "session_name": session_name, "arguments": args}),
                RequestOpts {
                    session_name: session_name.to_string(),
                    tenant: tenant.to_string(),
                    timeout_ms: 5000,
                    ..Default::default()
                },
            )
            .await?;
        Some(from_value(
            reply
                .payload
                .as_ref()
                .unwrap_or(&prost_types::Value { kind: None }),
        ))
    }

    pub async fn interrupt(&self, tenant: &str, ext_id: &str) {
        self.bus
            .publish(
                &ch_interrupt(tenant, ext_id),
                json!({}),
                PublishOpts {
                    tenant: tenant.to_string(),
                    ..Default::default()
                },
            )
            .await;
    }

    pub async fn interrupt_session(
        &self,
        tenant: &str,
        ext_id: &str,
        session_name: &str,
        reason: &str,
    ) {
        let mut body = json!({"session_name": session_name});
        if !reason.is_empty() {
            body["reason"] = json!(reason);
        }
        self.bus
            .publish(
                &ch_interrupt(tenant, ext_id),
                body,
                PublishOpts {
                    tenant: tenant.to_string(),
                    ..Default::default()
                },
            )
            .await;
    }

    pub async fn publish_lifecycle_event(
        &self,
        tenant: &str,
        kind: &str,
        session_name: &str,
        payload: Option<Value>,
    ) {
        let mut body = json!({"kind": lifecycle_kind_enum(kind), "session_name": session_name});
        if let Some(p) = payload {
            body["payload"] = p;
        }
        self.bus
            .publish(
                &ch_lifecycle(tenant, kind),
                body,
                PublishOpts {
                    tenant: tenant.to_string(),
                    ..Default::default()
                },
            )
            .await;
    }

    pub async fn publish_event_hook(
        &self,
        tenant: &str,
        hook: &str,
        session_name: &str,
        payload: Value,
    ) {
        self.bus
            .publish(
                &ch_hook_event(tenant, hook),
                json!({"hook": hook, "session_name": session_name, "payload": payload}),
                PublishOpts {
                    tenant: tenant.to_string(),
                    ..Default::default()
                },
            )
            .await;
    }

    pub async fn publish_mailbox(
        &self,
        tenant: &str,
        session_name: &str,
        typ: &str,
        payload: Value,
    ) {
        let id = new_id();
        self.bus
            .inbox_publish(
                &ch_mailbox(tenant, session_name),
                json!({"id": id, "type": typ, "payload": payload}),
                InboxPublishOpts {
                    id: id.clone(),
                    session_name: session_name.to_string(),
                    tenant: tenant.to_string(),
                },
            )
            .await;
    }

    pub async fn publish_session_event(
        &self,
        tenant: &str,
        session_name: &str,
        event: &str,
        params: Value,
    ) {
        let id = new_id();
        self.bus
            .inbox_publish(
                &ch_session_events(tenant, session_name),
                json!({"event": event, "params": params, "eid": id}),
                InboxPublishOpts {
                    id: id.clone(),
                    session_name: session_name.to_string(),
                    tenant: tenant.to_string(),
                },
            )
            .await;
    }

    /// Subscribes to every dead-letter message and hands each to `handler`.
    ///
    /// Dead letters are messages a consumer gave up on after repeated failures. Without this they
    /// accumulate in the DLQ stream and nobody finds out — the failure is real but silent, which
    /// is the worst combination.
    ///
    /// `handler` returns `true` to ack the message, `false` to leave it in the DLQ (nacked,
    /// retried later). A message that cannot be resolved to a tenant or session is TERMINATED
    /// rather than dead-lettered again: dead letters of dead letters help nobody.
    ///
    /// Returns a stop handle; dropping it ends the loop.
    pub async fn consume_dlq<F>(&self, mut handler: F) -> Result<DlqHandle, String>
    where
        F: FnMut(DlqMessage) -> bool + Send + 'static,
    {
        let sub = self
            .bus
            .inbox_consume(InboxConsumeOpts {
                subject: "easybus.*.dlq.>".to_string(),
            })
            .await;
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let task = tokio::spawn(async move {
            loop {
                if flag.load(Ordering::Relaxed) {
                    return;
                }
                let Some(msg) = sub.next().await else { return };
                let env = msg.envelope().clone();
                let session_name = env.session_name.clone().unwrap_or_default();
                let tenant = if env.tenant.is_empty() {
                    subject_tenant(&env.ch)
                } else {
                    env.tenant.clone()
                };
                let payload = from_value(
                    env.payload
                        .as_ref()
                        .unwrap_or(&prost_types::Value { kind: None }),
                );
                let id = payload.get("id").and_then(|v| v.as_str()).unwrap_or("");
                if session_name.is_empty() || tenant.is_empty() || env.id.is_none() || id.is_empty() {
                    // A dead letter with no address cannot be retried meaningfully.
                    msg.term_no_dlq().await;
                    continue;
                }
                let resolved = DlqMessage {
                    id: id.to_string(),
                    tenant,
                    session_name,
                    typ: payload
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    payload: payload.get("payload").cloned().unwrap_or(Value::Null),
                };
                if handler(resolved) {
                    msg.ack().await;
                } else {
                    msg.nak(5000).await;
                }
            }
        });
        Ok(DlqHandle {
            stop,
            task: Some(task),
        })
    }

    pub async fn put_object(&self, tenant: &str, name: &str, data: Vec<u8>) {
        self.bus
            .object_put(&tenant_object_name(tenant, name), data)
            .await;
    }
    pub async fn get_object(&self, tenant: &str, name: &str) -> Option<Vec<u8>> {
        self.bus.object_get(&tenant_object_name(tenant, name)).await
    }

    // ---- config authority --------------------------------------------------

    pub fn serve_config(&self, default_ack: bool) {
        self.config_started.store(true, Ordering::SeqCst);
        self.default_ack.store(default_ack, Ordering::SeqCst);
    }

    pub async fn set_config(
        &self,
        tenant: &str,
        ext_id: &str,
        name: &str,
        value: Value,
        session_name: &str,
        manifest: Option<pb::ExtensionManifest>,
        ack: Option<bool>,
    ) -> Result<(), ConfigError> {
        if !self.config_started.load(Ordering::SeqCst) {
            return Err(ConfigError::new(
                "invalid_argument",
                "ServeConfig() was not called",
            ));
        }
        let m = match manifest.or_else(|| self.manifest(ext_id)) {
            Some(m) => m,
            None => {
                return Err(ConfigError::new(
                    "not_found",
                    format!("no manifest for {ext_id}; Discover() first"),
                ))
            }
        };
        self.declare(tenant, &m).await;
        self.set_inner(tenant, ext_id, name, value, session_name, ack)
            .await
    }

    async fn declare(&self, tenant: &str, m: &pb::ExtensionManifest) {
        self.declarations
            .lock()
            .unwrap()
            .entry(tenant.to_string())
            .or_default()
            .insert(m.id.clone(), m.config.clone());
        for item in &m.config {
            let key = config_kv_key(tenant, &m.id, "global", "", &item.name);
            let raw = self.bus.kv_get(CONFIG_KV_BUCKET, &key).await;
            if raw.is_empty() {
                continue;
            }
            let Ok(parsed) = serde_json::from_str::<Value>(&raw) else {
                continue;
            };
            let rev = parsed.get("r").and_then(|x| x.as_i64()).unwrap_or(0);
            let val = parsed.get("v").cloned().unwrap_or(parsed.clone());
            let mut g = self.global.lock().unwrap();
            let by = g
                .entry(tenant.to_string())
                .or_default()
                .entry(m.id.clone())
                .or_default();
            let cur = by.get(&item.name);
            if cur.is_none() || rev > cur.unwrap().revision {
                by.insert(
                    item.name.clone(),
                    ConfigValue {
                        revision: rev,
                        value: val,
                    },
                );
            }
        }
    }

    async fn set_inner(
        &self,
        tenant: &str,
        ext_id: &str,
        name: &str,
        value: Value,
        session_name: &str,
        ack: Option<bool>,
    ) -> Result<(), ConfigError> {
        let items = self
            .declarations
            .lock()
            .unwrap()
            .get(tenant)
            .and_then(|m| m.get(ext_id))
            .cloned()
            .unwrap_or_default();
        let item = items.iter().find(|it| it.name == name).cloned();
        let Some(item) = item else {
            return Err(ConfigError::new(
                "not_found",
                format!("config {name} not declared by {ext_id}"),
            ));
        };
        let scope = if item.scope.is_empty() {
            "global".to_string()
        } else {
            item.scope.clone()
        };
        if scope == "session" && session_name.is_empty() {
            return Err(ConfigError::new(
                "invalid_argument",
                format!("config {name} requires a session name (scope=session)"),
            ));
        }
        if let Some(msg) = validate_config_value(&item, &value) {
            return Err(ConfigError::new("invalid_argument", msg));
        }
        let use_ack = ack.unwrap_or_else(|| self.default_ack.load(Ordering::SeqCst));
        let revision;
        if scope == "global" {
            let mut g = self.global.lock().unwrap();
            let by = g
                .entry(tenant.to_string())
                .or_default()
                .entry(ext_id.to_string())
                .or_default();
            revision = by.get(name).map(|c| c.revision).unwrap_or(0) + 1;
            by.insert(
                name.to_string(),
                ConfigValue {
                    revision,
                    value: value.clone(),
                },
            );
        } else {
            let mut s = self.sessions.lock().unwrap();
            let by = s
                .entry(tenant.to_string())
                .or_default()
                .entry(ext_id.to_string())
                .or_default()
                .entry(session_name.to_string())
                .or_default();
            revision = by.get(name).map(|c| c.revision).unwrap_or(0) + 1;
            by.insert(
                name.to_string(),
                ConfigValue {
                    revision,
                    value: value.clone(),
                },
            );
        }
        let key = config_kv_key(tenant, ext_id, &scope, session_name, name);
        self.bus
            .kv_put(
                CONFIG_KV_BUCKET,
                &key,
                &json!({"r": revision, "v": value}).to_string(),
                0,
            )
            .await;
        let mut payload = json!({"name": name, "value": value, "revision": revision, "scope": scope, "ack": use_ack, "tenant": tenant});
        if !session_name.is_empty() {
            payload["session_name"] = json!(session_name);
        }
        let reply = self
            .bus
            .request(
                &ch_config(tenant, ext_id),
                payload,
                RequestOpts {
                    timeout_ms: if use_ack { 5000 } else { 300 },
                    tenant: tenant.to_string(),
                    session_name: session_name.to_string(),
                    ..Default::default()
                },
            )
            .await;
        if !use_ack || reply.is_none() {
            return Ok(());
        }
        let hr = from_value(
            reply
                .unwrap()
                .payload
                .as_ref()
                .unwrap_or(&prost_types::Value { kind: None }),
        );
        if hr.get("ok").and_then(|x| x.as_bool()) != Some(true) {
            if scope == "global" {
                if let Some(by) = self
                    .global
                    .lock()
                    .unwrap()
                    .get_mut(tenant)
                    .and_then(|m| m.get_mut(ext_id))
                {
                    if by.get(name).map(|c| c.revision) == Some(revision) {
                        by.remove(name);
                    }
                }
            } else if let Some(by) = self
                .sessions
                .lock()
                .unwrap()
                .get_mut(tenant)
                .and_then(|m| m.get_mut(ext_id))
                .and_then(|m| m.get_mut(session_name))
            {
                if by.get(name).map(|c| c.revision) == Some(revision) {
                    by.remove(name);
                }
            }
            self.bus.kv_delete(CONFIG_KV_BUCKET, &key).await;
            let msg = hr
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(|x| x.as_str())
                .unwrap_or("extension rejected the config change");
            return Err(ConfigError::new("retryable", msg));
        }
        Ok(())
    }

    pub async fn drop_session_config(&self, tenant: &str, ext_id: &str, session_name: &str) {
        let by = self
            .sessions
            .lock()
            .unwrap()
            .get(tenant)
            .and_then(|m| m.get(ext_id))
            .and_then(|m| m.get(session_name))
            .cloned()
            .unwrap_or_default();
        for name in by.keys() {
            self.bus
                .kv_delete(
                    CONFIG_KV_BUCKET,
                    &config_kv_key(tenant, ext_id, "session", session_name, name),
                )
                .await;
        }
        if let Some(m) = self
            .sessions
            .lock()
            .unwrap()
            .get_mut(tenant)
            .and_then(|x| x.get_mut(ext_id))
        {
            m.remove(session_name);
        }
    }

    // ---- session lease -----------------------------------------------------

    pub async fn claim_session(
        &self,
        tenant: &str,
        session_name: &str,
        ttl_ms: u64,
    ) -> Option<i64> {
        let ttl = if ttl_ms > 0 {
            ttl_ms
        } else {
            LEASE_TTL_DEFAULT_MS
        };
        let rev = self
            .bus
            .kv_create(
                LEASE_BUCKET,
                &tenant_kv_key(tenant, &session_token(session_name)),
                "running",
                ttl,
            )
            .await;
        if rev == 0 {
            None
        } else {
            Some(rev)
        }
    }

    pub async fn renew_session(
        &self,
        tenant: &str,
        session_name: &str,
        revision: i64,
        _ttl_ms: u64,
    ) -> Option<i64> {
        let rev = self
            .bus
            .kv_cas(
                LEASE_BUCKET,
                &tenant_kv_key(tenant, &session_token(session_name)),
                "running",
                revision,
            )
            .await;
        if rev == 0 {
            None
        } else {
            Some(rev)
        }
    }

    pub async fn release_session(&self, tenant: &str, session_name: &str) {
        self.bus
            .kv_delete(
                LEASE_BUCKET,
                &tenant_kv_key(tenant, &session_token(session_name)),
            )
            .await;
    }

    pub async fn is_session_running(&self, tenant: &str, session_name: &str) -> bool {
        !self
            .bus
            .kv_get(
                LEASE_BUCKET,
                &tenant_kv_key(tenant, &session_token(session_name)),
            )
            .await
            .is_empty()
    }

    pub async fn consume_mailbox(
        self: &Arc<Self>,
        handler: Arc<dyn Fn(MailboxMessage) -> BoxFutureMail + Send + Sync>,
    ) -> Box<dyn Fn() + Send + Sync> {
        let sub = self
            .bus
            .inbox_consume(InboxConsumeOpts {
                subject: "easybus.*.mailbox.>".to_string(),
            })
            .await;
        let stopped = Arc::new(AtomicBool::new(false));
        let stopped2 = stopped.clone();
        let bus = self.bus.clone();
        tokio::spawn(async move {
            while !stopped2.load(Ordering::Relaxed) {
                let Some(m) = sub.next().await else { continue };
                let env = m.envelope();
                let tenant = if env.tenant.is_empty() {
                    subject_tenant(&env.ch)
                } else {
                    env.tenant.clone()
                };
                let body = from_value(
                    env.payload
                        .as_ref()
                        .unwrap_or(&prost_types::Value { kind: None }),
                );
                let msg = MailboxMessage {
                    id: body
                        .get("id")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    r#type: body
                        .get("type")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    source: body
                        .get("source")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    payload: body.get("payload").cloned().unwrap_or(Value::Null),
                    session_name: env.session_name.clone().unwrap_or_default(),
                    tenant,
                };
                handler(msg).await;
                m.ack().await;
            }
        });
        let _ = bus;
        Box::new(move || stopped.store(true, Ordering::Relaxed))
    }

    pub async fn close(&self) {
        if let Some(stop) = self.presence_stop.lock().unwrap().as_ref() {
            stop();
        }
        self.bus.close().await;
    }
}

pub type BoxFutureMail = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

fn config_kv_key(
    tenant: &str,
    ext_id: &str,
    scope: &str,
    session_name: &str,
    name: &str,
) -> String {
    if scope == "session" {
        return tenant_kv_key(
            tenant,
            &format!("{ext_id}.{}.{name}", escape_kv_segment(session_name)),
        );
    }
    tenant_kv_key(tenant, &format!("{ext_id}.{name}"))
}

fn validate_config_value(item: &pb::ExtensionConfigItem, value: &Value) -> Option<String> {
    let t = item.r#type.as_str();
    match t {
        "string" if !value.is_string() => {
            Some(format!("expected string, got {}", type_name(value)))
        }
        "number" if !value.is_number() => {
            Some(format!("expected number, got {}", type_name(value)))
        }
        "boolean" if !value.is_boolean() => {
            Some(format!("expected boolean, got {}", type_name(value)))
        }
        "enum" => {
            let Some(s) = value.as_str() else {
                return Some(format!("expected enum string, got {}", type_name(value)));
            };
            if !item.enum_values.is_empty() && !item.enum_values.iter().any(|e| e == s) {
                return Some(format!("value \"{s}\" not in the declared enum values"));
            }
            None
        }
        _ => None,
    }
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Subscribe to a progress channel (agent side).
pub async fn subscribe_progress(
    bus: &Arc<dyn Bus>,
    tenant: &str,
    call_id: &str,
) -> Box<dyn crate::bus::Subscription> {
    bus.subscribe(
        &format!("easybus.{tenant}.tool.progress.{call_id}"),
        SubscribeOpts::default(),
    )
    .await
}

/// Keep the Duration import used.
pub fn _dur() -> Duration {
    Duration::from_secs(1)
}

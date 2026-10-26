//! The NATS/JetStream adapter.

use crate::bus::{
    Bus, InboxConsumeOpts, InboxMsg, InboxPublishOpts, InboxSubscription, KvEvent, PublishOpts,
    RequestOpts, SubscribeOpts, Subscription,
};
use crate::gen::easybus::v1 as pb;
use crate::identity::{auth_header, verify, Fields as IdFields, Identity};
use crate::protocol::{session_token, subject_tenant, tenant_prefix};
use crate::wire::{decode_envelope, encode_envelope, from_value, to_value};
use async_nats::jetstream::consumer::{pull, Consumer};
use async_nats::jetstream::kv;
use async_nats::jetstream::object_store;
use async_nats::jetstream::stream::{Config as StreamConfig, StorageType};
use async_nats::jetstream::Context;
use async_nats::{Client, HeaderMap};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use prost_types::Value as PbValue;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub const STREAM_MAILBOX: &str = "EASYBUS_MAILBOX";
pub const STREAM_EVENTS: &str = "EASYBUS_EVENTS";
pub const STREAM_DLQ: &str = "EASYBUS_DLQ";
pub const INBOX_WILDCARD: &str = "easybus.*.mailbox.>";
pub const EVENTS_WILDCARD: &str = "easybus.*.session.events.>";
pub const LIFECYCLE_WILDCARD: &str = "easybus.*.session.lifecycle.>";
pub const CHANGED_WILDCARD: &str = "easybus.*.session.changed";
pub const DLQ_WILDCARD: &str = "easybus.*.dlq.>";
pub const DURABLE_CONSUMER: &str = "easybus-mailbox-push";
pub const OBJECT_BUCKET: &str = "EASYBUS_TOOL";
pub const OBJECT_BUCKET_DUR: &str = "EASYBUS_FILES";

#[derive(Clone, Default)]
pub struct Options {
    pub max_age_ms: u64,
    pub identity: Option<Identity>,
}

fn tenant_of(ch: &str, explicit: &str) -> String {
    if !explicit.is_empty() {
        explicit.to_string()
    } else {
        subject_tenant(ch)
    }
}

fn kind_enum(kind: &str) -> i32 {
    crate::wire::kind_from_string(kind)
}

fn build_envelope(
    kind: &str,
    ch: &str,
    payload: &Value,
    tenant: &str,
    id: &str,
    session_name: &str,
    reply_to: &str,
) -> Vec<u8> {
    let mut env = pb::Envelope {
        v: Some(2),
        ch: ch.to_string(),
        tenant: tenant.to_string(),
        kind: kind_enum(kind),
        ..Default::default()
    };
    if !id.is_empty() {
        env.id = Some(id.to_string());
    }
    if !session_name.is_empty() {
        env.session_name = Some(session_name.to_string());
    }
    if !reply_to.is_empty() {
        env.reply_to = Some(reply_to.to_string());
    }
    if !payload.is_null() {
        env.payload = Some(to_value(payload));
    }
    encode_envelope(&env)
}

fn dlq_subject_for(subject: &str) -> String {
    let token = subject.rsplit('.').next().unwrap_or("");
    let tenant = subject_tenant(subject);
    if tenant.is_empty() {
        format!("easybus.dlq.{token}")
    } else {
        format!("{}dlq.{token}", tenant_prefix(&tenant))
    }
}

/// NatsBus is the NATS/JetStream transport.
pub struct NatsBus {
    nc: Client,
    js: Context,
    idn: Option<Identity>,
}

impl NatsBus {
    /// Connect a NATS bus with the default stream topology.
    pub async fn connect(
        url: &str,
        opts: Options,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let nc = async_nats::connect(url).await?;
        let js = async_nats::jetstream::new(nc.clone());
        Self::ensure_streams(&js, &opts).await?;
        Ok(Arc::new(NatsBus {
            nc,
            js,
            idn: opts.identity,
        }))
    }

    async fn ensure_streams(
        js: &Context,
        opts: &Options,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let max_age = if opts.max_age_ms > 0 {
            Duration::from_millis(opts.max_age_ms)
        } else {
            Duration::from_secs(24 * 3600)
        };
        let specs: [(&str, Vec<&str>); 3] = [
            (STREAM_MAILBOX, vec![INBOX_WILDCARD]),
            (
                STREAM_EVENTS,
                vec![EVENTS_WILDCARD, LIFECYCLE_WILDCARD, CHANGED_WILDCARD],
            ),
            (STREAM_DLQ, vec![DLQ_WILDCARD]),
        ];
        for (name, subjects) in specs {
            let cfg = StreamConfig {
                name: name.to_string(),
                subjects: subjects.iter().map(|s| s.to_string()).collect(),
                max_age,
                storage: StorageType::File,
                ..Default::default()
            };
            let _ = js.get_or_create_stream(cfg).await?;
        }
        Ok(())
    }

    fn auth_headers(
        &self,
        ch: &str,
        kind: &str,
        id: &str,
        payload: Option<&Value>,
    ) -> Option<HeaderMap> {
        let idn = self.idn.as_ref()?;
        let h = auth_header(
            idn,
            &IdFields {
                ch: ch.to_string(),
                kind: kind.to_string(),
                id: id.to_string(),
                payload: payload.cloned(),
                ..Default::default()
            },
        );
        let mut hm = HeaderMap::new();
        hm.insert("easybus-id", h.id.as_str());
        hm.insert("easybus-sig", h.sig.as_str());
        Some(hm)
    }

    fn verify_msg(
        &self,
        headers: &HeaderMap,
        tenant: &str,
        ch: &str,
        kind: &str,
        id: &str,
        payload: Option<&Value>,
    ) -> bool {
        if self.idn.is_none() {
            return true;
        }
        let sig = headers.get("easybus-sig").map(|v| v.as_str()).unwrap_or("");
        if sig.is_empty() {
            return false;
        }
        let claimed = headers.get("easybus-id").map(|v| v.as_str()).unwrap_or("");
        verify(
            claimed,
            &self.idn.as_ref().unwrap().secret,
            &IdFields {
                ch: ch.to_string(),
                kind: kind.to_string(),
                tenant: tenant.to_string(),
                id: id.to_string(),
                payload: payload.cloned(),
            },
            sig,
        )
    }

    fn decode_msg(
        &self,
        subject: &str,
        headers: &HeaderMap,
        data: &[u8],
        reply: Option<&str>,
    ) -> Option<pb::Envelope> {
        let mut env = decode_envelope(data).ok()?;
        let payload = env.payload.as_ref().map(from_value);
        if !self.verify_msg(
            headers,
            &env.tenant,
            subject,
            crate::wire::kind_to_string(env.kind),
            env.id.as_deref().unwrap_or(""),
            payload.as_ref(),
        ) {
            return None;
        }
        if env.reply_to.is_none() {
            if let Some(r) = reply {
                env.reply_to = Some(r.to_string());
            }
        }
        Some(env)
    }
}

#[async_trait]
impl Bus for NatsBus {
    async fn request(&self, ch: &str, payload: Value, opts: RequestOpts) -> Option<pb::Envelope> {
        let data = build_envelope(
            "req",
            ch,
            &payload,
            &tenant_of(ch, &opts.tenant),
            "",
            &opts.session_name,
            "",
        );
        let timeout = if opts.timeout_ms > 0 {
            Duration::from_millis(opts.timeout_ms)
        } else {
            Duration::from_secs(5)
        };
        let headers = self.auth_headers(ch, "req", "", Some(&payload));
        let fut = self.nc.request_with_headers(
            ch.to_string(),
            headers.unwrap_or_default(),
            Bytes::from(data),
        );
        let msg = tokio::time::timeout(timeout, fut).await.ok()?.ok()?;
        let reply = msg.reply.as_ref().map(|r| r.as_str());
        self.decode_msg(
            ch,
            msg.headers.as_ref().unwrap_or(&HeaderMap::new()),
            &msg.payload,
            reply,
        )
    }

    async fn request_many(&self, ch: &str, payload: Value, opts: RequestOpts) -> Vec<pb::Envelope> {
        let data = build_envelope(
            "req",
            ch,
            &payload,
            &tenant_of(ch, &opts.tenant),
            "",
            "",
            "",
        );
        let max_wait = if opts.max_wait_ms > 0 {
            Duration::from_millis(opts.max_wait_ms)
        } else {
            Duration::from_millis(500)
        };
        let inbox = self.nc.new_inbox();
        let mut sub = match self.nc.subscribe(inbox.clone()).await {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        let _ = self.nc.flush().await;
        let headers = self.auth_headers(ch, "req", "", Some(&payload));
        let _ = self
            .nc
            .publish_with_reply_and_headers(
                ch.to_string(),
                inbox.clone(),
                headers.unwrap_or_default(),
                Bytes::from(data),
            )
            .await;
        let _ = self.nc.flush().await;
        let mut out = vec![];
        let deadline = tokio::time::Instant::now() + max_wait;
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }
            match tokio::time::timeout(deadline - now, sub.next()).await {
                Ok(Some(msg)) => {
                    let reply = msg.reply.as_ref().map(|r| r.as_str());
                    if let Some(env) = self.decode_msg(
                        &msg.subject,
                        msg.headers.as_ref().unwrap_or(&HeaderMap::new()),
                        &msg.payload,
                        reply,
                    ) {
                        out.push(env);
                    }
                }
                _ => break,
            }
        }
        out
    }

    async fn publish(&self, ch: &str, payload: Value, opts: PublishOpts) {
        let data = build_envelope(
            "pub",
            ch,
            &payload,
            &tenant_of(ch, &opts.tenant),
            "",
            "",
            &opts.reply_to,
        );
        let headers = self.auth_headers(ch, "pub", "", Some(&payload));
        if opts.reply_to.is_empty() {
            let _ = self
                .nc
                .publish_with_headers(
                    ch.to_string(),
                    headers.unwrap_or_default(),
                    Bytes::from(data),
                )
                .await;
        } else {
            let _ = self
                .nc
                .publish_with_reply_and_headers(
                    ch.to_string(),
                    opts.reply_to.clone(),
                    headers.unwrap_or_default(),
                    Bytes::from(data),
                )
                .await;
        }
        let _ = self.nc.flush().await;
    }

    async fn subscribe(&self, ch: &str, opts: SubscribeOpts) -> Box<dyn Subscription> {
        let sub = if opts.queue.is_empty() {
            self.nc.subscribe(ch.to_string()).await
        } else {
            self.nc
                .queue_subscribe(ch.to_string(), opts.queue.clone())
                .await
        };
        let sub = sub.expect("subscribe");
        let _ = self.nc.flush().await;
        Box::new(NatsSub {
            sub: tokio::sync::Mutex::new(sub),
        })
    }

    async fn inbox_publish(&self, ch: &str, payload: Value, opts: InboxPublishOpts) {
        let data = build_envelope(
            "queue",
            ch,
            &payload,
            &tenant_of(ch, &opts.tenant),
            &opts.id,
            &opts.session_name,
            "",
        );
        let mut hm = HeaderMap::new();
        hm.insert("Nats-Msg-Id", opts.id.as_str());
        let _ = self
            .js
            .publish_with_headers(ch.to_string(), hm, Bytes::from(data))
            .await;
    }

    async fn inbox_consume(&self, opts: InboxConsumeOpts) -> Box<dyn InboxSubscription> {
        let subject = if opts.subject.is_empty() {
            INBOX_WILDCARD.to_string()
        } else {
            opts.subject.clone()
        };
        let durable = if subject == INBOX_WILDCARD {
            DURABLE_CONSUMER.to_string()
        } else {
            format!("{DURABLE_CONSUMER}-{}", session_token(&subject))
        };
        // PullSubscribe picks the stream that covers the subject (mailbox OR
        // events) — a fixed stream would silently deliver nothing when the
        // filter subject lives in the other stream (e.g. session events).
        let consumer: Consumer<pull::Config> = self
            .js
            .create_consumer_on_stream(
                pull::Config {
                    durable_name: Some(durable.clone()),
                    filter_subject: subject.clone(),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream_for_subject(&subject),
            )
            .await
            .expect("consumer");
        Box::new(NatsInboxSub {
            consumer,
            js: self.js.clone(),
            nc: self.nc.clone(),
        })
    }

    async fn replay(&self, ch: &str) -> Vec<pb::Envelope> {
        let mut out = vec![];
        for name in [STREAM_EVENTS, STREAM_MAILBOX, STREAM_DLQ] {
            let Ok(mut stream) = self.js.get_stream(name).await else {
                continue;
            };
            let info = stream.info().await;
            let subjects: Vec<String> = info
                .as_ref()
                .map(|i| i.config.subjects.clone())
                .unwrap_or_default();
            if !subjects
                .iter()
                .any(|s| s == ch || (s.contains('*') || s.contains('>')) && subject_match(s, ch))
            {
                continue;
            }
            let cfg = pull::Config {
                deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                filter_subject: ch.to_string(),
                ..Default::default()
            };
            if let Ok(consumer) = stream.create_consumer(cfg).await {
                if let Ok(mut batch) = consumer
                    .fetch()
                    .max_messages(1000)
                    .expires(Duration::from_secs(1))
                    .messages()
                    .await
                {
                    while let Some(Ok(m)) = batch.next().await {
                        let reply = m.reply.as_ref().map(|r| r.as_str());
                        if let Some(env) = self.decode_msg(
                            &m.subject,
                            m.headers.as_ref().unwrap_or(&HeaderMap::new()),
                            &m.payload,
                            reply,
                        ) {
                            out.push(env);
                        }
                    }
                }
            }
            break;
        }
        out
    }

    async fn object_put(&self, name: &str, data: Vec<u8>) {
        let os = match self.js.get_object_store(OBJECT_BUCKET).await {
            Ok(os) => os,
            Err(_) => match self
                .js
                .create_object_store(object_store::Config {
                    bucket: OBJECT_BUCKET.to_string(),
                    ..Default::default()
                })
                .await
            {
                Ok(os) => os,
                Err(_) => return,
            },
        };
        let mut reader = std::io::Cursor::new(data);
        let _ = os
            .put(
                object_store::ObjectMetadata {
                    name: name.to_string(),
                    ..Default::default()
                },
                &mut reader,
            )
            .await;
    }

    async fn object_get(&self, name: &str) -> Option<Vec<u8>> {
        let os = self.js.get_object_store(OBJECT_BUCKET).await.ok()?;
        let obj = os.get(name).await.ok()?;
        let mut buf = vec![];
        let mut r = obj;
        use tokio::io::AsyncReadExt;
        r.read_to_end(&mut buf).await.ok()?;
        Some(buf)
    }

    async fn object_put_persistent(&self, name: &str, data: Vec<u8>) {
        let os = match self.js.get_object_store(OBJECT_BUCKET_DUR).await {
            Ok(os) => os,
            Err(_) => match self
                .js
                .create_object_store(object_store::Config {
                    bucket: OBJECT_BUCKET_DUR.to_string(),
                    ..Default::default()
                })
                .await
            {
                Ok(os) => os,
                Err(_) => return,
            },
        };
        let mut reader = std::io::Cursor::new(data);
        let _ = os
            .put(
                object_store::ObjectMetadata {
                    name: name.to_string(),
                    ..Default::default()
                },
                &mut reader,
            )
            .await;
    }

    async fn object_get_persistent(&self, name: &str) -> Option<Vec<u8>> {
        let os = self.js.get_object_store(OBJECT_BUCKET_DUR).await.ok()?;
        let obj = os.get(name).await.ok()?;
        let mut buf = vec![];
        let mut r = obj;
        use tokio::io::AsyncReadExt;
        r.read_to_end(&mut buf).await.ok()?;
        Some(buf)
    }

    async fn kv_watch(
        &self,
        bucket: &str,
        keys: &str,
    ) -> (mpsc::Receiver<KvEvent>, Box<dyn Fn() + Send + Sync>) {
        let store = self.kv_store(bucket, 0).await;
        let (tx, rx) = mpsc::channel(64);
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        if let Some(store) = store {
            // watch_with_history replays the current value per subject (the initial
            // snapshot), matching the Go SDK; plain `watch` is DeliverPolicy::New
            // and would miss values written before the watch started.
            let mut watch = store
                .watch_with_history(keys.to_string())
                .await
                .expect("kv watch");
            let stop2 = stop.clone();
            tokio::spawn(async move {
                while let Some(item) = watch.next().await {
                    if stop2.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                    let Ok(entry) = item else { continue };
                    let deleted = entry.operation == kv::Operation::Delete
                        || entry.operation == kv::Operation::Purge;
                    let ev = KvEvent {
                        key: entry.key.clone(),
                        value: String::from_utf8_lossy(&entry.value).to_string(),
                        revision: entry.revision as i64,
                        deleted,
                        is_update: true,
                        done: false,
                    };
                    if tx.send(ev).await.is_err() {
                        break;
                    }
                }
            });
        }
        let stop_fn = Box::new(move || stop.store(true, std::sync::atomic::Ordering::Relaxed));
        (rx, stop_fn)
    }

    async fn kv_create(&self, bucket: &str, key: &str, value: &str, ttl_ms: u64) -> i64 {
        let Some(store) = self.kv_store(bucket, ttl_ms).await else {
            // The bucket could not be opened at all. That is a real failure, not "somebody else
            // holds the key", and the caller reads 0 as the latter — so the distinction has to go
            // somewhere. This trait returns a bare revision, so the failure is logged rather than
            // swallowed silently: an operator can see it, and the boot backstop re-runs the work.
            tracing::warn!(bucket, key, "kv_create: bucket unavailable; reporting no claim");
            return 0;
        };
        match store
            .create(key.to_string(), Bytes::from(value.to_string()))
            .await
        {
            Ok(rev) => rev as i64,
            Err(e) => {
                let msg = e.to_string();
                // "already exists" is the normal LOSING side of a claim race: 0, no complaint.
                // Anything else is a real failure and must not look like a lost race.
                if !crate::protocol::is_already_exists(Some(&msg)) {
                    tracing::warn!(bucket, key, error = %msg,
                        "kv_create failed for a reason other than key-exists; reporting no claim");
                }
                0
            }
        }
    }

    async fn kv_put(&self, bucket: &str, key: &str, value: &str, ttl_ms: u64) {
        if let Some(store) = self.kv_store(bucket, ttl_ms).await {
            let _ = store
                .put(key.to_string(), Bytes::from(value.to_string()))
                .await;
        }
    }

    async fn kv_get(&self, bucket: &str, key: &str) -> String {
        let Some(store) = self.kv_store(bucket, 0).await else {
            return String::new();
        };
        match store.get(key.to_string()).await {
            Ok(Some(b)) => String::from_utf8_lossy(&b).to_string(),
            Ok(None) => String::new(), // absent: a MISS, the caller's default branch
            Err(e) => {
                let msg = e.to_string();
                // An expired key ("no message found") is a miss. Anything else is a real failure,
                // and returning "" for it makes a caller take its default branch and never retry.
                if !crate::protocol::is_absent_key(Some(&msg)) {
                    tracing::warn!(bucket, key, error = %msg,
                        "kv_get failed for a reason other than a miss; reporting absent");
                }
                String::new()
            }
        }
    }

    async fn kv_cas(&self, bucket: &str, key: &str, value: &str, revision: i64) -> i64 {
        let Some(store) = self.kv_store(bucket, 0).await else {
            return 0;
        };
        match store
            .update(
                key.to_string(),
                Bytes::from(value.to_string()),
                revision as u64,
            )
            .await
        {
            Ok(rev) => rev as i64,
            Err(e) => {
                let msg = e.to_string();
                // A lost CAS is a normal outcome: 0 with no complaint. Anything else is a real
                // failure, and collapsing the two is what let a network blip abort a healthy
                // in-flight turn — the caller reads 0 as "the lease was LOST".
                if !crate::protocol::is_lost_race(Some(&msg)) {
                    tracing::warn!(bucket, key, error = %msg,
                        "kv_cas failed for a reason other than a lost race; reporting no renewal");
                }
                0
            }
        }
    }

    async fn kv_delete(&self, bucket: &str, key: &str) {
        if let Some(store) = self.kv_store(bucket, 0).await {
            let _ = store.delete(key.to_string()).await;
        }
    }

    async fn close(&self) {
        let _ = self.nc.flush().await;
    }
}

impl NatsBus {
    async fn kv_store(&self, bucket: &str, ttl_ms: u64) -> Option<kv::Store> {
        if let Ok(s) = self.js.get_key_value(bucket.to_string()).await {
            return Some(s);
        }
        let cfg = kv::Config {
            bucket: bucket.to_string(),
            max_age: if ttl_ms > 0 {
                Duration::from_millis(ttl_ms)
            } else {
                Duration::from_secs(0)
            },
            ..Default::default()
        };
        self.js.create_key_value(cfg).await.ok()
    }
}

/// The stream that owns a subject (mailbox vs events vs dlq).
fn stream_for_subject(subject: &str) -> &'static str {
    let seg: Vec<&str> = subject.split('.').collect();
    if seg.len() > 3 && seg[2] == "session" {
        return STREAM_EVENTS;
    }
    if seg.len() > 2 && seg[2] == "dlq" {
        return STREAM_DLQ;
    }
    STREAM_MAILBOX
}

fn subject_match(pattern: &str, subject: &str) -> bool {
    let p: Vec<&str> = pattern.split('.').collect();
    let s: Vec<&str> = subject.split('.').collect();
    let mut pi = 0;
    for si in 0..s.len() {
        if pi >= p.len() {
            return false;
        }
        match p[pi] {
            ">" => return true,
            "*" => pi += 1,
            t => {
                if t != s[si] {
                    return false;
                }
                pi += 1;
            }
        }
    }
    pi == p.len()
}

struct NatsSub {
    sub: tokio::sync::Mutex<async_nats::Subscriber>,
}

#[async_trait]
impl Subscription for NatsSub {
    async fn next(&self) -> Option<pb::Envelope> {
        let mut sub = self.sub.lock().await;
        let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
            .await
            .ok()??;
        let reply = msg.reply.as_ref().map(|r| r.as_str());
        // decode without identity (bus-level verify happens in the transport).
        let mut env = decode_envelope(&msg.payload).ok()?;
        if env.reply_to.is_none() {
            if let Some(r) = reply {
                env.reply_to = Some(r.to_string());
            }
        }
        Some(env)
    }

    async fn close(&self) {
        let _ = self.sub.lock().await.drain().await;
    }
}

struct NatsInboxSub {
    consumer: Consumer<pull::Config>,
    js: Context,
    nc: Client,
}

#[async_trait]
impl InboxSubscription for NatsInboxSub {
    async fn next(&self) -> Option<Box<dyn InboxMsg>> {
        let mut batch = match self
            .consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_secs(2))
            .messages()
            .await
        {
            Ok(b) => b,
            Err(_) => return None,
        };
        let m = batch.next().await?.ok()?;
        let reply = m.reply.as_ref().map(|r| r.as_str());
        let env = decode_envelope(&m.payload).ok()?;
        let _ = reply;
        Some(Box::new(NatsInboxMsg {
            env,
            msg: Arc::new(m),
            js: self.js.clone(),
            nc: self.nc.clone(),
        }))
    }

    async fn close(&self) {}
}

struct NatsInboxMsg {
    env: pb::Envelope,
    msg: Arc<async_nats::jetstream::Message>,
    js: Context,
    nc: Client,
}

#[async_trait]
impl InboxMsg for NatsInboxMsg {
    fn envelope(&self) -> &pb::Envelope {
        &self.env
    }
    async fn ack(&self) {
        let _ = self.msg.ack().await;
    }
    async fn nak(&self, delay_ms: u64) {
        let _ = self
            .msg
            .ack_with(async_nats::jetstream::AckKind::Nak(Some(
                Duration::from_millis(delay_ms),
            )))
            .await;
    }
    async fn term(&self) {
        let subject = dlq_subject_for(&self.msg.subject);
        let mut hm = HeaderMap::new();
        if let Some(id) = &self.env.id {
            hm.insert("Nats-Msg-Id", id.as_str());
        }
        let _ = self
            .js
            .publish_with_headers(subject, hm, self.msg.payload.clone())
            .await;
        let _ = self
            .msg
            .ack_with(async_nats::jetstream::AckKind::Term)
            .await;
    }
    async fn term_no_dlq(&self) {
        let _ = self
            .msg
            .ack_with(async_nats::jetstream::AckKind::Term)
            .await;
    }
}

/// A convenience for building a payload `Value`.
pub fn json_value(v: Value) -> PbValue {
    to_value(&v)
}

/// Unused helper kept for parity with the other SDKs.
pub fn _unused(_: &Value) -> Value {
    json!(null)
}

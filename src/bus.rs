//! The transport-agnostic message bus — the Rust twin of the Go
//! `easybus-sdk/bus` package. The wire envelope is the GENERATED
//! `easybus.v1.Envelope` (see [`crate::wire`]).

use crate::gen::easybus::v1 as pb;
use async_trait::async_trait;
use tokio::sync::mpsc;

#[derive(Clone, Debug, Default)]
pub struct RequestOpts {
    pub timeout_ms: u64,
    pub max_wait_ms: u64,
    pub session_name: String,
    pub tenant: String,
}

#[derive(Clone, Debug, Default)]
pub struct SubscribeOpts {
    pub queue: String,
}

#[derive(Clone, Debug, Default)]
pub struct PublishOpts {
    pub reply_to: String,
    pub tenant: String,
}

#[derive(Clone, Debug, Default)]
pub struct InboxPublishOpts {
    pub id: String,
    pub session_name: String,
    pub tenant: String,
}

#[derive(Clone, Debug, Default)]
pub struct InboxConsumeOpts {
    pub subject: String,
}

#[derive(Clone, Debug, Default)]
pub struct KvEvent {
    pub key: String,
    pub value: String,
    pub revision: i64,
    pub deleted: bool,
    pub is_update: bool,
    pub done: bool,
}

/// A live channel subscription.
#[async_trait]
pub trait Subscription: Send + Sync {
    /// Await the next envelope (None on close/timeout).
    async fn next(&self) -> Option<pb::Envelope>;
    async fn close(&self);
}

/// A durable-inbox message with explicit ack/nak/term.
#[async_trait]
pub trait InboxMsg: Send + Sync {
    fn envelope(&self) -> &pb::Envelope;
    async fn ack(&self);
    async fn nak(&self, delay_ms: u64);
    /// Terminate + copy to the dead-letter stream.
    async fn term(&self);
    /// Terminate without the dead-letter copy.
    async fn term_no_dlq(&self);
}

#[async_trait]
pub trait InboxSubscription: Send + Sync {
    async fn next(&self) -> Option<Box<dyn InboxMsg>>;
    async fn close(&self);
}

/// The transport-agnostic message bus.
#[async_trait]
pub trait Bus: Send + Sync {
    async fn request(&self, ch: &str, payload: serde_json::Value, opts: RequestOpts) -> Option<pb::Envelope>;
    async fn request_many(&self, ch: &str, payload: serde_json::Value, opts: RequestOpts) -> Vec<pb::Envelope>;
    async fn publish(&self, ch: &str, payload: serde_json::Value, opts: PublishOpts);
    async fn subscribe(&self, ch: &str, opts: SubscribeOpts) -> Box<dyn Subscription>;

    async fn inbox_publish(&self, ch: &str, payload: serde_json::Value, opts: InboxPublishOpts);
    async fn inbox_consume(&self, opts: InboxConsumeOpts) -> Box<dyn InboxSubscription>;

    /// Retained (durably queued) envelopes for a channel, oldest first.
    async fn replay(&self, ch: &str) -> Vec<pb::Envelope>;

    async fn object_put(&self, name: &str, data: Vec<u8>);
    async fn object_get(&self, name: &str) -> Option<Vec<u8>>;
    async fn object_put_persistent(&self, name: &str, data: Vec<u8>);
    async fn object_get_persistent(&self, name: &str) -> Option<Vec<u8>>;

    async fn kv_watch(&self, bucket: &str, keys: &str) -> (mpsc::Receiver<KvEvent>, Box<dyn Fn() + Send + Sync>);
    async fn kv_create(&self, bucket: &str, key: &str, value: &str, ttl_ms: u64) -> i64;
    async fn kv_put(&self, bucket: &str, key: &str, value: &str, ttl_ms: u64);
    async fn kv_get(&self, bucket: &str, key: &str) -> String;
    async fn kv_cas(&self, bucket: &str, key: &str, value: &str, revision: i64) -> i64;
    async fn kv_delete(&self, bucket: &str, key: &str);

    async fn close(&self);
}

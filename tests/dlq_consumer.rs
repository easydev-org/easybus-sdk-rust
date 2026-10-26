//! The consumer side of the DLQ.
//!
//! The other DLQ cases prove a message ARRIVES in the dead-letter stream. This proves something
//! can pick it up — a queue nobody reads is a graveyard, and before `consume_dlq` existed that was
//! the only option in this SDK.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use easybus_sdk::agent::Agent;
use easybus_sdk::bus::{Bus, InboxPublishOpts};
use easybus_sdk::transport::nats::{NatsBus, Options};

fn nats_url() -> Option<String> {
    std::env::var("EASYBUS_NATS_URL").ok()
}

#[tokio::test]
async fn dlq_consumer_delivers() {
    let Some(url) = nats_url() else {
        eprintln!("no EASYBUS_NATS_URL; skipping");
        return;
    };
    // The variable being SET is not the same as a broker being there: a CI matrix can export it
    // while starting no server, and then the connection is refused. Skip in that case rather than
    // fail — a case that cannot reach a broker has not tested anything.
    let bus: Arc<NatsBus> = match NatsBus::connect(&url, Options::default()).await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("no broker at {url} ({e}); skipping");
            return;
        }
    };
    // Agent::new takes Arc<dyn Bus> and returns Arc<Self>.
    let agent = Agent::new(bus.clone() as Arc<dyn Bus>);

    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let _handle = agent
        .consume_dlq(move |m| {
            sink.lock().unwrap().push(m.typ.clone());
            true // ack
        })
        .await
        .expect("consume dlq");

    // The subscription is established asynchronously; give it a beat so the test fails on
    // DELIVERY rather than on a subscribe race.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let tag = "dlq-deliver-rust";
    bus.inbox_publish(
        &format!("easybus.t1.dlq.{tag}"),
        serde_json::json!({"id": tag, "type": tag, "payload": {"n": 1}}),
        InboxPublishOpts {
            id: tag.to_string(),
            session_name: "sess-dlq2".to_string(),
            tenant: "t1".to_string(),
        },
    )
    .await;

    let mut delivered = false;
    for _ in 0..150 {
        if seen.lock().unwrap().iter().any(|t| t == tag) {
            delivered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let got = seen.lock().unwrap().clone();
    assert!(delivered, "consume_dlq never delivered {tag}; saw {got:?}");
    bus.close().await;
}

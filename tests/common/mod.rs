//! Shared test helpers.
//!
//! ONE nats-server for the whole test binary (like the TS suite, which starts a
//! single broker per file). Tests run in parallel and therefore each uses a
//! UNIQUE tenant, so their KV/stream state never collides.
use std::net::TcpStream;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

static SERVER: OnceLock<Option<(String, Child)>> = OnceLock::new();
static NEXT_TENANT: AtomicU32 = AtomicU32::new(0);

/// The shared nats-server URL, or None when the binary is absent.
pub fn nats_url() -> Option<&'static str> {
    SERVER
        .get_or_init(spawn_nats)
        .as_ref()
        .map(|(u, _)| u.as_str())
}

/// A unique tenant for the calling test (parallel-safe isolation).
pub fn tenant() -> &'static str {
    let n = NEXT_TENANT.fetch_add(1, Ordering::SeqCst);
    Box::leak(format!("t{n}").into_boxed_str())
}

fn spawn_nats() -> Option<(String, Child)> {
    let bin = std::env::var("EASYBUS_NATS_SERVER_BIN").unwrap_or_else(|_| "nats-server".into());
    let port = 15600 + (std::process::id() % 700) as u16;
    let dir = std::env::temp_dir().join(format!("easybus-rs-{port}"));
    let child = Command::new(bin)
        .args(["-p", &port.to_string(), "-sd", dir.to_str().unwrap(), "-js"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Some((format!("nats://127.0.0.1:{port}"), child));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    None
}

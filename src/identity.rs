//! Static pre-shared identity signing (HMAC-SHA256) — the Rust twin of the Go
//! `easybus-sdk/identity` package.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub struct Identity {
    pub id: String,
    pub secret: String,
}

#[derive(Clone, Debug, Default)]
pub struct Fields {
    pub ch: String,
    pub kind: String,
    pub tenant: String,
    pub id: String,
    pub payload: Option<Value>,
}

#[derive(Clone, Debug)]
pub struct Header {
    pub id: String,
    pub sig: String,
}

fn canonical(identity_id: &str, f: &Fields) -> String {
    let payload = f
        .payload
        .as_ref()
        .map(|p| serde_json::to_string(p).unwrap_or_default())
        .unwrap_or_default();
    format!("{}\n{}\n{}\n{}\n{}\n{}", identity_id, f.tenant, f.ch, f.kind, f.id, payload)
}

fn sign(identity: &Identity, f: &Fields) -> String {
    let mut mac = HmacSha256::new_from_slice(identity.secret.as_bytes()).expect("hmac key");
    mac.update(canonical(&identity.id, f).as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

/// Build the sender-side auth header.
pub fn auth_header(identity: &Identity, f: &Fields) -> Header {
    Header {
        id: identity.id.clone(),
        sig: sign(identity, f),
    }
}

/// Check an HMAC against a known secret (constant-time).
pub fn verify(claimed_id: &str, secret: &str, f: &Fields, provided: &str) -> bool {
    let want = sign(
        &Identity {
            id: claimed_id.to_string(),
            secret: secret.to_string(),
        },
        f,
    );
    // Constant-time compare.
    if want.len() != provided.len() {
        return false;
    }
    want.bytes().zip(provided.bytes()).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_verify() {
        let idn = Identity {
            id: "ext-1".into(),
            secret: "s3cret".into(),
        };
        let f = Fields {
            ch: "easybus.t1.tool.call.ops.echo".into(),
            kind: "req".into(),
            tenant: "t1".into(),
            ..Default::default()
        };
        let h = auth_header(&idn, &f);
        assert!(verify(&h.id, "s3cret", &f, &h.sig));
        assert!(!verify(&h.id, "wrong", &f, &h.sig));
    }
}

//! Wire codec — the Go-compatible JSON form of the generated messages.
//!
//! The Go SDK marshals with `encoding/json` (NOT protojson): snake_case field
//! names, proto enum values as INTEGERS, and the Envelope `kind` as a SHORT
//! STRING ("req"/"pub"/"queue"). prost has no JSON codec, so this module maps
//! each message to/from `serde_json::Value` by hand — deliberately, to stay
//! byte-compatible with Go. NOTHING here changes the wire SHAPE; it only spells
//! out the JSON the Go `encoding/json` path produces.

use crate::gen::easybus::v1 as pb;
use prost::Message;
use serde_json::{json, Map, Value};

// ---- Value <-> serde_json --------------------------------------------------

/// Wrap a plain JSON value into a google.protobuf.Value.
pub fn to_value(v: &Value) -> prost_types::Value {
    use prost_types::value::Kind;
    let kind = match v {
        Value::Null => Kind::NullValue(0),
        Value::Bool(b) => Kind::BoolValue(*b),
        Value::Number(n) => Kind::NumberValue(n.as_f64().unwrap_or(0.0)),
        Value::String(s) => Kind::StringValue(s.clone()),
        Value::Array(a) => Kind::ListValue(prost_types::ListValue {
            values: a.iter().map(to_value).collect(),
        }),
        Value::Object(o) => Kind::StructValue(prost_types::Struct {
            fields: o.iter().map(|(k, v)| (k.clone(), to_value(v))).collect(),
        }),
    };
    prost_types::Value { kind: Some(kind) }
}

/// Unwrap a google.protobuf.Value into a plain JSON value.
pub fn from_value(v: &prost_types::Value) -> Value {
    use prost_types::value::Kind;
    match &v.kind {
        None | Some(Kind::NullValue(_)) => Value::Null,
        Some(Kind::BoolValue(b)) => Value::Bool(*b),
        Some(Kind::NumberValue(n)) => serde_json::Number::from_f64(*n).map(Value::Number).unwrap_or(Value::Null),
        Some(Kind::StringValue(s)) => Value::String(s.clone()),
        Some(Kind::ListValue(l)) => Value::Array(l.values.iter().map(from_value).collect()),
        Some(Kind::StructValue(s)) => Value::Object(s.fields.iter().map(|(k, v)| (k.clone(), from_value(v))).collect()),
    }
}

/// Unwrap an `Option<prost_types::Value>` (None -> Null).
pub fn from_value_opt(v: &Option<prost_types::Value>) -> Value {
    v.as_ref().map(from_value).unwrap_or(Value::Null)
}

// ---- enum helpers ----------------------------------------------------------

pub fn kind_to_string(kind: i32) -> &'static str {
    match kind {
        x if x == pb::envelope::Kind::Req as i32 => "req",
        x if x == pb::envelope::Kind::Queue as i32 => "queue",
        _ => "pub",
    }
}

pub fn kind_from_string(s: &str) -> i32 {
    match s {
        "req" => pb::envelope::Kind::Req as i32,
        "queue" => pb::envelope::Kind::Queue as i32,
        _ => pb::envelope::Kind::Pub as i32,
    }
}

/// The wire name of a proto `Code` (integer -> "CODE_*").
pub fn code_from_wire(code: i64) -> String {
    match code {
        0 => "CODE_UNSPECIFIED",
        1 => "CODE_INVALID_ARGUMENT",
        2 => "CODE_NOT_FOUND",
        3 => "CODE_PERMISSION_DENIED",
        4 => "CODE_RETRYABLE",
        5 => "CODE_BUSINESS",
        6 => "CODE_INTERNAL",
        _ => "CODE_UNSPECIFIED",
    }
    .to_string()
}

/// The wire integer for a proto `Code` name.
pub fn code_to_wire(name: &str) -> i32 {
    match name {
        "CODE_INVALID_ARGUMENT" => 1,
        "CODE_NOT_FOUND" => 2,
        "CODE_PERMISSION_DENIED" => 3,
        "CODE_RETRYABLE" => 4,
        "CODE_BUSINESS" => 5,
        "CODE_INTERNAL" => 6,
        _ => 0,
    }
}

pub fn lifecycle_kind_string(kind: i32) -> &'static str {
    match kind {
        x if x == pb::lifecycle_event::Kind::Created as i32 => "created",
        x if x == pb::lifecycle_event::Kind::Forked as i32 => "forked",
        x if x == pb::lifecycle_event::Kind::Renamed as i32 => "renamed",
        x if x == pb::lifecycle_event::Kind::Deleted as i32 => "deleted",
        _ => "",
    }
}

pub fn lifecycle_kind_enum(kind: &str) -> i32 {
    match kind {
        "created" => pb::lifecycle_event::Kind::Created as i32,
        "forked" => pb::lifecycle_event::Kind::Forked as i32,
        "renamed" => pb::lifecycle_event::Kind::Renamed as i32,
        "deleted" => pb::lifecycle_event::Kind::Deleted as i32,
        _ => pb::lifecycle_event::Kind::Unspecified as i32,
    }
}

// ---- Envelope --------------------------------------------------------------

/// Encode an Envelope to the Go-compatible JSON bytes (snake_case, `kind` a
/// short string).
pub fn encode_envelope(env: &pb::Envelope) -> Vec<u8> {
    let mut m = Map::new();
    if let Some(v) = env.v {
        m.insert("v".into(), json!(v));
    }
    if !env.ch.is_empty() {
        m.insert("ch".into(), json!(env.ch));
    }
    if !env.tenant.is_empty() {
        m.insert("tenant".into(), json!(env.tenant));
    }
    m.insert("kind".into(), json!(kind_to_string(env.kind)));
    if let Some(p) = &env.payload {
        m.insert("payload".into(), from_value(p));
    }
    if let Some(id) = &env.id {
        m.insert("id".into(), json!(id));
    }
    if let Some(r) = &env.reply_to {
        m.insert("reply_to".into(), json!(r));
    }
    if let Some(s) = &env.session_name {
        m.insert("session_name".into(), json!(s));
    }
    if let Some(p) = env.pending {
        m.insert("pending".into(), json!(p));
    }
    serde_json::to_vec(&Value::Object(m)).unwrap_or_default()
}

/// Parse Go-compatible JSON bytes into an Envelope.
pub fn decode_envelope(data: &[u8]) -> Result<pb::Envelope, serde_json::Error> {
    let v: Value = serde_json::from_slice(data)?;
    let mut env = pb::Envelope::default();
    if let Some(o) = v.as_object() {
        env.v = o.get("v").and_then(|x| x.as_i64()).map(|x| x as i32);
        env.ch = o.get("ch").and_then(|x| x.as_str()).unwrap_or("").to_string();
        env.tenant = o.get("tenant").and_then(|x| x.as_str()).unwrap_or("").to_string();
        env.kind = o.get("kind").and_then(|x| x.as_str()).map(kind_from_string).unwrap_or(pb::envelope::Kind::Pub as i32);
        env.payload = o.get("payload").map(to_value);
        env.id = o.get("id").and_then(|x| x.as_str()).map(|s| s.to_string());
        env.reply_to = o.get("reply_to").and_then(|x| x.as_str()).map(|s| s.to_string());
        env.session_name = o.get("session_name").and_then(|x| x.as_str()).map(|s| s.to_string());
        env.pending = o.get("pending").and_then(|x| x.as_i64()).map(|x| x as i32);
    }
    Ok(env)
}

// ---- generic message <-> JSON ---------------------------------------------

/// Encode any generated message to Go-compatible JSON bytes.
///
/// prost messages carry no JSON mapping, so the concrete shapes are spelled out
/// here (the handful the protocol actually puts on the wire).
pub fn encode_tool_result(tr: &pb::ToolResult) -> Vec<u8> {
    let mut m = Map::new();
    m.insert("call_id".into(), json!(tr.call_id));
    m.insert("tool".into(), json!(tr.tool));
    if let Some(c) = &tr.content {
        m.insert("content".into(), json!(c));
    }
    if let Some(d) = &tr.data {
        m.insert("data".into(), from_value(d));
    }
    if let Some(md) = &tr.metadata {
        m.insert("metadata".into(), from_value(md));
    }
    if let Some(e) = &tr.error {
        m.insert("error".into(), json!({"code": e.code, "message": e.message}));
    }
    if let Some(o) = &tr.object {
        let mut om = Map::new();
        om.insert("id".into(), json!(o.id));
        if let Some(ct) = &o.content_type {
            om.insert("content_type".into(), json!(ct));
        }
        m.insert("object".into(), Value::Object(om));
    }
    serde_json::to_vec(&Value::Object(m)).unwrap_or_default()
}

/// Parse a Go-compatible ToolResult.
pub fn decode_tool_result(data: &[u8]) -> Result<pb::ToolResult, serde_json::Error> {
    let v: Value = serde_json::from_slice(data)?;
    let mut tr = pb::ToolResult::default();
    if let Some(o) = v.as_object() {
        tr.call_id = o.get("call_id").and_then(|x| x.as_str()).unwrap_or("").to_string();
        tr.tool = o.get("tool").and_then(|x| x.as_str()).unwrap_or("").to_string();
        tr.content = o.get("content").and_then(|x| x.as_str()).map(|s| s.to_string());
        tr.data = o.get("data").map(to_value);
        tr.metadata = o.get("metadata").map(to_value);
        if let Some(e) = o.get("error").and_then(|x| x.as_object()) {
            tr.error = Some(pb::ToolError {
                code: e.get("code").and_then(|x| x.as_i64()).unwrap_or(0) as i32,
                message: e.get("message").and_then(|x| x.as_str()).unwrap_or("").to_string(),
            });
        }
        if let Some(ob) = o.get("object").and_then(|x| x.as_object()) {
            tr.object = Some(pb::ObjectRef {
                id: ob.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                content_type: ob.get("content_type").and_then(|x| x.as_str()).map(|s| s.to_string()),
            });
        }
    }
    Ok(tr)
}

/// Encode an ExtensionManifest to Go-compatible JSON bytes.
pub fn encode_manifest(m: &pb::ExtensionManifest) -> Vec<u8> {
    let mut out = Map::new();
    out.insert("id".into(), json!(m.id));
    out.insert("version".into(), json!(m.version));
    out.insert("capabilities".into(), json!(m.capabilities));
    out.insert("features".into(), json!(m.features));
    let tools: Vec<Value> = m
        .tools
        .iter()
        .map(|t| {
            json!({
                "name": t.name,
                "description": t.description,
                "descriptions": t.descriptions,
                "input_schema": struct_to_json(&t.input_schema),
                "required_config": t.required_config,
            })
        })
        .collect();
    out.insert("tools".into(), Value::Array(tools));
    let vars: Vec<Value> = m
        .prompt_variables
        .iter()
        .map(|v| json!({"name": v.name, "description": v.description, "descriptions": v.descriptions, "scope": v.scope}))
        .collect();
    out.insert("prompt_variables".into(), Value::Array(vars));
    let config: Vec<Value> = m
        .config
        .iter()
        .map(|c| {
            let mut o = Map::new();
            o.insert("name".into(), json!(c.name));
            o.insert("type".into(), json!(c.r#type));
            if let Some(k) = &c.kind {
                o.insert("kind".into(), json!(k));
            }
            if let Some(cap) = &c.capability {
                o.insert("capability".into(), json!(cap));
            }
            o.insert("enum_values".into(), json!(c.enum_values));
            if let Some(d) = &c.description {
                o.insert("description".into(), json!(d));
            }
            o.insert("descriptions".into(), json!(c.descriptions));
            o.insert("scope".into(), json!(c.scope));
            if let Some(d) = &c.default {
                o.insert("default".into(), from_value(d));
            }
            Value::Object(o)
        })
        .collect();
    out.insert("config".into(), Value::Array(config));
    out.insert("lifecycle".into(), json!(m.lifecycle));
    let (hcall, hevent) = match &m.hooks {
        Some(h) => (h.call.clone(), h.event.clone()),
        None => (vec![], vec![]),
    };
    out.insert("hooks".into(), json!({"call": hcall, "event": hevent}));
    serde_json::to_vec(&Value::Object(out)).unwrap_or_default()
}

/// Parse a Go-compatible ExtensionManifest.
pub fn decode_manifest(data: &[u8]) -> Result<pb::ExtensionManifest, serde_json::Error> {
    let v: Value = serde_json::from_slice(data)?;
    let mut m = pb::ExtensionManifest::default();
    let Some(o) = v.as_object() else { return Ok(m) };
    m.id = o.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
    m.version = o.get("version").and_then(|x| x.as_str()).unwrap_or("").to_string();
    m.capabilities = str_vec(o.get("capabilities"));
    m.features = str_vec(o.get("features"));
    m.lifecycle = str_vec(o.get("lifecycle"));
    if let Some(ts) = o.get("tools").and_then(|x| x.as_array()) {
        for t in ts {
            m.tools.push(pb::ExtensionTool {
                name: t.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                description: t.get("description").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                input_schema: t.get("input_schema").and_then(json_to_struct),
                required_config: str_vec(t.get("required_config")),
                ..Default::default()
            });
        }
    }
    if let Some(cs) = o.get("config").and_then(|x| x.as_array()) {
        for c in cs {
            m.config.push(pb::ExtensionConfigItem {
                name: c.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                r#type: c.get("type").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                kind: c.get("kind").and_then(|x| x.as_str()).map(|s| s.to_string()),
                capability: c.get("capability").and_then(|x| x.as_str()).map(|s| s.to_string()),
                enum_values: str_vec(c.get("enum_values")),
                description: c.get("description").and_then(|x| x.as_str()).map(|s| s.to_string()),
                scope: c.get("scope").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                default: c.get("default").map(to_value),
                ..Default::default()
            });
        }
    }
    if let Some(h) = o.get("hooks").and_then(|x| x.as_object()) {
        m.hooks = Some(pb::HookSpec {
            call: str_vec(h.get("call")),
            event: str_vec(h.get("event")),
            ..Default::default()
        });
    }
    Ok(m)
}

fn struct_to_json(s: &Option<prost_types::Struct>) -> Value {
    match s {
        Some(st) => Value::Object(st.fields.iter().map(|(k, v)| (k.clone(), from_value(v))).collect()),
        None => Value::Object(Map::new()),
    }
}

fn json_to_struct(v: &Value) -> Option<prost_types::Struct> {
    match v {
        Value::Object(o) => Some(prost_types::Struct {
            fields: o.iter().map(|(k, val)| (k.clone(), to_value(val))).collect(),
        }),
        _ => None,
    }
}

fn str_vec(v: Option<&Value>) -> Vec<String> {
    v.and_then(|x| x.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
        .unwrap_or_default()
}

/// Encode any prost message via a provided JSON `Value` (escape hatch).
pub fn encode_json(v: &Value) -> Vec<u8> {
    serde_json::to_vec(v).unwrap_or_default()
}

/// A prost message to bytes (for KV payloads that store raw proto, unused here).
pub fn prost_encode<M: Message>(m: &M) -> Vec<u8> {
    m.encode_to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrip() {
        let mut env = pb::Envelope {
            v: Some(2),
            ch: "easybus.t1.tool.call.ops.echo".into(),
            tenant: "t1".into(),
            kind: pb::envelope::Kind::Req as i32,
            reply_to: Some("inbox".into()),
            ..Default::default()
        };
        env.payload = Some(to_value(&json!({"call_id": "c1"})));
        let bytes = encode_envelope(&env);
        let s = String::from_utf8(bytes.clone()).unwrap();
        assert!(s.contains("\"kind\":\"req\""));
        assert!(s.contains("\"reply_to\":\"inbox\""));
        let back = decode_envelope(&bytes).unwrap();
        assert_eq!(back.kind, pb::envelope::Kind::Req as i32);
        assert_eq!(back.ch, env.ch);
    }
}

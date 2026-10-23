//! The YAML manifest -> extension Config builder — the Rust twin of the Go
//! `easybus-sdk/manifest` package. The manifest declares id/version/tools/
//! variables/config/lifecycle/hooks; handlers are bound by name.

use crate::extension::{Config, ConfigSpec, ToolSpec, VariableSpec};
use serde::Deserialize;
use std::collections::HashMap;

/// Config knob kinds (kind is an open string; these are the special values).
pub const CONFIG_KIND_VALUE: &str = "value";
pub const CONFIG_KIND_MODEL: &str = "model";
/// A secret's value is never returned to a client (the agent withholds it and
/// reports only whether it is set).
pub const CONFIG_KIND_SECRET: &str = "secret";

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ManifestTool {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub descriptions: HashMap<String, String>,
    #[serde(default)]
    pub input_schema: Option<serde_json::Value>,
    #[serde(default)]
    pub required_config: Vec<String>,
    /// Grouping of this tool (pvc, build, storage, ...). Organisational only:
    /// the tool keeps its name; it lets a client group the list.
    #[serde(default)]
    pub namespace: String,
    /// Permission keys a caller must hold to invoke this tool.
    #[serde(default)]
    pub requires: Vec<String>,
}

/// One permission an extension can require.
///
/// A permission is a (key, params) pair, NOT a bare name: "merge in agentdev/mrflow" is a
/// different authorisation from "merge in other/repo", so a session may hold several grants of the
/// same key with different params.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ManifestPermission {
    pub key: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub descriptions: HashMap<String, String>,
    /// read | write | destructive. Presentation only — it does not affect enforcement.
    #[serde(default)]
    pub risk: String,
    /// The params a grant carries, using the same field schema as a structured config knob.
    #[serde(default)]
    pub params: Vec<ManifestField>,
}

/// One topic pattern a channel uses.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ManifestChannelTopic {
    #[serde(default)]
    pub pattern: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub descriptions: HashMap<String, String>,
}

/// One channel an extension publishes to.
///
/// A channel is a pub/sub topic, not a command queue: knowing the topic FORMAT is what lets a user
/// write a subscription pattern (`tag.agentdev/mrflow.>`) instead of following everything.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ManifestChannel {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub descriptions: HashMap<String, String>,
    #[serde(default)]
    pub topics: Vec<ManifestChannelTopic>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ManifestVariable {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub descriptions: HashMap<String, String>,
    #[serde(default)]
    pub scope: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ManifestConfig {
    pub name: String,
    #[serde(default, rename = "type")]
    pub r#type: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub capability: String,
    #[serde(default)]
    pub enum_values: Vec<String>,
    #[serde(default)]
    pub default: Option<serde_json::Value>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub descriptions: HashMap<String, String>,
    #[serde(default)]
    pub scope: String,
    /// Shape of a structured knob (type list/object): the keys of the object, or
    /// of each list item. Empty = untyped json blob (the fallback).
    #[serde(default)]
    pub fields: Vec<ManifestField>,
    /// Element type when `type == "list"` (object | string | number | boolean).
    #[serde(default)]
    pub item_type: String,
    #[serde(default)]
    pub min_items: i32,
    #[serde(default)]
    pub max_items: i32,
    /// Scopes this knob's STORED VALUE to one namespace of the extension.
    /// Empty = extension-level (shared by all its namespaces).
    #[serde(default)]
    pub namespace: String,
}

/// One key of a structured config value (or of one list item's keys).
///
/// It exists so a client can render a real form instead of a JSON textarea: an
/// `enum` becomes a picker, `kind = "secret"` a masked input, `format = "uri"` a
/// URL field, and a list of objects becomes repeatable cards with a "+".
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ManifestField {
    pub name: String,
    #[serde(default, rename = "type")]
    pub r#type: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub format: String,
    #[serde(default)]
    pub enum_values: Vec<String>,
    #[serde(default)]
    pub default: Option<serde_json::Value>,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub descriptions: HashMap<String, String>,
    /// Nested shape for an object field (one level: enough for every current
    /// knob, and it keeps the renderer honest).
    #[serde(default)]
    pub fields: Vec<ManifestField>,
}

/// One declared grouping of an extension's tools and config.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ManifestNamespace {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub descriptions: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ManifestHooks {
    #[serde(default)]
    pub call: Vec<String>,
    #[serde(default)]
    pub event: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Manifest {
    pub id: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub tools: Vec<ManifestTool>,
    #[serde(default)]
    pub variables: Vec<ManifestVariable>,
    #[serde(default)]
    pub config: Vec<ManifestConfig>,
    #[serde(default)]
    pub lifecycle: Vec<String>,
    #[serde(default)]
    pub hooks: Option<ManifestHooks>,
    /// Declared groupings (pvc, build, storage, ...), in the author's ORDER.
    #[serde(default)]
    pub namespaces: Vec<ManifestNamespace>,
    /// Permissions this extension can require.
    ///
    /// Missing this field is how a manifest could declare `permissions:` and have them silently
    /// dropped: serde ignores unknown keys by default, so the YAML parsed, the data had nowhere to
    /// go, and every tool ran ungated while the file said otherwise.
    #[serde(default)]
    pub permissions: Vec<ManifestPermission>,
    /// Channels this extension publishes to, so a user can discover the topic format.
    #[serde(default)]
    pub channels: Vec<ManifestChannel>,
}

/// parse_manifest parses YAML manifest text and validates its config knobs.
pub fn parse_manifest(data: &str) -> Result<Manifest, String> {
    let m: Manifest = serde_yaml::from_str(data).map_err(|e| format!("parse manifest: {e}"))?;
    if m.id.is_empty() {
        return Err("manifest: id is required".into());
    }
    for c in &m.config {
        if c.kind != CONFIG_KIND_SECRET {
            continue;
        }
        // A secret knob is a credential: its value is never returned to a
        // client, and a `default` would ride the (broadcast) manifest — so a
        // secret MUST NOT declare a default.
        //
        // It MAY be structured (a list/object whose records each carry their own
        // `kind: secret`), which is how a sandbox list declares a token per
        // sandbox: withholding the whole knob is then a second guard on top of the
        // per-field masking.
        if !c.r#type.is_empty() && !matches!(c.r#type.as_str(), "string" | "list" | "object") {
            return Err(format!(
                "manifest: config \"{}\": kind=secret requires type string, list or object (got \"{}\")",
                c.name, c.r#type
            ));
        }
        if let Some(d) = &c.default {
            if !d.is_null() && d.as_str() != Some("") {
                return Err(format!(
                    "manifest: config \"{}\": kind=secret must not declare a default",
                    c.name
                ));
            }
        }
    }
    Ok(m)
}

/// Bindings carries the name-keyed handlers/resolvers used to build a Config.
#[derive(Default)]
pub struct Bindings {
    pub handlers: HashMap<String, std::sync::Arc<dyn crate::extension::ToolHandler>>,
    pub variables: HashMap<String, VariableSpec>,
}

/// Map declared fields onto the wire-facing specs (recursing one level).
///
/// A field may itself be an object; both the client renderer and the validator
/// stop at that depth on purpose.
fn to_field_specs(fields: &[ManifestField]) -> Vec<crate::extension::FieldSpec> {
    fields
        .iter()
        .map(|f| crate::extension::FieldSpec {
            name: f.name.clone(),
            r#type: f.r#type.clone(),
            kind: f.kind.clone(),
            format: f.format.clone(),
            enum_values: f.enum_values.clone(),
            default: f.default.clone(),
            required: f.required,
            description: f.description.clone(),
            descriptions: f.descriptions.clone(),
            fields: to_field_specs(&f.fields),
        })
        .collect()
}

/// Maps declared param fields onto the wire schema.
///
/// A permission's params use the SAME ConfigFieldSchema as a structured config knob, so a client
/// renders one form implementation for both instead of a second type system.
pub fn params_to_schema(
    fields: &[ManifestField],
) -> Vec<crate::gen::easybus::v1::ConfigFieldSchema> {
    fields
        .iter()
        .map(|f| {
            let spec = crate::extension::FieldSpec {
                name: f.name.clone(),
                r#type: f.r#type.clone(),
                kind: f.kind.clone(),
                format: f.format.clone(),
                enum_values: f.enum_values.clone(),
                default: f.default.clone(),
                required: f.required,
                description: f.description.clone(),
                descriptions: f.descriptions.clone(),
                fields: to_field_specs(&f.fields),
            };
            crate::extension::field_spec_to_schema(&spec)
        })
        .collect()
}

/// build_config builds an extension.Config from the manifest + bindings.
pub fn build_config(m: &Manifest, b: Bindings) -> Config {
    let mut tools: HashMap<String, (ToolSpec, std::sync::Arc<dyn crate::extension::ToolHandler>)> =
        HashMap::new();
    for t in &m.tools {
        if let Some(h) = b.handlers.get(&t.name) {
            tools.insert(
                t.name.clone(),
                (
                    ToolSpec {
                        description: t.description.clone(),
                        descriptions: t.descriptions.clone(),
                        input_schema: t.input_schema.clone().unwrap_or(serde_json::json!({})),
                        required_config: t.required_config.clone(),
                        namespace: t.namespace.clone(),
                        // The manifest is the single source for what a tool requires: a handler
                        // cannot quietly attach a requirement the manifest does not document, and
                        // the manifest entry is what a client reads to know what to ask for.
                        requires: t.requires.clone(),
                    },
                    h.clone(),
                ),
            );
        }
    }
    let mut variables: HashMap<String, VariableSpec> = HashMap::new();
    for v in &m.variables {
        variables.insert(
            v.name.clone(),
            VariableSpec {
                description: v.description.clone(),
                scope: if v.scope.is_empty() {
                    "global".into()
                } else {
                    v.scope.clone()
                },
            },
        );
    }
    let mut config: HashMap<String, ConfigSpec> = HashMap::new();
    for c in &m.config {
        config.insert(
            c.name.clone(),
            ConfigSpec {
                r#type: if c.r#type.is_empty() {
                    "string".into()
                } else {
                    c.r#type.clone()
                },
                kind: if c.kind.is_empty() {
                    "value".into()
                } else {
                    c.kind.clone()
                },
                capability: c.capability.clone(),
                enum_values: c.enum_values.clone(),
                default: c.default.clone(),
                description: c.description.clone(),
                scope: if c.scope.is_empty() {
                    "global".into()
                } else {
                    c.scope.clone()
                },
                fields: to_field_specs(&c.fields),
                item_type: c.item_type.clone(),
                min_items: c.min_items,
                max_items: c.max_items,
                namespace: c.namespace.clone(),
            },
        );
    }
    let namespaces = m
        .namespaces
        .iter()
        .map(|n| crate::extension::NamespaceSpec {
            name: n.name.clone(),
            description: n.description.clone(),
            descriptions: n.descriptions.clone(),
        })
        .collect();
    let (call_hooks, event_hooks) = match &m.hooks {
        Some(h) => (h.call.clone(), h.event.clone()),
        None => (vec![], vec![]),
    };
    Config {
        id: m.id.clone(),
        version: m.version.clone(),
        tools,
        variables,
        namespaces,
        config,
        call_hooks,
        event_hooks,
        lifecycle: m.lifecycle.clone(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_validation() {
        let ok = "id: x\nversion: \"1\"\nconfig:\n  - name: gh-token\n    type: string\n    kind: secret\n";
        assert!(parse_manifest(ok).is_ok());

        let bad_type = "id: x\nversion: \"1\"\nconfig:\n  - name: gh-token\n    type: number\n    kind: secret\n";
        let e = parse_manifest(bad_type).unwrap_err();
        assert!(e.contains("requires type"), "{e}");

        // A STRUCTURED secret is allowed: a sandbox list holds a token per
        // sandbox, so the knob is a list/object while each record's token field
        // carries the masking. Rejecting it made the typed form impossible for
        // exactly the knob that needs it most.
        let structured = "id: x\nversion: \"1\"\nconfig:\n  - name: sandboxes\n    type: list\n    kind: secret\n    item_type: object\n";
        assert!(
            parse_manifest(structured).is_ok(),
            "structured secret must be accepted"
        );

        let bad_default = "id: x\nversion: \"1\"\nconfig:\n  - name: gh-token\n    type: string\n    kind: secret\n    default: ghp_leaked\n";
        let e = parse_manifest(bad_default).unwrap_err();
        assert!(e.contains("must not declare a default"), "{e}");

        let plain =
            "id: x\nversion: \"1\"\nconfig:\n  - name: region\n    type: string\n    default: us\n";
        assert!(parse_manifest(plain).is_ok());
    }
}

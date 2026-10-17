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
        // secret MUST be a string and MUST NOT declare a default.
        if !c.r#type.is_empty() && c.r#type != "string" {
            return Err(format!(
                "manifest: config \"{}\": kind=secret requires type=string (got \"{}\")",
                c.name, c.r#type
            ));
        }
        if let Some(d) = &c.default {
            if !d.is_null() && d.as_str() != Some("") {
                return Err(format!("manifest: config \"{}\": kind=secret must not declare a default", c.name));
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

/// build_config builds an extension.Config from the manifest + bindings.
pub fn build_config(m: &Manifest, b: Bindings) -> Config {
    let mut tools: HashMap<String, (ToolSpec, std::sync::Arc<dyn crate::extension::ToolHandler>)> = HashMap::new();
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
                scope: if v.scope.is_empty() { "global".into() } else { v.scope.clone() },
            },
        );
    }
    let mut config: HashMap<String, ConfigSpec> = HashMap::new();
    for c in &m.config {
        config.insert(
            c.name.clone(),
            ConfigSpec {
                r#type: if c.r#type.is_empty() { "string".into() } else { c.r#type.clone() },
                kind: if c.kind.is_empty() { "value".into() } else { c.kind.clone() },
                capability: c.capability.clone(),
                enum_values: c.enum_values.clone(),
                default: c.default.clone(),
                description: c.description.clone(),
                scope: if c.scope.is_empty() { "global".into() } else { c.scope.clone() },
            },
        );
    }
    let (call_hooks, event_hooks) = match &m.hooks {
        Some(h) => (h.call.clone(), h.event.clone()),
        None => (vec![], vec![]),
    };
    Config {
        id: m.id.clone(),
        version: m.version.clone(),
        tools,
        variables,
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
        assert!(e.contains("type=string"), "{e}");

        let bad_default = "id: x\nversion: \"1\"\nconfig:\n  - name: gh-token\n    type: string\n    kind: secret\n    default: ghp_leaked\n";
        let e = parse_manifest(bad_default).unwrap_err();
        assert!(e.contains("must not declare a default"), "{e}");

        let plain = "id: x\nversion: \"1\"\nconfig:\n  - name: region\n    type: string\n    default: us\n";
        assert!(parse_manifest(plain).is_ok());
    }
}

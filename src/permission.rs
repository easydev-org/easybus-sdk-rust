//! Permission enforcement, extension side.
//!
//! The agent stores what a session MAY do and hands those grants over on every call. This module
//! is the other half: deciding whether a grant COVERS the call about to happen.
//!
//! WHY the decision lives here: coverage depends on the call's target — this repository, that
//! sandbox — and turning the arguments into that target is domain knowledge only the extension
//! has (`https://host/o/r.git` -> `o/r` is a git rule, not a protocol rule). Keeping it here is
//! what lets the same mechanism serve an extension that guards repositories and one that guards
//! sandboxes.
//!
//! The agent's own check is an optimisation, not a boundary: it can say "this session has no
//! merge permission at all", but it cannot say "this grant covers THIS pull request". Only this
//! module can, and only this module is the security boundary — the extension holds the credential
//! and performs the action.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;

use crate::gen::easybus::v1 as pb;

/// A grant's params, flattened to strings.
pub type GrantParams = HashMap<String, String>;

/// Returned when a required permission is absent or does not cover the call.
///
/// It carries the target that was attempted, so the caller can say WHICH repository was refused
/// rather than "permission denied" — and so an audit log records what was tried.
#[derive(Debug, Clone)]
pub struct PermissionDenied {
    pub key: String,
    pub target: GrantParams,
    pub granted: Vec<GrantParams>,
}

impl fmt::Display for PermissionDenied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "permission denied: {}", self.key)?;
        let want = format_params(&self.target);
        if !want.is_empty() {
            write!(f, " on {want}")?;
        }
        if self.granted.is_empty() {
            return write!(f, " (no grant held for this permission)");
        }
        let held: Vec<String> = self.granted.iter().map(format_params).collect();
        write!(f, " (held: {})", held.join("; "))
    }
}

impl std::error::Error for PermissionDenied {}

/// Renders params deterministically, so the same gap always reads the same.
fn format_params(p: &GrantParams) -> String {
    let mut keys: Vec<&String> = p.keys().collect();
    keys.sort();
    keys.iter()
        .map(|k| format!("{k}={}", p[*k]))
        .collect::<Vec<_>>()
        .join(",")
}

tokio::task_local! {
    /// The grants bound to the CURRENT call.
    ///
    /// WHY a task-local: a tool handler is async and may await before reaching its permission
    /// check, so threading grants through every signature would be invasive and easy to forget.
    /// A task-local is per-task by construction and does not leak between concurrent calls —
    /// the same properties Go gets from a context.Context and Python from a ContextVar.
    static GRANTS: Vec<pb::Grant>;
}

/// Runs `f` with `grants` bound for the duration of the call.
///
/// Used by the dispatcher; a tool handler only ever reads through [`require`]/[`allowed`].
pub async fn with_grants<F, T>(grants: Vec<pb::Grant>, f: F) -> T
where
    F: Future<Output = T>,
{
    GRANTS.scope(grants, f).await
}

/// Returns the params of every grant held for one key.
///
/// A session may hold SEVERAL grants of the same key with different params — "merge in
/// agentdev/mrflow" and "merge in scratch/*" are two authorisations, not one. Coverage means at
/// least one of them matches.
pub fn grants_for(key: &str) -> Vec<GrantParams> {
    let held = GRANTS.try_with(|g| g.clone()).unwrap_or_default();
    held.iter()
        .filter(|g| g.key == key)
        .map(|g| struct_to_strings(g.params.as_ref()))
        .collect()
}

/// Reports whether the caller may perform `key` against `target`.
pub fn allowed(key: &str, target: &GrantParams) -> bool {
    grants_for(key).iter().any(|g| matches(g, target))
}

/// Returns `Ok(())` when the call is covered, or the refusal when it is not.
///
/// The expected shape inside a tool:
///
/// ```ignore
/// let target = params([("repo", repo_slug(arg_str(&args, "remote")))]);
/// require("mr.merge", &target)?;
/// ```
pub fn require(key: &str, target: &GrantParams) -> Result<(), PermissionDenied> {
    if allowed(key, target) {
        return Ok(());
    }
    Err(PermissionDenied {
        key: key.to_string(),
        target: target.clone(),
        granted: grants_for(key),
    })
}

/// Reports whether a grant's params cover a target.
///
/// Semantics, deliberately small:
///
///   - every key in the GRANT must appear in the target and match (AND across keys);
///   - a value may use `*` as a wildcard: `agentdev/*` covers `agentdev/mrflow`;
///   - otherwise it is an exact comparison.
///
/// WHY not regex: a permission a reader cannot predict is one they cannot judge. `*` is the
/// smallest thing that makes "all my repositories" expressible, and a client can explain it in
/// one sentence. An extension that needs more can ignore these params and enforce its own rule.
pub fn matches(granted: &GrantParams, target: &GrantParams) -> bool {
    granted.iter().all(|(k, want)| match target.get(k) {
        Some(got) => match_value(want, got),
        None => false,
    })
}

/// Compares one param value, honouring `*`.
fn match_value(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    match pattern.find('*') {
        Some(i) => {
            let prefix = &pattern[..i];
            let suffix = &pattern[i + 1..];
            value.len() >= prefix.len() + suffix.len()
                && value.starts_with(prefix)
                && value.ends_with(suffix)
        }
        None => pattern == value,
    }
}

/// Flattens a param Struct to strings: the declared leaves are scalars.
fn struct_to_strings(s: Option<&prost_types::Struct>) -> GrantParams {
    let mut out = GrantParams::new();
    if let Some(st) = s {
        for (k, v) in &st.fields {
            out.insert(k.clone(), value_to_string(v));
        }
    }
    out
}

fn value_to_string(v: &prost_types::Value) -> String {
    use prost_types::value::Kind;
    match v.kind.as_ref() {
        Some(Kind::StringValue(s)) => s.clone(),
        Some(Kind::NumberValue(n)) => {
            // Render an integral number without a trailing ".0", so a param declared as a
            // number compares the way it was written.
            if n.fract() == 0.0 {
                format!("{}", *n as i64)
            } else {
                n.to_string()
            }
        }
        Some(Kind::BoolValue(b)) => b.to_string(),
        Some(Kind::NullValue(_)) | None => String::new(),
        Some(other) => format!("{other:?}"),
    }
}

/// Builds a target from (key, value) pairs, for brevity at call sites.
pub fn params<const N: usize>(pairs: [(&str, &str); N]) -> GrantParams {
    pairs
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

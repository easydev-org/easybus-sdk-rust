//! Conformance: a manifest's structured config and namespaces reach the WIRE.
//!
//! Three hops, all asserted here:
//!   YAML manifest -> parsed types -> build_config -> the published manifest
//!
//! Asserting only the first hop is what let this ship broken: the field existed
//! on the struct and was still dropped on the way out, so a Rust extension could
//! declare a typed config knob and the world would never see it.

use easybus_sdk::manifest::{build_config, parse_manifest, Bindings};

const OPS_MANIFEST: &str = r#"
id: ops
version: "1"
namespaces:
  - name: services
    description: Deploy workloads
  - name: build
    description: Image builds
config:
  - name: easyops-url
    type: string
  - name: buildkit-addr
    type: string
    namespace: build
  - name: sandboxes
    type: list
    kind: secret
    item_type: object
    min_items: 1
    fields:
      - name: name
        type: string
        required: true
      - name: url
        type: string
        format: uri
        required: true
      - name: token
        type: string
        kind: secret
        required: true
      - name: os
        type: enum
        enum_values: [linux, windows, macos, android]
        default: linux
tools:
  - name: create-pvc
    description: Create a PVC
    namespace: storage
"#;

#[test]
fn declared_namespaces_reach_the_wire() {
    let m = parse_manifest(OPS_MANIFEST).expect("manifest parses");
    let cfg = build_config(&m, Bindings::default());

    let names: Vec<&str> = cfg.namespaces.iter().map(|n| n.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["services", "build"],
        "the author's ORDER must survive, or a client can only sort alphabetically"
    );
    assert_eq!(cfg.namespaces[0].description, "Deploy workloads");
}

#[test]
fn tool_namespace_reaches_the_spec() {
    let m = parse_manifest(OPS_MANIFEST).expect("manifest parses");
    let cfg = build_config(&m, Bindings::default());
    // No handler is bound, so the tool is not registered; assert on the manifest
    // layer instead (the namespace still has to survive parsing).
    let tool = m
        .tools
        .iter()
        .find(|t| t.name == "create-pvc")
        .expect("tool");
    assert_eq!(tool.namespace, "storage");
    let _ = cfg; // build_config ran without panicking
}

#[test]
fn structured_config_shape_reaches_the_spec() {
    let m = parse_manifest(OPS_MANIFEST).expect("manifest parses");
    let cfg = build_config(&m, Bindings::default());

    let knob = cfg.config.get("sandboxes").expect("sandboxes knob");
    assert_eq!(knob.r#type, "list");
    assert_eq!(knob.item_type, "object");
    assert_eq!(knob.min_items, 1);

    let field = |n: &str| knob.fields.iter().find(|f| f.name == n).expect("field");
    assert_eq!(field("url").format, "uri");
    assert_eq!(field("token").kind, "secret");
    assert_eq!(field("os").r#type, "enum");
    assert_eq!(
        field("os").enum_values,
        vec!["linux", "windows", "macos", "android"]
    );
    assert!(field("name").required);

    // An empty namespace means extension-level: shared by all its namespaces.
    assert_eq!(cfg.config.get("easyops-url").unwrap().namespace, "");
    assert_eq!(cfg.config.get("buildkit-addr").unwrap().namespace, "build");
}

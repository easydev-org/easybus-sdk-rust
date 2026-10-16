// Generates the wire messages from proto/easybus/v1/protocol.proto — the single
// source of truth, copied from the Go reference. `buf generate` produces the
// same code; this build script makes a plain `cargo build` self-contained by
// vendoring protoc.
use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    env::set_var("PROTOC", protoc);
    let out = PathBuf::from(env::var("OUT_DIR")?).join("easybus");
    std::fs::create_dir_all(&out)?;
    prost_build::Config::new()
        .out_dir(&out)
        .compile_protos(&["proto/easybus/v1/protocol.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/easybus/v1/protocol.proto");
    Ok(())
}

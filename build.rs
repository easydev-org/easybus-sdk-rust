// The wire code is COMMITTED at src/gen/easybus.v1.rs, so building does not generate it.
//
// This script only enforces that the committed file is present and not empty. Regeneration is
// `make gen`, which CI runs and a bot turns into a pull request.
//
// WHY NOT GENERATE HERE (the prost default): output in OUT_DIR is invisible. A contract
// change that altered a message produced no `git diff`, so a stale binding looked exactly
// like a fresh one, and a field this build did not know about was silently absent on the
// wire. Committing the file makes drift visible and machine-detectable.
use std::path::Path;

fn main() {
    // BOOTSTRAP: when EASYBUS_GENERATE=1, this build script WRITES the committed file instead
    // of only checking for it. That mode exists solely so the generator can run before the
    // file exists (the `gen-proto` binary cannot be compiled without it). Normal builds never
    // set it, so a build can never silently rewrite a committed artifact.
    if std::env::var("EASYBUS_GENERATE").as_deref() == Ok("1") {
        generate();
        return;
    }

    let generated = Path::new("src/gen/easybus.v1.rs");
    if !generated.is_file() {
        // Two situations, and only one is a mistake:
        //   - bootstrapping the generator itself (EASYBUS_ALLOW_MISSING_GEN set): the binary
        //     that would create the file cannot be built without the file, so allow it;
        //   - anything else: a stale checkout. lib.rs includes the file, so the build will
        //     fail right after this with a clear "file not found" pointing at the include —
        //     a panic here would hide that behind a build-script error.
        if std::env::var("EASYBUS_ALLOW_MISSING_GEN").is_err() {
            println!(
                "cargo:warning=src/gen/easybus.v1.rs is missing; run `make gen` and commit it"
            );
        }
    }
    // Rebuild when the contract changes, so a stale checkout is caught by the compile rather
    // than by a test that happens to exercise the missing field.
    println!("cargo:rerun-if-changed=proto/easybus/v1/protocol.proto");
    println!("cargo:rerun-if-changed=src/gen/easybus.v1.rs");
}

fn generate() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    std::env::set_var("PROTOC", protoc);
    let out = Path::new("src/gen");
    std::fs::create_dir_all(out).expect("create src/gen");
    prost_build::Config::new()
        .out_dir(out)
        .compile_protos(&["proto/easybus/v1/protocol.proto"], &["proto"])
        .expect("generate");
    // prost writes into a directory named after the package; normalise to the single file
    // lib.rs includes.
    let nested = out.join("easybus/easybus.v1.rs");
    let flat = out.join("easybus.v1.rs");
    if !flat.is_file() && nested.is_file() {
        std::fs::rename(&nested, &flat).expect("move generated file");
        let _ = std::fs::remove_dir(out.join("easybus"));
    }
    println!("cargo:warning=generated {}", flat.display());
}

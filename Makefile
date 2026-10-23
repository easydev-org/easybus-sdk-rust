.PHONY: gen test

# buf, like the Go, TypeScript and Python SDKs.
#
# It used prost_build from build.rs, which worked but had two costs: `cargo build` rewrote a
# committed artifact (a build must never do that), and prost_build DISCARDS the proto's
# comments, so the generated Rust had no doc comments at all. The BSR prost plugin keeps them
# as `///` docs, which is the difference between a struct a caller can understand and one they
# have to open the proto to read.
gen:
	cd proto && buf generate --template buf.gen.yaml

test:
	cargo test

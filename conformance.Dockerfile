# Runs the conformance suite against a REAL broker.
#
# Same shape as the other three SDKs' images, so a failure means the IMPLEMENTATION
# differs rather than the environment. The broker version matches docker-compose.yml.
#
# Build context is the repository root.
FROM rust:1.90-slim AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
      pkg-config libssl-dev ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Dependency layer first: Cargo.toml changes far less often than src/.
#
# Cargo.lock is OPTIONAL: this crate is a library, and libraries conventionally do
# not commit a lockfile (it is in .gitignore). A `COPY` of a missing file fails the
# build outright, which is how this broke the first time — so the copy is done with a
# shell glob that tolerates its absence, and `cargo` resolves versions when it is gone.
COPY Cargo.tom[l] ./
RUN mkdir -p src && echo 'fn main(){}' > src/main.rs \
 && cargo build --release --tests 2>/dev/null || true

COPY . .

# Build the test binaries, then collect the two that constitute the suite. Copying
# them to fixed paths means the runtime image has no reliance on cargo's hashed
# filenames, which change whenever a dependency does.
RUN cargo test --release --no-run \
 && mkdir -p /out \
 && for t in conformance shared_cases; do \
      bin=$(find target/release/deps -maxdepth 1 -type f -name "$t-*" ! -name '*.d' | head -1); \
      [ -n "$bin" ] || { echo "test binary for $t not found"; exit 1; }; \
      cp "$bin" "/out/$t"; \
    done

FROM debian:trixie-slim

ARG NATS_VERSION=2.15.1
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl \
 && curl -fsSL -o /tmp/nats.tgz \
      "https://github.com/nats-io/nats-server/releases/download/v${NATS_VERSION}/nats-server-v${NATS_VERSION}-linux-amd64.tar.gz" \
 && tar -xzf /tmp/nats.tgz -C /tmp \
 && mv /tmp/nats-server-v${NATS_VERSION}-linux-amd64/nats-server /usr/local/bin/ \
 && rm -rf /tmp/nats.tgz /tmp/nats-server-v${NATS_VERSION}-linux-amd64 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY --from=build /out/conformance /usr/local/bin/conformance.test
COPY --from=build /out/shared_cases /usr/local/bin/shared_cases.test
COPY --from=build /src/conformance-cases /src/conformance-cases

RUN useradd -m -u 10001 runner && chown -R runner /src
USER runner

# Serial: the suite starts a broker per test, and running several at once multiplies
# the process count without testing anything additional.
ENTRYPOINT ["sh", "-c", "/usr/local/bin/shared_cases.test --test-threads=1 && /usr/local/bin/conformance.test --test-threads=1"]

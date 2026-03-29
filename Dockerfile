FROM rust:latest AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the local teckel-rs dependency (injected via additional_contexts)
COPY --from=teckel-rs . ./teckel-rs/

# Copy the API workspace
COPY . ./teckel-api/

# Patch git dependencies → local paths
WORKDIR /build/teckel-api
RUN mkdir -p .cargo && printf '\
[patch."https://github.com/eff3ct0/teckel-rs"]\n\
teckel-model  = { path = "/build/teckel-rs/crates/teckel-model" }\n\
teckel-parser = { path = "/build/teckel-rs/crates/teckel-parser" }\n' \
> .cargo/config.toml

RUN cargo build -p teckel-worker --release

# ── Run ──────────────────────────────────────────────────────
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/teckel-api/target/release/teckel-worker /usr/local/bin/teckel-worker

EXPOSE 50051
ENV TECKEL_HOST=0.0.0.0 TECKEL_PORT=50051 RUST_LOG=info

CMD ["teckel-worker"]

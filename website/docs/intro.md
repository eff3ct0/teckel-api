---
sidebar_position: 1
slug: /intro
---

# Teckel Engine (teckel-api)

**teckel-api** is the execution layer for [Teckel v3.0](https://teckel.rafaelfernandez.dev/docs/intro) pipelines. It depends on [teckel-rs](https://teckel.rafaelfernandez.dev/rs/docs/intro) for parsing YAML into a typed domain model, then executes the pipeline DAG using a pluggable backend system.

## Overview

The engine separates **what** to execute (the pipeline DAG) from **how** to execute it (the backend). This separation enables the same pipeline YAML to run on a local DataFusion engine during development and on a distributed Spark cluster in production -- without changing a single line of pipeline code.

## Crate Map

| Crate | Role |
|---|---|
| `teckel-api` | High-level entry points: `etl()`, `etl_with()`, `explain()`, `validate()` |
| `teckel-engine` | Backend-agnostic DAG executor, `Backend` trait, dry-run explain |
| `teckel-datafusion` | Local backend using Apache DataFusion (Arrow-native) |
| `teckel-polars` | Local backend using Polars (lazy evaluation) |
| `teckel-spark` | Remote backend using Spark Connect (distributed execution) |
| `teckel-remote` | gRPC client backend delegating to remote workers |
| `teckel-server` | Axum HTTP server + gRPC service for the Teckel UI |
| `teckel-worker` | Worker process for remote pipeline execution |

## Quick Start

```bash
cargo build            # Build all crates
cargo test             # Run all tests
```

### Run a Pipeline Programmatically

```rust
use teckel_api;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let yaml = std::fs::read_to_string("pipeline.yaml")?;
    let variables = std::collections::BTreeMap::new();

    // Validate without executing
    teckel_api::validate(&yaml, &variables)?;

    // Generate execution plan
    let plan = teckel_api::explain(&yaml, &variables)?;
    println!("{plan}");

    // Execute with the default DataFusion backend
    teckel_api::etl(&yaml, &variables).await?;

    Ok(())
}
```

### Run with the HTTP Server

```bash
# Start the server
cargo run --bin teckel-server

# Submit a pipeline
curl -X POST http://localhost:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d '{"yaml": "...", "variables": {}}'
```

## Architecture at a Glance

```
                    Pipeline YAML
                         |
                    teckel-parser (from teckel-rs)
                         |
                    Context<Asset>
                         |
                  ┌──────┴──────┐
                  │ teckel-engine│
                  │   PipelineDag│
                  │   Executor   │
                  └──────┬──────┘
                         │
          ┌──────────────┼──────────────┐
          │              │              │
   teckel-datafusion  teckel-polars  teckel-spark
   (local, Arrow)    (local, Polars) (remote, Spark)
```

The engine builds a DAG from the pipeline assets, computes a wave-parallel schedule, and dispatches each operation to the chosen backend. See [Architecture Overview](./architecture/overview.md) for details.

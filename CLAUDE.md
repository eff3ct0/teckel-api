# CLAUDE.md

## Project Overview

**teckel-api** is the execution layer for Teckel v2.0 pipelines. It depends on [teckel-rs](https://github.com/eff3ct0/teckel-rs) for parsing YAML into a typed domain model, then executes the pipeline DAG using a pluggable backend (currently DataFusion).

## Build & Test

```bash
cargo build            # Build all crates
cargo test             # Run all tests
cargo clippy           # Lint
cargo fmt --check      # Check formatting
```

## Architecture

```
teckel-engine  ←  teckel-datafusion
     ↑                   ↑
     └───── teckel-api ──┘
              (also depends on teckel-parser from teckel-rs)
```

### teckel-engine (engine-agnostic)

- `backend.rs`: `Backend` trait with associated type `DataFrame`. Backends implement `read_input`, `write_output`, `apply`.
- `dag.rs`: `PipelineDag` wrapping petgraph. `topological_order()` and `parallel_schedule()` (waves).
- `executor.rs`: `PipelineExecutor<B>` walks DAG, caches computed DataFrames, dispatches to backend.
- `dry_run.rs`: `explain(context)` generates human-readable plan without executing.

### teckel-datafusion (local backend)

- `backend.rs`: `DataFusionBackend` implements `Backend` with `DataFrame = datafusion::DataFrame`.
- `reader.rs`: CSV/Parquet/JSON input with options mapping.
- `writer.rs`: Output with write mode handling (error/overwrite/append/ignore).
- `transforms.rs`: Core transforms (select, where, group, order, join, union, intersect, except, distinct, limit) + addColumns, dropColumns, renameColumns, sql.
- `type_mapping.rs`: `TeckelDataType ↔ arrow::DataType`.

### teckel-api (high-level)

Entry points: `etl()`, `etl_with()`, `explain()`, `validate()`.

## Key Design: Backend trait for future remote execution

The `Backend` trait is shaped so that a future `RemoteBackend` can serialize the pipeline plan and send it to gRPC workers (Spark Connect-style) without changing the API. The associated `DataFrame` type would become a `JobHandle` or similar.

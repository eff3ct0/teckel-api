---
sidebar_position: 1
---

# Architecture Overview

The Teckel Engine is organized into a layered architecture that separates pipeline orchestration from execution. This design enables pluggable backends: the same pipeline definition can run on DataFusion locally or on a Spark cluster remotely.

![Architecture](/img/diagrams/architecture.svg)

## Crate Dependency Graph

```
teckel-model   ──>  teckel-parser        (from teckel-rs)
                         │
                         v
                    teckel-engine          (DAG, executor, Backend trait)
                    /    |    \
                   v     v     v
   teckel-datafusion  teckel-polars  teckel-spark
                   \     |     /
                    v    v    v
                    teckel-api            (high-level entry points)
                         │
                    teckel-server         (HTTP + gRPC)
                         │
                    teckel-worker         (remote execution worker)
                         │
                    teckel-remote         (gRPC client backend)
```

## Design Principles

### 1. Backend Trait for Pluggable Execution

The core abstraction is the `Backend` trait in `teckel-engine`. It defines three operations -- `read_input`, `write_output`, and `apply` -- with an associated `DataFrame` type that is opaque to the engine. Each backend chooses its own representation:

- **DataFusion**: `datafusion::DataFrame` (Arrow-native, in-process)
- **Polars**: `polars::DataFrame` (lazy evaluation, in-process)
- **Spark Connect**: `spark_connect_rs::DataFrame` (remote, distributed)
- **Remote**: `RemoteRef` (opaque handle ID on a gRPC worker)

### 2. Engine-Agnostic DAG Execution

The `PipelineExecutor` in `teckel-engine` walks the DAG without knowing which backend is in use. It receives a `Backend` as a generic parameter and delegates all data operations to it. This means adding a new backend requires only implementing the trait -- no changes to the executor.

### 3. Wave-Parallel Scheduling

The `PipelineDag` computes execution waves: groups of assets that have no mutual dependencies. Assets within a wave are executed concurrently using `tokio::spawn`, while waves are processed sequentially. This achieves maximal parallelism while respecting data dependencies.

### 4. Shared DataFrame Cache

Computed DataFrames are cached in an `Arc<RwLock<BTreeMap<String, B::DataFrame>>>` so that shared dependencies are only computed once. When asset C depends on both A and B, and A and B are in the same wave, C will find both results in the cache when its wave runs.

## Module Layout

### teckel-engine

| File | Purpose |
|---|---|
| `backend.rs` | `Backend` trait with `read_input`, `write_output`, `apply` |
| `dag.rs` | `PipelineDag` using petgraph -- topological sort and wave scheduling |
| `executor.rs` | `PipelineExecutor<B>` -- wave-parallel DAG walker |
| `dry_run.rs` | `explain()` -- generates human-readable plan without executing |

### teckel-api

High-level entry points that wire together the parser and engine:

- `etl(yaml, variables)` -- parse and execute with default DataFusion backend
- `etl_with(yaml, variables, backend)` -- parse and execute with any backend
- `explain(yaml, variables)` -- parse and generate execution plan
- `validate(yaml, variables)` -- parse only, check for errors

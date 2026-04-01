# teckel-api

Execution engine and DataFusion backend for [Teckel v3.0](https://github.com/eff3ct0/teckel-spec) pipelines. Parses YAML pipeline definitions (via [teckel-rs](https://github.com/eff3ct0/teckel-rs)) and executes them using Apache DataFusion.

## Architecture

```
teckel-api (high-level)
  ├── teckel-engine   (Backend trait + DAG executor, engine-agnostic)
  └── teckel-datafusion (DataFusion local backend)
```

The `Backend` trait is designed for evolution: Phase 1 runs DataFusion in-process, but a future `RemoteBackend` can delegate to gRPC workers or K8s pods without changing the API surface.

## Quick Start

```rust
use teckel_api;
use std::collections::BTreeMap;

#[tokio::main]
async fn main() {
    let yaml = std::fs::read_to_string("pipeline.yaml").unwrap();
    teckel_api::etl(&yaml, &BTreeMap::new()).await.unwrap();
}
```

## API

```rust
// Execute with default DataFusion backend
teckel_api::etl(yaml, &vars).await?;

// Execute with a custom backend
teckel_api::etl_with(yaml, &vars, my_backend).await?;

// Get execution plan without running
let plan = teckel_api::explain(yaml, &vars)?;

// Validate only
teckel_api::validate(yaml, &vars)?;
```

## Crates

| Crate | Description |
|-------|-------------|
| `teckel-engine` | `Backend` trait, DAG builder (petgraph), pipeline executor, dry-run formatter |
| `teckel-datafusion` | DataFusion implementation: CSV/Parquet/JSON I/O, core transforms |
| `teckel-api` | High-level API combining parser + engine + backend |

## Build

```bash
cargo build
cargo test
cargo clippy
```

## License

Apache License 2.0

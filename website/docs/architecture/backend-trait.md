---
sidebar_position: 3
---

# Backend Trait

The `Backend` trait is the central abstraction that makes Teckel's execution engine pluggable. Every execution backend -- DataFusion, Polars, Spark Connect, or a custom implementation -- implements this trait.

![Backend trait](/img/diagrams/backend-trait.svg)

## Definition

```rust
#[async_trait]
pub trait Backend: Send + Sync {
    /// The backend's representation of a dataset.
    type DataFrame: Clone + Send + Sync;

    /// Backend identifier (e.g., "datafusion", "polars", "remote").
    fn name(&self) -> &str;

    /// Read an input source into a DataFrame.
    async fn read_input(&self, input: &InputSource) -> Result<Self::DataFrame, TeckelError>;

    /// Write a DataFrame to an output destination.
    async fn write_output(
        &self,
        df: Self::DataFrame,
        output: &OutputSource,
    ) -> Result<(), TeckelError>;

    /// Apply a transformation, given the upstream DataFrames keyed by AssetRef.
    async fn apply(
        &self,
        source: &Source,
        inputs: &BTreeMap<String, Self::DataFrame>,
    ) -> Result<Self::DataFrame, TeckelError>;
}
```

## Associated Type: DataFrame

The `DataFrame` associated type is opaque to the engine. Each backend defines its own:

| Backend | DataFrame Type | Description |
|---|---|---|
| DataFusion | `datafusion::DataFrame` | Lazy logical plan over Arrow arrays |
| Polars | `polars::DataFrame` | Eager columnar DataFrame |
| Spark Connect | `spark_connect_rs::DataFrame` | Handle to a remote Spark DataFrame |
| Remote | `RemoteRef` | Opaque handle ID for a DataFrame on a gRPC worker |

The `Clone + Send + Sync` bounds ensure that DataFrames can be shared across the wave-parallel executor's Tokio tasks and cached in the shared `BTreeMap`.

## Methods

### `name()`

Returns a human-readable identifier for the backend. Used in tracing logs:

```
INFO executing pipeline backend="datafusion" assets=5 waves=3
```

### `read_input()`

Reads an `InputSource` (format + path + options) into the backend's DataFrame type. The backend handles format dispatch:

- **CSV**: Column delimiter, header, quote character, schema inference
- **Parquet**: Predicate pushdown, column projection
- **JSON**: Schema inference, multiline handling

### `write_output()`

Writes a DataFrame to an `OutputSource` with a specified write mode:

| Mode | Behavior |
|---|---|
| `Error` | Fail if the output path already exists |
| `Overwrite` | Replace existing data |
| `Append` | Add to existing data |
| `Ignore` | Do nothing if the output path already exists |

### `apply()`

The core method. Receives a `Source` (the transformation variant) and a `BTreeMap` of all upstream DataFrames keyed by asset name. The backend pattern-matches on the `Source` enum to dispatch to the appropriate transform implementation.

The `Source` enum has 45 variants covering all Teckel v3.0 transformations -- from basic operations like `Select`, `Where`, `GroupBy` to advanced ones like `Window`, `Pivot`, `Scd2`, and `Merge`.

## Design Rationale

### Why an associated type instead of a concrete type?

Using an associated type (`type DataFrame`) instead of a concrete type (e.g., `Arc<dyn Any>`) provides:

1. **Zero-cost abstraction**: No dynamic dispatch or downcasting at runtime.
2. **Type safety**: The compiler verifies that DataFrames produced by one backend method are consumed by the same backend.
3. **Backend-specific optimizations**: Polars can use eager DataFrames, DataFusion can use lazy plans, Spark can use remote handles -- each naturally.

### Why `BTreeMap<String, Self::DataFrame>` in `apply()`?

The full cache is passed to `apply()` rather than just the direct dependencies. This allows complex transforms like `Join` (which needs multiple inputs) and `Sql` (which registers multiple views) to find all their inputs by name, without the engine needing to know the specific dependency structure of each transform variant.

## Implementing a Backend

See the [Custom Backend](../backends/custom-backend.md) guide for a step-by-step walkthrough of implementing the `Backend` trait for your own execution engine.

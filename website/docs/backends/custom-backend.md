---
sidebar_position: 5
---

# Custom Backend

You can implement your own execution backend by implementing the `Backend` trait from `teckel-engine`. This guide walks through the process.

## Step 1: Define Your DataFrame Type

Your backend needs a type to represent datasets. This could be anything that implements `Clone + Send + Sync`:

```rust
#[derive(Clone)]
pub struct MyDataFrame {
    // Your internal representation
    columns: Vec<String>,
    data: Vec<Vec<String>>,
}
```

## Step 2: Implement the Backend Trait

```rust
use async_trait::async_trait;
use std::collections::BTreeMap;
use teckel_engine::Backend;
use teckel_model::source::{InputSource, OutputSource, Source};
use teckel_model::TeckelError;

pub struct MyBackend {
    // Your backend state (connections, configuration, etc.)
}

#[async_trait]
impl Backend for MyBackend {
    type DataFrame = MyDataFrame;

    fn name(&self) -> &str {
        "my-backend"
    }

    async fn read_input(&self, input: &InputSource) -> Result<MyDataFrame, TeckelError> {
        match input.format.as_str() {
            "csv" => {
                // Read CSV file into your DataFrame
                todo!("implement CSV reading")
            }
            "parquet" => {
                todo!("implement Parquet reading")
            }
            "json" => {
                todo!("implement JSON reading")
            }
            other => Err(TeckelError::Execution(
                format!("unsupported format: {other}")
            )),
        }
    }

    async fn write_output(
        &self,
        df: MyDataFrame,
        output: &OutputSource,
    ) -> Result<(), TeckelError> {
        // Handle write modes: Error, Overwrite, Append, Ignore
        match output.mode {
            teckel_model::types::WriteMode::Error => {
                // Check if path exists, fail if so
            }
            teckel_model::types::WriteMode::Overwrite => {
                // Write, replacing existing data
            }
            teckel_model::types::WriteMode::Append => {
                // Append to existing data
            }
            teckel_model::types::WriteMode::Ignore => {
                // Skip if path exists
            }
        }
        todo!("implement writing")
    }

    async fn apply(
        &self,
        source: &Source,
        inputs: &BTreeMap<String, MyDataFrame>,
    ) -> Result<MyDataFrame, TeckelError> {
        match source {
            Source::Select(t) => {
                let df = inputs.get(&t.from).ok_or_else(|| {
                    TeckelError::Execution(format!("input '{}' not found", t.from))
                })?;
                // Implement column selection
                todo!()
            }
            Source::Where(t) => {
                let df = inputs.get(&t.from).ok_or_else(|| {
                    TeckelError::Execution(format!("input '{}' not found", t.from))
                })?;
                // Implement filtering
                todo!()
            }
            // ... handle all Source variants
            _ => Err(TeckelError::Execution(
                format!("transform not supported by {}", self.name())
            )),
        }
    }
}
```

## Step 3: Handle the Source Enum

The `Source` enum has 45 variants. You do not need to implement all of them -- return a descriptive `TeckelError` for unsupported transforms. At minimum, handle:

- `Source::Input` -- handled by `read_input()` (not in `apply()`)
- `Source::Output` -- handled by `write_output()` (not in `apply()`)
- `Source::Select` -- column selection
- `Source::Where` -- row filtering
- `Source::GroupBy` -- aggregation
- `Source::OrderBy` -- sorting
- `Source::Join` -- joining datasets
- `Source::Union` -- combining datasets
- `Source::Sql` -- raw SQL execution (if your backend supports SQL)

The full list of Source variants (all 45) can be found in the `dry_run.rs` source:

Select, Where, GroupBy, OrderBy, Join, Union, Intersect, Except, Distinct, Limit, AddColumns, DropColumns, RenameColumns, CastColumns, Window, Pivot, Unpivot, Flatten, Sample, Conditional, Split, Sql, Rollup, Cube, Scd2, Enrich, SchemaEnforce, Assertion, Repartition, Coalesce, Custom, Offset, Tail, FillNa, DropNa, Replace, Merge, Parse, AsOfJoin, LateralJoin, Transpose, GroupingSets, Describe, Crosstab, Hint.

## Step 4: Use Your Backend

```rust
use teckel_engine::PipelineExecutor;
use teckel_parser;

let context = teckel_parser::parse(&yaml, &variables)?;
let backend = MyBackend::new(/* config */);
let executor = PipelineExecutor::new(backend);
executor.execute(&context).await?;
```

Or use `teckel_api::etl_with()` for the high-level API:

```rust
let backend = MyBackend::new(/* config */);
teckel_api::etl_with(&yaml, &variables, backend).await?;
```

## Tips

- **Start small**: Implement `read_input`, `write_output`, and a few basic transforms first. Return errors for unsupported ones.
- **Use SQL as a fallback**: If your backend has a SQL engine, many transforms can be implemented by registering DataFrames as views and executing SQL.
- **Look at existing backends**: The DataFusion backend (`teckel-datafusion/src/transforms.rs`) is the most straightforward reference. The Polars backend shows how to mix native API with SQL. The Spark backend shows how to work with remote DataFrames.
- **Test incrementally**: Run simple pipelines (input -> filter -> output) before tackling complex transforms like Window or Scd2.

---
sidebar_position: 2
---

# Dry Run and Explain

The `explain()` function generates a human-readable execution plan for a pipeline without executing anything. This is useful for debugging pipeline structure, understanding execution order, and validating that your DAG is correct.

## Using Explain

### Rust API

```rust
use teckel_api;
use std::collections::BTreeMap;

let yaml = std::fs::read_to_string("pipeline.yaml")?;
let variables = BTreeMap::new();

let plan = teckel_api::explain(&yaml, &variables)?;
println!("{plan}");
```

### HTTP Server

```bash
curl -X POST http://localhost:8080/api/explain \
  -H "Content-Type: application/json" \
  -d '{"yaml": "...", "variables": {}}'
```

### gRPC

```
rpc ExplainPipeline(PipelineRequest) returns (ExplainResponse)
```

## Plan Output

The explain plan contains four sections:

### 1. Inputs

Lists all input assets with their format and path:

```
--- Inputs ---
  [raw_customers] format=csv, path=data/customers.csv
  [raw_orders] format=parquet, path=data/orders.parquet
```

### 2. Transformations

Lists all transform assets with their operation type and dependencies:

```
--- Transformations ---
  [active_customers] WHERE (from: raw_customers)
  [enriched] JOIN (from: active_customers, raw_orders)
  [summary] GROUP BY (from: enriched)
  [sorted] ORDER BY (from: summary)
```

The operation name is derived from the `Source` variant:

| Source Variant | Operation Name |
|---|---|
| `Select` | SELECT |
| `Where` | WHERE |
| `GroupBy` | GROUP BY |
| `OrderBy` | ORDER BY |
| `Join` | JOIN |
| `Union` | UNION |
| `Intersect` | INTERSECT |
| `Except` | EXCEPT |
| `Distinct` | DISTINCT |
| `Limit` | LIMIT |
| `AddColumns` | ADD COLUMNS |
| `DropColumns` | DROP COLUMNS |
| `RenameColumns` | RENAME COLUMNS |
| `CastColumns` | CAST COLUMNS |
| `Window` | WINDOW |
| `Pivot` | PIVOT |
| `Unpivot` | UNPIVOT |
| `Flatten` | FLATTEN |
| `Sample` | SAMPLE |
| `Conditional` | CONDITIONAL |
| `Sql` | SQL |
| `Rollup` | ROLLUP |
| `Cube` | CUBE |
| `Scd2` | SCD2 |
| `Merge` | MERGE |
| `FillNa` | FILL NA |
| `DropNa` | DROP NA |
| `Replace` | REPLACE |
| `Offset` | OFFSET |
| `Tail` | TAIL |
| ... | (and more) |

### 3. Outputs

Lists all output assets with their source, format, write mode, and path:

```
--- Outputs ---
  [customer_report] from=sorted, format=parquet, mode=Overwrite, path=output/report.parquet
```

### 4. Execution Schedule

Shows the total counts and the wave-parallel schedule:

```
Total: 2 inputs, 4 transformations, 1 outputs
Execution waves: 5
  Wave 0: raw_customers, raw_orders
  Wave 1: active_customers
  Wave 2: enriched
  Wave 3: summary
  Wave 4: sorted, customer_report
```

## Understanding Waves

Waves represent groups of assets that can execute concurrently:

- **Wave 0** contains all independent inputs -- they have no dependencies and can be read in parallel.
- **Subsequent waves** contain assets whose dependencies are all in previous waves.
- Assets in the same wave are executed concurrently via `tokio::spawn`.
- Waves are executed sequentially -- all assets in Wave N must complete before Wave N+1 starts.

### Diamond DAG Example

For a pipeline with shape `a -> b, a -> c, b+c -> d`:

```
Wave 0: [a]        (input, no deps)
Wave 1: [b, c]     (both depend only on a, run in parallel)
Wave 2: [d]        (depends on b and c)
```

### Linear Pipeline

For a pipeline with shape `input -> filter -> select -> output`:

```
Wave 0: [input]
Wave 1: [filter]
Wave 2: [select]
Wave 3: [output]
```

Each wave has one asset, so there is no parallelism -- but the executor optimizes this by skipping `tokio::spawn` for single-asset waves.

## How It Works

Internally, `explain()` does the following:

1. Parses the YAML into a `Context<Asset>` using `teckel-parser`.
2. Builds a `PipelineDag` from the context using `PipelineDag::from_context()`.
3. Computes topological order with `dag.topological_order()`.
4. Computes parallel waves with `dag.parallel_schedule()`.
5. Formats the results as a human-readable string.

No backend is involved -- explain works purely on the pipeline structure.

## Use Cases

- **CI/CD validation**: Run `explain()` in your pipeline tests to verify the DAG structure before deploying.
- **Debugging**: Understand why assets execute in a particular order or why parallelism is limited.
- **Documentation**: Generate execution plans to include in pipeline documentation.
- **UI preview**: The Teckel UI calls the ExplainPipeline gRPC endpoint to show users what will happen before they click "Run".

---
sidebar_position: 2
---

# DataFusion Backend

The DataFusion backend executes Teckel pipelines locally using [Apache DataFusion](https://datafusion.apache.org/), an extensible query engine written in Rust that uses Apache Arrow as its in-memory format.

## Architecture

```
teckel-datafusion
├── backend.rs       DataFusionBackend implements Backend<DataFrame = datafusion::DataFrame>
├── reader.rs        CSV / Parquet / JSON input handling
├── writer.rs        Output with write mode support
├── transforms.rs    All 45 transform implementations
└── type_mapping.rs  TeckelDataType <-> arrow::DataType conversion
```

### DataFusionBackend

The `DataFusionBackend` wraps a DataFusion `SessionContext` and implements the `Backend` trait with `DataFrame = datafusion::DataFrame`. The SessionContext provides SQL execution, function registry, and table registration.

## Transform Implementation

DataFusion transforms use a mix of native DataFrame API calls and SQL. The approach depends on the transform:

### Native API Transforms

These transforms map directly to DataFusion's DataFrame methods:

| Transform | Implementation |
|---|---|
| **Select** | `df.select(cols)` -- columns parsed as expressions with schema |
| **Where** | `df.filter(expr)` -- expression parsed with schema context |
| **GroupBy** | `df.aggregate(group_exprs, agg_exprs)` |
| **OrderBy** | `df.sort(sort_exprs)` with `SortExpr` for direction and null ordering |
| **Union** | `df.union(other)` chained, then `df.distinct()` if not `UNION ALL` |
| **Intersect** | `df.intersect(other)` chained across sources |
| **Except** | `df.except(other)` |
| **Distinct** | `df.distinct()` |
| **Limit** | `df.limit(0, Some(count))` |
| **AddColumns** | `df.with_column(name, expr)` iterating over column definitions |
| **DropColumns** | `df.drop_columns(&cols)` |
| **RenameColumns** | `df.with_column_renamed(old, new)` iterating over mappings |

### SQL-Based Transforms

Complex transforms register the input DataFrame as a temporary view and execute SQL:

| Transform | SQL Pattern |
|---|---|
| **Sql** | Register views, execute `ctx.sql(&query)` directly |
| **CastColumns** | `SELECT CAST("col" AS type) AS "col", ... FROM view` |
| **Window** | `SELECT *, func() OVER (PARTITION BY ... ORDER BY ... frame) AS alias FROM view` |
| **Pivot** | Conditional aggregation: `agg FILTER (WHERE pivot_col = 'val') AS "val"` |
| **Unpivot** | `UNION ALL` of `SELECT ids, 'col' AS var, "col" AS val` per value column |
| **Flatten** | Recursive struct field projection with configurable separator |
| **Rollup** | `SELECT ... GROUP BY ROLLUP(cols)` |
| **Cube** | `SELECT ... GROUP BY CUBE(cols)` |
| **Scd2** | Complex multi-part UNION ALL with current/incoming dimension logic |
| **Conditional** | `CASE WHEN cond THEN val ... END` as expression |
| **Sample** | `SELECT * FROM view WHERE random() < fraction` |

### Join Implementation

Joins use the native DataFusion join API with filter expressions. The join condition is parsed against a combined schema of left and right DataFrames:

```rust
let combined = left_schema.join(right_schema)?;
let filter_expr = parse_expr_with_schema(ctx, &filter, &combined)?;
result = result.join(right_df, join_type, &[], &[], Some(filter_expr))?;
```

Join type mapping:

| Teckel Type | DataFusion Type |
|---|---|
| `Inner` | `JoinType::Inner` |
| `Left` | `JoinType::Left` |
| `Right` | `JoinType::Right` |
| `Outer` | `JoinType::Full` |
| `Cross` | `JoinType::Inner` with empty columns |
| `LeftSemi` | `JoinType::LeftSemi` |
| `LeftAnti` | `JoinType::LeftAnti` |

Cross joins use `JoinType::Inner` with no columns and no filter to produce the Cartesian product.

## Type Mapping

The `type_mapping.rs` module provides bidirectional conversion between Teckel data types and Arrow data types:

### `teckel_to_arrow()`

Converts Teckel's `TeckelDataType` enum to Arrow's `DataType`:

| Teckel Type | Arrow Type |
|---|---|
| `String` | `DataType::Utf8` |
| `Integer` | `DataType::Int64` |
| `Long` | `DataType::Int64` |
| `Float` | `DataType::Float32` |
| `Double` | `DataType::Float64` |
| `Boolean` | `DataType::Boolean` |
| `Date` | `DataType::Date32` |
| `Timestamp` | `DataType::Timestamp(Nanosecond, None)` |
| `Binary` | `DataType::Binary` |
| `Decimal(p, s)` | `DataType::Decimal128(p, s)` |

### `arrow_to_teckel()`

Reverse mapping used when inferring schema from existing DataFrames.

## Reader

The `reader.rs` module handles input source reading based on format:

| Format | Method | Options |
|---|---|---|
| CSV | `ctx.read_csv(path, options)` | `header`, `delimiter`, `quote`, `escape`, `inferSchema` |
| Parquet | `ctx.read_parquet(path, options)` | Column projection, predicate pushdown |
| JSON | `ctx.read_json(path, options)` | Schema inference, multiline |

## Writer

The `writer.rs` module handles output writing with write mode support:

- **Error**: Check if path exists; fail if it does
- **Overwrite**: Write directly (DataFusion creates or replaces)
- **Append**: Read existing data, union with new data, write combined
- **Ignore**: Check if path exists; skip if it does

Output formats: CSV, Parquet, JSON.

## Limitations

| Feature | Status | Notes |
|---|---|---|
| **Merge** | Not supported | Requires mutable table format (Delta Lake) |
| **Parse CSV** | Partial | Basic CSV parsing supported, complex formats may not work |
| **Repartition** | No-op | Single-machine execution, no shuffle needed |
| **Coalesce** | No-op | Single-machine execution |

## Usage

```rust
use teckel_datafusion::DataFusionBackend;
use teckel_engine::PipelineExecutor;
use teckel_parser;

let context = teckel_parser::parse(&yaml, &variables)?;
let backend = DataFusionBackend::new();
let executor = PipelineExecutor::new(backend);
executor.execute(&context).await?;
```

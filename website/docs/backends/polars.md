---
sidebar_position: 3
---

# Polars Backend

The Polars backend executes Teckel pipelines using [Polars](https://pola.rs/), a high-performance DataFrame library written in Rust with a focus on lazy evaluation and memory efficiency.

![Polars flow](/img/diagrams/polars-flow.svg)

## Architecture

### PolarsBackend

The `PolarsBackend` implements the `Backend` trait with `DataFrame = polars::DataFrame`. Unlike DataFusion which uses lazy logical plans by default, Polars uses eager DataFrames as the caching type -- but individual transforms leverage lazy evaluation internally via `.lazy()` / `.collect()`.

## Execution Model

Polars transforms follow a common pattern:

1. Get the input DataFrame from the cache.
2. Convert to a `LazyFrame` with `.lazy()`.
3. Apply the transformation.
4. Collect back to an eager `DataFrame` with `.collect()`.

This gives the Polars query optimizer a chance to optimize each transform while keeping the cache simple (eager DataFrames).

For complex transforms that cannot be expressed with the Polars expression API, a `polars::sql::SQLContext` is used to execute SQL queries.

## Transform Implementation

### Polars-Native Transforms

These use the Polars expression API directly:

| Transform | Implementation |
|---|---|
| **Select** | `df.lazy().select(exprs).collect()` |
| **Where** | `df.lazy().filter(expr).collect()` |
| **GroupBy** | `df.lazy().group_by(exprs).agg(exprs).collect()` |
| **OrderBy** | `df.lazy().sort_by_exprs(cols, opts).collect()` with `SortMultipleOptions` |
| **Distinct** | `df.unique_stable(None, UniqueKeepStrategy::First, None)` |
| **Limit** | `df.head(Some(count))` |
| **AddColumns** | `lf.with_column(expr.alias(name))` per column |
| **DropColumns** | `df.drop_many(&cols)` |
| **RenameColumns** | `df.rename(old, new)` per mapping |
| **CastColumns** | `lf.with_column(col(name).cast(dtype))` per column |
| **Sample** | `df.sample_n_literal(n, with_replacement, false, seed)` |

### SQL-Based Transforms

These use Polars' `SQLContext` for complex operations:

| Transform | SQL Pattern |
|---|---|
| **Join** | `SELECT * FROM __left {JOIN_TYPE} __right ON condition` |
| **Intersect** | `SELECT * FROM t0 INTERSECT SELECT * FROM t1` |
| **Except** | `SELECT * FROM __left EXCEPT SELECT * FROM __right` |
| **Sql** | Register views, execute user's SQL query |
| **Window** | `SELECT *, func() OVER (PARTITION BY ... ORDER BY ...) AS alias FROM __src` |
| **Pivot** | Conditional aggregation via SQL |
| **Unpivot** | `UNION ALL` of per-value-column SELECTs |
| **Rollup** | `SELECT ... GROUP BY ROLLUP(cols)` |
| **Cube** | `SELECT ... GROUP BY CUBE(cols)` |
| **Conditional** | `SELECT *, CASE WHEN ... END AS col FROM __src` |
| **Scd2** | Complex multi-part SQL with current/incoming dimension logic |

### Union Implementation

Union uses Polars' `concat()` function to stack multiple LazyFrames:

```rust
let dfs: Vec<LazyFrame> = sources.iter()
    .map(|s| get(cache, s).map(|df| df.lazy()))
    .collect::<Result<Vec<_>, _>>()?;
let mut result = concat(dfs, UnionArgs::default())?;
if !t.all {
    result = result.unique(None, UniqueKeepStrategy::First);
}
result.collect()
```

### Join Implementation

Joins in Polars use SQL via `SQLContext` because Polars' native join API does not directly support arbitrary join conditions (ON clauses with expressions). The left and right DataFrames are registered as temporary views and a SQL join query is constructed:

```rust
let mut ctx = polars::sql::SQLContext::new();
ctx.register("__left", result_df.lazy());
ctx.register("__right", right_df.lazy());
let query = format!("SELECT * FROM __left {jt} __right ON {on_clause}");
result = ctx.execute(&query)?;
```

Cross joins use the native `result.cross_join(right, None)` API.

### SchemaEnforce

The Polars backend supports schema enforcement by selecting and casting columns:

```rust
let select_exprs: Vec<Expr> = t.columns.iter()
    .map(|c| col(c.name.as_str()).cast(parse_polars_dtype(&c.data_type)))
    .collect();
df.lazy().select(select_exprs).collect()
```

### No-Op Transforms

Since Polars runs in-process on a single machine, these transforms are no-ops that simply return the input DataFrame:

- **Repartition** -- no concept of partitions in local execution
- **Coalesce** -- no concept of partitions in local execution

## Type Handling

Polars has its own type system separate from Arrow. The `parse_polars_dtype()` function maps Teckel type strings to Polars `DataType`:

| Teckel Type | Polars Type |
|---|---|
| `string` | `DataType::String` |
| `integer`, `int` | `DataType::Int32` |
| `long`, `bigint` | `DataType::Int64` |
| `float` | `DataType::Float32` |
| `double` | `DataType::Float64` |
| `boolean` | `DataType::Boolean` |
| `date` | `DataType::Date` |
| `timestamp` | `DataType::Datetime(Microseconds, None)` |

## Limitations

| Feature | Status | Notes |
|---|---|---|
| **Merge** | Not supported | Requires mutable table format |
| **Parse** | Not supported | CSV/JSON parsing within transforms not implemented |
| **Flatten** | Not supported | Struct flattening not available in Polars SQL |
| **Repartition** | No-op | Single-machine, no partitions |
| **Coalesce** | No-op | Single-machine, no partitions |

## Usage

```rust
use teckel_polars::PolarsBackend;
use teckel_engine::PipelineExecutor;
use teckel_parser;

let context = teckel_parser::parse(&yaml, &variables)?;
let backend = PolarsBackend::new();
let executor = PipelineExecutor::new(backend);
executor.execute(&context).await?;
```

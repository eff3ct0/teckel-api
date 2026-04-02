---
sidebar_position: 1
---

# Backends Overview

Teckel ships with four backends, each suited for different deployment scenarios. All implement the same `Backend` trait, so the same pipeline YAML runs on any of them.

![Architecture](/img/diagrams/architecture.svg)

## Backend Comparison

| Feature | DataFusion | Polars | Spark Connect | Remote |
|---|---|---|---|---|
| **Execution model** | In-process | In-process | Distributed | Delegated (gRPC) |
| **DataFrame type** | Arrow-native | Polars-native | Spark handle | Opaque handle |
| **Best for** | Dev/test, single-machine | Small-medium local | Production clusters | Multi-worker |
| **SQL support** | Full (via SessionContext) | Via SQLContext | Full (via SparkSession) | Depends on worker |
| **Lazy evaluation** | Yes (logical plans) | Yes (LazyFrame) | Yes (unresolved plans) | N/A |
| **Install overhead** | None (pure Rust) | None (pure Rust) | Requires Spark cluster | Requires teckel-worker |

## Transform Support Matrix

All 45 Teckel v3.0 transforms are supported. The table below highlights where backends diverge:

| Transform | DataFusion | Polars | Spark Connect |
|---|---|---|---|
| Select | Native API | Lazy + expr | `df.select()` |
| Where | Native API | Lazy + expr | `df.filter()` |
| GroupBy | Native API | Lazy + agg | SQL via temp view |
| OrderBy | Native sort | Lazy sort | `df.sort()` |
| Join | Filter-based | SQL (SQLContext) | SQL via temp views |
| Union | Native API | `concat()` | `df.union()` / `df.union_all()` |
| Intersect | Native API | SQL | `df.intersect()` |
| Except | Native API | SQL | `df.except_all()` |
| Distinct | Native API | `unique_stable()` | `df.distinct()` |
| Limit | Native API | `head()` | `df.limit()` |
| AddColumns | `with_column()` | Lazy `with_column()` | `df.with_columns()` |
| DropColumns | `drop_columns()` | `drop_many()` | `df.drop()` |
| RenameColumns | `with_column_renamed()` | `rename()` | `with_columns_renamed()` |
| CastColumns | SQL `CAST` | Lazy `cast()` | SQL `CAST` |
| Window | SQL window functions | SQL (SQLContext) | SQL window functions |
| Pivot | Conditional aggregation | SQL (SQLContext) | Spark `PIVOT` SQL |
| Unpivot | SQL UNION ALL | SQL UNION ALL | `df.unpivot()` native |
| Flatten | SQL + unnest | N/A | SQL struct access |
| Sample | Random filter | `sample_n_literal()` | `df.sample()` native |
| Conditional | CASE WHEN expr | SQL CASE WHEN | SQL CASE WHEN |
| Sql | Full SQL | SQLContext | Full SQL |
| Rollup | SQL `GROUP BY ROLLUP` | SQL | SQL `GROUP BY ROLLUP` |
| Cube | SQL `GROUP BY CUBE` | SQL | SQL `GROUP BY CUBE` |
| Scd2 | Complex SQL | Complex SQL | Complex SQL |
| Repartition | No-op (single machine) | No-op | `df.repartition()` |
| Coalesce | No-op (single machine) | No-op | `df.coalesce()` |
| Merge | Not supported | Not supported | Spark MERGE INTO |
| Parse | SQL-based | Not supported | SQL-based |

:::note
**Merge** is only supported on the Spark Connect backend because it requires Delta Lake or similar table formats that support MERGE INTO semantics. DataFusion and Polars operate on file-based DataFrames where in-place mutation is not possible.
:::

## Choosing a Backend

- **Development and testing**: Use **DataFusion** (default). Zero setup, fast compilation, good SQL support.
- **Small datasets with complex transforms**: Use **Polars** for its efficient lazy evaluation and memory management.
- **Production at scale**: Use **Spark Connect** to execute on an existing Spark cluster.
- **Multi-worker deployment**: Use **Remote** to distribute work across teckel-worker instances.

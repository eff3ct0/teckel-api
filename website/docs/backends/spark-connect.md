---
sidebar_position: 4
---

# Spark Connect Backend

The Spark Connect backend is the most fully-featured backend in Teckel. It executes pipelines on a remote Apache Spark cluster via the [Spark Connect protocol](https://spark.apache.org/docs/latest/spark-connect-overview.html), enabling distributed processing of large-scale datasets.

## What is Spark Connect?

Spark Connect is a protocol introduced in Apache Spark 3.4 that decouples the Spark client from the Spark cluster. Instead of running within the JVM, clients communicate with a Spark Connect server via gRPC, sending logical plans and receiving results via Arrow Flight.

```
┌─────────────────┐       gRPC        ┌──────────────────────────┐
│  teckel-spark   │  ─────────────>   │  Spark Connect Server    │
│  (Rust client)  │  <─────────────   │  (JVM, on Spark cluster) │
│                 │   Arrow Flight    │                          │
└─────────────────┘                   └──────────────────────────┘
```

This architecture means Teckel can submit pipelines to Spark without any JVM dependency -- the entire client is pure Rust.

## The Rust Implementation

Teckel uses [spark-connect-rs](https://github.com/rafafrdz/spark-connect-rust), a Rust implementation of the Spark Connect client. This library provides:

- `SparkSession`: The main entry point for interacting with Spark
- `DataFrame`: A handle representing a Spark DataFrame (unresolved plan)
- `Column` and `expr()` functions for building expressions
- SQL execution via `session.sql(&query)`
- Temp view management via `df.create_or_replace_temp_view(name)`

## Architecture

```
teckel-spark
├── backend.rs       SparkConnectBackend implements Backend<DataFrame = spark_connect_rs::DataFrame>
├── reader.rs        CSV / Parquet / JSON input via Spark
├── writer.rs        Output via Spark's write API
└── transforms.rs    All 45 transforms (most comprehensive backend)
```

### SparkConnectBackend

```rust
pub struct SparkConnectBackend {
    session: SparkSession,
}
```

The backend wraps a `SparkSession` connected to a Spark Connect server. All operations are sent as gRPC requests to the server, which executes them on the Spark cluster.

### Connection

```rust
let backend = SparkConnectBackend::new("sc://127.0.0.1:15002/").await?;
```

The connection URL uses the `sc://` scheme (Spark Connect). Configuration options:

- **App name**: `"teckel"` (shown in Spark UI)
- **Connect timeout**: 30 seconds
- **Request timeout**: 300 seconds (5 minutes, for long-running transforms)

You can also create a backend from an existing session:

```rust
let session = SparkSessionBuilder::remote("sc://spark-master:15002/")
    .app_name("my-pipeline")
    .build()
    .await?;
let backend = SparkConnectBackend::with_session(session);
```

## Transform Implementation

The Spark backend implements all 45 Teckel v3.0 transforms -- the most complete implementation of any backend. Many transforms use a hybrid approach: the Spark Connect DataFrame API for simple operations, and SQL via temp views for complex ones.

### DataFrame API Transforms

| Transform | Implementation |
|---|---|
| **Select** | `df.select(cols)` using `expr()` to parse column expressions |
| **Where** | `df.filter(condition)` with string-based filter |
| **OrderBy** | `df.sort(cols)` with `col.asc_nulls_first()`, `.desc_nulls_last()`, etc. |
| **Union** | `df.union_all(other)` for UNION ALL, `df.union(other)` for UNION DISTINCT |
| **Intersect** | `df.intersect(other)` chained |
| **Except** | `df.except_all(other)` |
| **Distinct** | `df.distinct()` |
| **Limit** | `df.limit(count)` |
| **AddColumns** | `df.with_columns([(name, expr)])` |
| **DropColumns** | `df.drop(cols)` |
| **RenameColumns** | `df.with_columns_renamed(mappings)` |
| **Unpivot** | `df.unpivot(ids, values, var_col, val_col)` -- native API |
| **Sample** | `df.sample(lower, upper, with_replacement, seed)` |
| **Coalesce** | `df.coalesce(num_partitions)` |
| **Repartition** | `df.repartition(num_partitions, None)` |

### SQL-Based Transforms

For transforms that require complex logic, the Spark backend registers DataFrames as temp views and executes SQL:

| Transform | SQL Pattern |
|---|---|
| **GroupBy** | `SELECT group_cols, agg_exprs FROM view GROUP BY group_cols` |
| **Join** | Multi-table SQL with `INNER JOIN`, `LEFT JOIN`, etc. |
| **CastColumns** | `SELECT CAST(\`col\` AS type) AS \`col\` FROM view` |
| **Window** | `SELECT *, func() OVER (PARTITION BY ... ORDER BY ... frame) AS alias FROM view` |
| **Pivot** | Spark native PIVOT syntax: `SELECT * FROM (...) PIVOT (agg FOR col IN (values))` |
| **Sql** | Register views, execute user's query via `session.sql()` |
| **Rollup** | `SELECT ... GROUP BY ROLLUP(cols)` |
| **Cube** | `SELECT ... GROUP BY CUBE(cols)` |
| **Offset** | `SELECT * FROM view OFFSET count` |
| **Tail** | Count total rows, then `LIMIT n OFFSET (total - n)` |
| **FillNa** | `SELECT COALESCE(\`col\`, fill_value) AS \`col\` FROM view` |
| **DropNa** | `SELECT * FROM view WHERE conditions` based on how (any/all) and threshold |
| **Replace** | `SELECT CASE WHEN ... END AS \`col\` FROM view` per replacement mapping |
| **Scd2** | Complex multi-part SQL for slowly changing dimensions |

### Join Implementation

The Spark backend uses SQL for all joins because spark-connect-rs's join API differs from Teckel's condition model. DataFrames are registered as temp views and a SQL query is built dynamically:

```rust
let mut query = "SELECT * FROM __teckel_join_left".to_string();
for (i, target) in t.right.iter().enumerate() {
    let join_type = match target.join_type {
        JoinType::Inner => "INNER JOIN",
        JoinType::Left => "LEFT JOIN",
        JoinType::Right => "RIGHT JOIN",
        JoinType::Outer => "FULL OUTER JOIN",
        JoinType::Cross => "CROSS JOIN",
        JoinType::LeftSemi => "LEFT SEMI JOIN",
        JoinType::LeftAnti => "LEFT ANTI JOIN",
    };
    let condition = target.on.join(" AND ");
    query.push_str(&format!(" {join_type} __teckel_join_right_{i} ON {condition}"));
}
session.sql(&query).await
```

This supports multi-way joins in a single transform: the query chains as many JOIN clauses as needed.

### Merge Transform (Spark-Only)

The **Merge** transform is only available on the Spark Connect backend. It generates a Spark SQL `MERGE INTO` statement, which requires a table format that supports updates (e.g., Delta Lake, Apache Iceberg):

```sql
MERGE INTO __teckel_merge_target t
USING __teckel_merge_source s
ON condition
WHEN MATCHED [AND condition] THEN UPDATE SET ...
WHEN MATCHED [AND condition] THEN DELETE
WHEN NOT MATCHED [AND condition] THEN INSERT ...
```

The implementation handles:

- Multiple `WHEN MATCHED` clauses with optional conditions
- Star expressions (`UPDATE SET *`, `INSERT *`)
- Explicit column assignments (`UPDATE SET t.col = expr`)
- `WHEN NOT MATCHED` for insert operations

### GroupBy via SQL

Unlike the other backends that use the native GroupBy API, Spark uses SQL because spark-connect-rs's `GroupedData` returns `RecordBatch` rather than a `DataFrame`, which would break the caching model:

```rust
let view = "__teckel_groupby_src";
df.create_or_replace_temp_view(view).await?;
let query = format!("SELECT {group_cols}, {agg_exprs} FROM {view} GROUP BY {group_cols}");
session.sql(&query).await
```

## Session Management

Each `SparkConnectBackend` holds a `SparkSession` that represents a connection to the Spark Connect server. Sessions are:

- **Isolated**: Each session has its own temp views and configurations
- **Stateful**: Temp views registered in one transform are visible to later transforms
- **Long-lived**: The session persists for the lifetime of the backend

### Configuration

```rust
let session = SparkSessionBuilder::remote("sc://spark-master:15002/")
    .app_name("teckel")
    .connect_timeout(Duration::from_secs(30))
    .request_timeout(Duration::from_secs(300))
    .build()
    .await?;
```

## Comparison with JVM Spark Connect Client

| Feature | Rust (spark-connect-rs) | JVM (Spark official) |
|---|---|---|
| Language | Pure Rust | Java/Scala |
| Binary size | ~10 MB | ~200 MB (with Spark jars) |
| Startup time | Milliseconds | Seconds |
| Memory footprint | Low | High (JVM) |
| DataFrame API | Partial (growing) | Complete |
| SQL support | Full (via server) | Full |
| Arrow Flight | Yes | Yes |
| Cluster features | Read/write/transform | Full (including streaming) |

The Rust client trades some API completeness for dramatically lower resource usage -- which is why Teckel supplements the DataFrame API with SQL for transforms that the Rust client does not expose natively.

## Production Deployment

### Connecting to a Real Cluster

```rust
// Standalone Spark cluster
let backend = SparkConnectBackend::new("sc://spark-master:15002/").await?;

// Kubernetes
let backend = SparkConnectBackend::new("sc://spark-connect-service.spark:15002/").await?;

// Databricks (with token)
let session = SparkSessionBuilder::remote("sc://your-workspace.databricks.com:443/")
    .token("your-token")
    .build()
    .await?;
let backend = SparkConnectBackend::with_session(session);
```

### Resource Allocation

Spark resource allocation is controlled on the cluster side. When Teckel connects via Spark Connect, it uses whatever resources the Spark Connect server has been configured with. Adjust these in your Spark configuration:

- `spark.executor.instances` -- number of executors
- `spark.executor.memory` -- memory per executor
- `spark.executor.cores` -- CPU cores per executor

## All 45 Transforms Supported

The Spark Connect backend supports every Teckel v3.0 transform:

Select, Where, GroupBy, OrderBy, Join, Union, Intersect, Except, Distinct, Limit, AddColumns, DropColumns, RenameColumns, CastColumns, Window, Pivot, Unpivot, Flatten, Sample, Conditional, Split, Sql, Rollup, Cube, Scd2, Enrich, SchemaEnforce, Assertion, Repartition, Coalesce, Custom, Offset, Tail, FillNa, DropNa, Replace, Merge, Parse, AsOfJoin, LateralJoin, Transpose, GroupingSets, Describe, Crosstab, Hint.

This makes it the recommended backend for production pipelines that need full Teckel v3.0 compatibility.

## Usage

```rust
use teckel_spark::SparkConnectBackend;
use teckel_engine::PipelineExecutor;
use teckel_parser;

let context = teckel_parser::parse(&yaml, &variables)?;
let backend = SparkConnectBackend::new("sc://127.0.0.1:15002/").await?;
let executor = PipelineExecutor::new(backend);
executor.execute(&context).await?;
```

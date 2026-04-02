---
sidebar_position: 2
---

# DAG Executor

The execution engine converts a flat asset context into a directed acyclic graph (DAG), computes an optimal parallel schedule, and executes it using Tokio tasks.

## PipelineDag

The `PipelineDag` struct in `teckel-engine/src/dag.rs` wraps a petgraph `DiGraph<String, ()>` where:

- **Nodes** are asset names (strings)
- **Edges** represent data dependencies (an edge from A to B means B depends on A)

### Building the DAG

```rust
let dag = PipelineDag::from_context(context)?;
```

`from_context()` iterates all assets in the `Context`, adds each as a node, then calls `asset.source.dependencies()` to discover edges. If asset "filtered" has `from: "raw_data"`, an edge from "raw_data" to "filtered" is added.

### Topological Order

```rust
let order: Vec<String> = dag.topological_order()?;
```

Uses petgraph's `toposort` to produce a valid execution order. If a cycle is detected, it returns `TeckelError::spec(ECycle001, ...)` with the offending asset name.

### Parallel Schedule (Waves)

```rust
let waves: Vec<Vec<String>> = dag.parallel_schedule()?;
```

The wave scheduler assigns each asset to the earliest possible wave:

1. Start with topological order.
2. For each asset, find the maximum wave index of its dependencies.
3. Place the asset in wave `max_dep_wave + 1` (or wave 0 if it has no dependencies).

This produces groups of independent assets that can execute concurrently.

**Example**: For a diamond DAG `a -> b, a -> c, b+c -> d`:

```
Wave 0: [a]           (input, no dependencies)
Wave 1: [b, c]        (both depend only on a)
Wave 2: [d]           (depends on b and c)
```

## PipelineExecutor

The `PipelineExecutor<B: Backend>` in `teckel-engine/src/executor.rs` is the runtime that walks the DAG:

```rust
pub struct PipelineExecutor<B: Backend + 'static> {
    backend: Arc<B>,
}
```

### Execution Flow

1. **Build DAG**: `PipelineDag::from_context(context)`
2. **Compute waves**: `dag.parallel_schedule()`
3. **For each wave**:
   - If the wave has a single asset, execute it directly (no spawn overhead).
   - If the wave has multiple assets, spawn each into a `tokio::task::JoinSet`.
   - Wait for all tasks in the wave. Fail fast on the first error and abort remaining tasks.
4. **Move to the next wave** only after all tasks in the current wave complete.

### Single-Asset Optimization

When a wave contains only one asset, the executor calls `execute_asset()` directly on the current task instead of spawning. This avoids the overhead of `tokio::spawn` for sequential portions of the pipeline.

### Concurrent Execution with JoinSet

For multi-asset waves, the executor creates a `JoinSet` and spawns one task per asset:

```rust
let mut join_set = JoinSet::new();
for asset_name in wave {
    let name = asset_name.clone();
    let backend = Arc::clone(&self.backend);
    let cache = Arc::clone(&cache);
    let context = context.clone();
    join_set.spawn(async move {
        execute_asset_inner(&name, &context, backend.as_ref(), &cache).await
    });
}
```

### DataFrame Cache

The cache is an `Arc<RwLock<BTreeMap<String, B::DataFrame>>>` shared across all tasks:

- **Input assets**: Read from source, store the resulting DataFrame in the cache.
- **Output assets**: Retrieve the source DataFrame from the cache, write to destination.
- **Transform assets**: Clone the entire cache (read lock), apply the transform, store the result (write lock).

The clone-on-read strategy avoids holding a read lock during the potentially slow transform operation, preventing deadlocks while still sharing data between parallel tasks.

### Error Handling

The executor uses fail-fast semantics:

- If any task in a wave returns an error, `join_set.abort_all()` cancels remaining tasks.
- If a task panics, the `JoinError` is caught and converted to `TeckelError::Execution`.
- The error propagates upward, stopping all subsequent waves.

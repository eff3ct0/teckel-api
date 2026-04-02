---
sidebar_position: 3
---

# Job Management

The Teckel server manages pipeline executions as asynchronous jobs. This page describes the job lifecycle, the in-memory store, and the worker pool architecture.

![Job lifecycle](/img/diagrams/job-lifecycle.svg)

## Job Lifecycle

```
  ┌─────────┐     submit     ┌─────────┐    acquire     ┌─────────┐
  │ Created │ ────────────> │ Queued  │ ────────────> │ Running │
  └─────────┘               └─────────┘               └────┬────┘
                                  │                         │
                            cancel│                    ┌────┴────┐
                                  │               ok   │         │  error
                                  v                    v         v
                            ┌───────────┐     ┌───────────┐ ┌────────┐
                            │ Cancelled │     │ Completed │ │ Failed │
                            └───────────┘     └───────────┘ └────────┘
```

### States

| State | Description | Transitions |
|---|---|---|
| **Queued** | Job is in the mpsc channel, waiting for a semaphore permit | Running, Cancelled |
| **Running** | Job is actively executing on a worker task | Completed, Failed, Cancelled |
| **Completed** | Pipeline executed successfully | Terminal |
| **Failed** | Pipeline execution encountered an error | Terminal |
| **Cancelled** | User cancelled the job | Terminal |

### Timestamps

Each job tracks:

- `created_at` -- when the job was submitted (set on `Job::new()`)
- `started_at` -- when execution began (set when status changes to Running)
- `completed_at` -- when execution ended (set for Completed, Failed, or Cancelled)
- `duration_ms` -- computed as `completed_at - started_at` (only available for terminal states)

## JobStore

The `JobStore` is a thread-safe in-memory job store backed by `DashMap<String, Job>`:

```rust
#[derive(Clone)]
pub struct JobStore {
    jobs: Arc<DashMap<String, Job>>,
}
```

### Operations

| Method | Description |
|---|---|
| `insert(job)` | Add a new job to the store, returns job ID |
| `get_response(id)` | Get a serializable `JobResponse` for a job |
| `update_status(id, status)` | Update job status and set timestamps |
| `set_error(id, error)` | Mark job as failed with an error message |
| `get_cancel_token(id)` | Get the `CancellationToken` for a job |
| `get_yaml_and_vars(id)` | Get the pipeline YAML and variables for execution |
| `list_jobs()` | List all jobs as `JobResponse` objects |
| `wait_for_completion(id, timeout)` | Poll every 100ms until job reaches a terminal state |

### Job IDs

Job IDs are UUIDs (`Uuid::new_v4()`) generated at creation time.

### Cancellation

Each job carries a `CancellationToken` from `tokio-util`. When a cancel request arrives:

1. The token is retrieved from the store.
2. `token.cancel()` is called.
3. If the job is still queued, its status is immediately set to Cancelled.
4. If the job is running, the worker task detects cancellation via `tokio::select!`.

## Worker Pool

The `WorkerPool` manages concurrent pipeline execution using a tokio `Semaphore` to limit parallelism:

```rust
pub struct WorkerPool {
    sender: mpsc::Sender<JobMessage>,
}
```

### Architecture

```
  submit_job()
       │
       v
  ┌─────────────┐     mpsc channel     ┌──────────────┐
  │ HTTP handler │ ──────────────────> │  Dispatcher  │
  └─────────────┘                      └──────┬───────┘
                                              │
                               ┌──────────────┼──────────────┐
                               │              │              │
                          ┌────v────┐   ┌────v────┐   ┌────v────┐
                          │ Worker  │   │ Worker  │   │ Worker  │
                          │ Task 1  │   │ Task 2  │   │ Task 3  │
                          └─────────┘   └─────────┘   └─────────┘
                               ▲              ▲              ▲
                               └──────────────┴──────────────┘
                                    Semaphore (max_concurrency)
```

### Flow

1. **Submit**: The HTTP handler sends a `JobMessage` (containing the job ID) to the mpsc channel.
2. **Dispatch**: A background dispatcher task reads messages from the channel and spawns a new tokio task for each job.
3. **Acquire**: The spawned task acquires a semaphore permit. If all permits are taken (at max concurrency), the task blocks until one is available.
4. **Check cancellation**: Before starting execution, the task checks if the job was already cancelled.
5. **Execute**: The task calls `teckel_api::etl(&yaml, &variables)` with cancellation support via `tokio::select!`.
6. **Complete**: On success, the job status is updated to Completed. On error, it is set to Failed with the error message.

### Concurrency Control

The semaphore is initialized with `max_concurrency` permits (default: number of CPU cores). This limits how many pipelines can execute simultaneously. Jobs that exceed this limit wait in the mpsc channel until a permit becomes available.

### Cancellation Support

The worker uses `tokio::select!` to race between the pipeline execution and the cancellation token:

```rust
let result = tokio::select! {
    _ = cancel_token.cancelled() => {
        store.update_status(&job_id, JobStatus::Cancelled);
        return;
    }
    result = teckel_api::etl(&yaml, &variables) => result,
};
```

This ensures that a cancelled job stops at the next `.await` point inside the pipeline execution.

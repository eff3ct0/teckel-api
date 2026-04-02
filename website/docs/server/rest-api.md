---
sidebar_position: 2
---

# REST API

The Teckel HTTP server is built with [Axum](https://github.com/tokio-rs/axum) and provides a RESTful interface for the Teckel UI and external clients. CORS is enabled for all origins.

## Endpoints

### GET /api/health

Health check.

**Response** `200 OK`:
```json
{
  "status": "ok",
  "version": "0.1.0"
}
```

### POST /api/validate

Validates a pipeline YAML without executing it.

**Request**:
```json
{
  "yaml": "version: '3.0'\npipeline:\n  ...",
  "variables": {
    "ENV": "dev"
  }
}
```

**Response** `200 OK`:
```json
{
  "valid": true
}
```

**Response** `200 OK` (invalid pipeline):
```json
{
  "valid": false,
  "error": "V-001: asset \"missing\" references unknown asset \"nonexistent\""
}
```

### POST /api/explain

Generates a human-readable execution plan.

**Request**: Same as `/api/validate`.

**Response** `200 OK`:
```json
{
  "plan": "=== Pipeline Execution Plan ===\n\n--- Inputs ---\n  [raw_data] format=csv, path=data.csv\n\n--- Transformations ---\n  [filtered] WHERE (from: raw_data)\n\n--- Outputs ---\n  [result] from=filtered, format=parquet, mode=Overwrite, path=output.parquet\n\nTotal: 1 inputs, 1 transformations, 1 outputs\nExecution waves: 3\n  Wave 0: raw_data\n  Wave 1: filtered\n  Wave 2: result"
}
```

**Response** `400 Bad Request`:
```json
{
  "error": "parse error: ..."
}
```

### POST /api/jobs

Submits a pipeline for asynchronous execution. The pipeline is validated first (fail-fast). If valid, the job is queued for execution by the worker pool.

**Request**: Same as `/api/validate`.

**Response** `202 Accepted`:
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "queued"
}
```

**Response** `400 Bad Request` (validation failure):
```json
{
  "error": "validation failed: V-001: ..."
}
```

### GET /api/jobs

Lists all jobs.

**Response** `200 OK`:
```json
{
  "jobs": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "status": "completed",
      "created_at": "2025-01-15T10:30:00Z",
      "started_at": "2025-01-15T10:30:01Z",
      "completed_at": "2025-01-15T10:30:05Z",
      "duration_ms": 4000
    }
  ]
}
```

### GET /api/jobs/{id}

Gets the status of a specific job.

**Response** `200 OK`:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "created_at": "2025-01-15T10:30:00Z",
  "started_at": "2025-01-15T10:30:01Z"
}
```

**Response** `404 Not Found`:
```json
{
  "error": "job not found"
}
```

### GET /api/jobs/{id}/wait

Long-polls until the job reaches a terminal state (completed, failed, or cancelled). Optional query parameter `timeout` specifies the maximum wait time in seconds (default: 300).

**Example**: `GET /api/jobs/550e8400.../wait?timeout=60`

**Response** `200 OK`:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "created_at": "2025-01-15T10:30:00Z",
  "started_at": "2025-01-15T10:30:01Z",
  "completed_at": "2025-01-15T10:30:05Z",
  "duration_ms": 4000
}
```

If the timeout expires before the job completes, the current job status is returned (which may still be `running` or `queued`).

### DELETE /api/jobs/{id}

Cancels a running or queued job. Uses `CancellationToken` from `tokio-util` to signal the running task.

**Response** `200 OK`:
```json
{
  "status": "cancelling"
}
```

If the job was queued (not yet started), it is immediately marked as cancelled. If the job is running, cancellation is cooperative -- the task is signaled via `tokio::select!` and will stop at the next await point.

**Response** `404 Not Found`:
```json
{
  "error": "job not found"
}
```

## Job Status Values

| Status | Description |
|---|---|
| `queued` | Job is waiting in the worker pool queue |
| `running` | Job is actively executing |
| `completed` | Job finished successfully |
| `failed` | Job encountered an error |
| `cancelled` | Job was cancelled by the user |

## Authentication

The current API does not include authentication. For production deployments, place the server behind a reverse proxy (nginx, Traefik, etc.) that handles authentication and TLS termination.

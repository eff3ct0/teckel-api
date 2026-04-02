---
sidebar_position: 1
---

# gRPC API

The Teckel gRPC service is defined in `proto/teckel.proto` under the `teckel.v1` package. It provides two usage modes:

1. **High-level API** -- used by the Teckel UI and CLI for pipeline validation, execution plans, and async job management.
2. **Low-level API** -- Spark Connect-style session-based operations for programmatic clients.

## Service Definition

```protobuf
service TeckelService {
  // High-level API
  rpc Health(HealthRequest) returns (HealthResponse);
  rpc ValidatePipeline(PipelineRequest) returns (ValidateResponse);
  rpc ExplainPipeline(PipelineRequest) returns (ExplainResponse);
  rpc SubmitJob(PipelineRequest) returns (SubmitJobResponse);
  rpc GetJob(GetJobRequest) returns (JobResponse);
  rpc WaitForJob(WaitForJobRequest) returns (JobResponse);
  rpc CancelJob(CancelJobRequest) returns (CancelJobResponse);
  rpc ListJobs(ListJobsRequest) returns (ListJobsResponse);
  rpc InspectSource(InspectSourceRequest) returns (InspectSourceResponse);

  // Low-level API (Spark Connect-style)
  rpc CreateSession(CreateSessionRequest) returns (CreateSessionResponse);
  rpc CloseSession(CloseSessionRequest) returns (CloseSessionResponse);
  rpc ReadInput(ReadInputRequest) returns (DataFrameResponse);
  rpc ApplyTransform(ApplyTransformRequest) returns (DataFrameResponse);
  rpc WriteOutput(WriteOutputRequest) returns (WriteOutputResponse);
  rpc ExecutePipeline(ExecutePipelineRequest) returns (ExecutePipelineResponse);
}
```

## High-Level API

### Health

Health check endpoint.

```protobuf
message HealthRequest {}

message HealthResponse {
  string status = 1;    // "ok"
  string version = 2;   // Server version
}
```

### ValidatePipeline

Validates a pipeline YAML without executing it. Returns whether the pipeline is valid and any errors encountered during parsing.

```protobuf
message PipelineRequest {
  string yaml = 1;
  map<string, string> variables = 2;
}

message ValidateResponse {
  bool valid = 1;
  string error = 2;    // Empty if valid
}
```

### ExplainPipeline

Generates a human-readable execution plan showing inputs, transforms, outputs, and the wave schedule.

```protobuf
message ExplainResponse {
  string plan = 1;     // Multi-line text plan
}
```

### SubmitJob

Submits a pipeline for asynchronous execution. Returns immediately with a job ID that can be polled via GetJob or waited on via WaitForJob.

```protobuf
message SubmitJobResponse {
  string job_id = 1;
  string status = 2;   // "queued"
}
```

### GetJob

Retrieves the current status of a job.

```protobuf
message GetJobRequest {
  string job_id = 1;
}

message JobResponse {
  string id = 1;
  string status = 2;        // queued, running, completed, failed, cancelled
  string error = 3;         // Error message if failed
  string created_at = 4;    // ISO 8601
  string started_at = 5;    // ISO 8601, empty if not started
  string completed_at = 6;  // ISO 8601, empty if not completed
  int64 duration_ms = 7;    // -1 if not completed
}
```

### WaitForJob

Long-polls until a job reaches a terminal state (completed, failed, or cancelled) or the timeout expires.

```protobuf
message WaitForJobRequest {
  string job_id = 1;
  uint32 timeout_seconds = 2;  // Default: 300
}
```

Returns a `JobResponse`.

### CancelJob

Cancels a running or queued job.

```protobuf
message CancelJobRequest {
  string job_id = 1;
}

message CancelJobResponse {
  bool cancelled = 1;
  string status = 2;
}
```

### ListJobs

Lists all jobs.

```protobuf
message ListJobsRequest {}

message ListJobsResponse {
  repeated JobResponse jobs = 1;
}
```

### InspectSource

Inspects an input source to infer schema (field names and types) and row count.

```protobuf
message InspectSourceRequest {
  string format = 1;
  string path = 2;
  map<string, string> options = 3;
}

message InspectSourceResponse {
  repeated FieldInfo fields = 1;
  int64 row_count = 2;
}

message FieldInfo {
  string name = 1;
  string data_type = 2;
  bool nullable = 3;
}
```

## Low-Level API (Spark Connect-style)

The low-level API provides session-based, fine-grained control over pipeline execution. It is modeled after the Spark Connect protocol.

### CreateSession

Creates a new execution session on the worker. Each session has its own state (cached DataFrames, temp views).

```protobuf
message CreateSessionRequest {
  string backend = 1;     // "datafusion", "polars", etc.
}

message CreateSessionResponse {
  string session_id = 1;
}
```

### CloseSession

Closes a session and releases all associated resources.

```protobuf
message CloseSessionRequest {
  string session_id = 1;
}

message CloseSessionResponse {}
```

### ReadInput

Reads an input source and returns a handle to the resulting DataFrame.

```protobuf
message ReadInputRequest {
  string session_id = 1;
  string asset_name = 2;
  string format = 3;
  string path = 4;
  map<string, string> options = 5;
}

message DataFrameResponse {
  string session_id = 1;
  string handle_id = 2;
  int64 row_count = 3;
}
```

### ApplyTransform

Applies a transformation to one or more DataFrames identified by handle IDs.

```protobuf
message ApplyTransformRequest {
  string session_id = 1;
  string asset_name = 2;
  string source_json = 3;                    // Serialized Source enum
  map<string, string> input_handles = 4;     // asset_name -> handle_id
}
```

Returns a `DataFrameResponse` with the new handle ID.

### WriteOutput

Writes a DataFrame (identified by handle) to an output destination.

```protobuf
message WriteOutputRequest {
  string session_id = 1;
  string handle_id = 2;
  string format = 3;
  string path = 4;
  string mode = 5;
  map<string, string> options = 6;
}

message WriteOutputResponse {
  int64 rows_written = 1;
}
```

### ExecutePipeline

Submits a full pipeline for synchronous execution within a session. This is a convenience method that combines parsing, DAG construction, and execution.

```protobuf
message ExecutePipelineRequest {
  string session_id = 1;
  string yaml = 2;
  map<string, string> variables = 3;
}

message ExecutePipelineResponse {
  string job_id = 1;
  string status = 2;     // "completed" or "failed"
  string error = 3;
  int64 duration_ms = 4;
}
```

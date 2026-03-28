//! gRPC service implementation for the Teckel worker.

use dashmap::DashMap;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use teckel_remote::proto::{self, pipeline_service_server::PipelineService};
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// Worker state: manages sessions and their execution contexts.
pub struct TeckelWorker {
    sessions: Arc<DashMap<String, SessionState>>,
}

#[allow(dead_code)]
struct SessionState {
    backend: String,
    created_at: Instant,
}

impl TeckelWorker {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
        }
    }
}

#[tonic::async_trait]
impl PipelineService for TeckelWorker {
    async fn create_session(
        &self,
        request: Request<proto::CreateSessionRequest>,
    ) -> Result<Response<proto::CreateSessionResponse>, Status> {
        let req = request.into_inner();
        let session_id = Uuid::new_v4().to_string();

        tracing::info!(
            session_id = %session_id,
            backend = %req.backend,
            "session created"
        );

        self.sessions.insert(
            session_id.clone(),
            SessionState {
                backend: req.backend,
                created_at: Instant::now(),
            },
        );

        Ok(Response::new(proto::CreateSessionResponse { session_id }))
    }

    async fn close_session(
        &self,
        request: Request<proto::CloseSessionRequest>,
    ) -> Result<Response<proto::CloseSessionResponse>, Status> {
        let req = request.into_inner();

        if self.sessions.remove(&req.session_id).is_some() {
            tracing::info!(session_id = %req.session_id, "session closed");
            Ok(Response::new(proto::CloseSessionResponse {}))
        } else {
            Err(Status::not_found(format!(
                "session {} not found",
                req.session_id
            )))
        }
    }

    async fn read_input(
        &self,
        request: Request<proto::ReadInputRequest>,
    ) -> Result<Response<proto::DataFrameResponse>, Status> {
        let req = request.into_inner();

        if !self.sessions.contains_key(&req.session_id) {
            return Err(Status::not_found("session not found"));
        }

        // Per-op mode: placeholder for future implementation
        let handle_id = Uuid::new_v4().to_string();
        tracing::debug!(
            session_id = %req.session_id,
            handle_id = %handle_id,
            format = %req.format,
            path = %req.path,
            "read_input (per-op mode)"
        );

        Ok(Response::new(proto::DataFrameResponse {
            session_id: req.session_id,
            handle_id,
            row_count: -1,
        }))
    }

    async fn apply_transform(
        &self,
        request: Request<proto::ApplyTransformRequest>,
    ) -> Result<Response<proto::DataFrameResponse>, Status> {
        let req = request.into_inner();

        if !self.sessions.contains_key(&req.session_id) {
            return Err(Status::not_found("session not found"));
        }

        let handle_id = Uuid::new_v4().to_string();
        tracing::debug!(
            session_id = %req.session_id,
            handle_id = %handle_id,
            asset = %req.asset_name,
            "apply_transform (per-op mode)"
        );

        Ok(Response::new(proto::DataFrameResponse {
            session_id: req.session_id,
            handle_id,
            row_count: -1,
        }))
    }

    async fn write_output(
        &self,
        request: Request<proto::WriteOutputRequest>,
    ) -> Result<Response<proto::WriteOutputResponse>, Status> {
        let req = request.into_inner();

        if !self.sessions.contains_key(&req.session_id) {
            return Err(Status::not_found("session not found"));
        }

        tracing::debug!(
            session_id = %req.session_id,
            handle_id = %req.handle_id,
            format = %req.format,
            path = %req.path,
            "write_output (per-op mode)"
        );

        Ok(Response::new(proto::WriteOutputResponse { rows_written: 0 }))
    }

    async fn execute_pipeline(
        &self,
        request: Request<proto::ExecutePipelineRequest>,
    ) -> Result<Response<proto::ExecutePipelineResponse>, Status> {
        let req = request.into_inner();

        if !self.sessions.contains_key(&req.session_id) {
            return Err(Status::not_found("session not found"));
        }

        let job_id = Uuid::new_v4().to_string();
        tracing::info!(
            session_id = %req.session_id,
            job_id = %job_id,
            "executing full pipeline on worker"
        );

        let start = Instant::now();

        // Execute the pipeline using the local teckel-api (DataFusion backend)
        let variables: BTreeMap<String, String> = req.variables.into_iter().collect();
        match teckel_api::etl(&req.yaml, &variables).await {
            Ok(()) => {
                let duration_ms = start.elapsed().as_millis() as i64;
                tracing::info!(
                    job_id = %job_id,
                    duration_ms,
                    "pipeline completed on worker"
                );
                Ok(Response::new(proto::ExecutePipelineResponse {
                    job_id,
                    status: "completed".to_string(),
                    error: String::new(),
                    duration_ms,
                }))
            }
            Err(e) => {
                let duration_ms = start.elapsed().as_millis() as i64;
                let error = format!("{e}");
                tracing::error!(
                    job_id = %job_id,
                    error = %error,
                    duration_ms,
                    "pipeline failed on worker"
                );
                Ok(Response::new(proto::ExecutePipelineResponse {
                    job_id,
                    status: "failed".to_string(),
                    error,
                    duration_ms,
                }))
            }
        }
    }

    async fn get_status(
        &self,
        _request: Request<proto::GetStatusRequest>,
    ) -> Result<Response<proto::GetStatusResponse>, Status> {
        // For full pipeline mode, status is returned synchronously in execute_pipeline.
        // This endpoint is for future async pipeline mode.
        Err(Status::unimplemented(
            "async pipeline status tracking not yet implemented",
        ))
    }

    async fn cancel_pipeline(
        &self,
        _request: Request<proto::CancelPipelineRequest>,
    ) -> Result<Response<proto::CancelPipelineResponse>, Status> {
        Err(Status::unimplemented(
            "pipeline cancellation not yet implemented",
        ))
    }
}

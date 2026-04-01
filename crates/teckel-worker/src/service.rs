//! Unified gRPC service: high-level job API + low-level Spark Connect-style.

use chrono::Utc;
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use teckel_remote::proto::{self, teckel_service_server::TeckelService};
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};
use uuid::Uuid;

// ── Job state ────────────────────────────────────────────────

#[derive(Clone)]
struct JobState {
    id: String,
    status: String,
    error: String,
    created_at: String,
    started_at: String,
    completed_at: String,
    duration_ms: i64,
    yaml: String,
    variables: BTreeMap<String, String>,
    cancel: tokio_util::sync::CancellationToken,
}

impl JobState {
    fn to_proto(&self) -> proto::JobResponse {
        proto::JobResponse {
            id: self.id.clone(),
            status: self.status.clone(),
            error: self.error.clone(),
            created_at: self.created_at.clone(),
            started_at: self.started_at.clone(),
            completed_at: self.completed_at.clone(),
            duration_ms: self.duration_ms,
        }
    }
}

// ── Session state (low-level API) ────────────────────────────

#[allow(dead_code)]
struct SessionState {
    backend: String,
    created_at: Instant,
}

// ── Worker ───────────────────────────────────────────────────

pub struct TeckelWorker {
    jobs: Arc<DashMap<String, JobState>>,
    sessions: Arc<DashMap<String, SessionState>>,
    semaphore: Arc<Semaphore>,
}

impl TeckelWorker {
    pub fn new(max_concurrency: usize) -> Self {
        Self {
            jobs: Arc::new(DashMap::new()),
            sessions: Arc::new(DashMap::new()),
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
        }
    }
}

#[tonic::async_trait]
impl TeckelService for TeckelWorker {
    // ── Health ───────────────────────────────────────────────

    async fn health(
        &self,
        _request: Request<proto::HealthRequest>,
    ) -> Result<Response<proto::HealthResponse>, Status> {
        Ok(Response::new(proto::HealthResponse {
            status: "ok".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }

    // ── Validate ─────────────────────────────────────────────

    async fn validate_pipeline(
        &self,
        request: Request<proto::PipelineRequest>,
    ) -> Result<Response<proto::ValidateResponse>, Status> {
        let req = request.into_inner();
        let variables: BTreeMap<String, String> = req.variables.into_iter().collect();
        match teckel_api::validate(&req.yaml, &variables) {
            Ok(()) => Ok(Response::new(proto::ValidateResponse {
                valid: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::ValidateResponse {
                valid: false,
                error: format!("{e}"),
            })),
        }
    }

    // ── Explain ──────────────────────────────────────────────

    async fn explain_pipeline(
        &self,
        request: Request<proto::PipelineRequest>,
    ) -> Result<Response<proto::ExplainResponse>, Status> {
        let req = request.into_inner();
        let variables: BTreeMap<String, String> = req.variables.into_iter().collect();
        match teckel_api::explain(&req.yaml, &variables) {
            Ok(plan) => Ok(Response::new(proto::ExplainResponse { plan })),
            Err(e) => Err(Status::invalid_argument(format!("{e}"))),
        }
    }

    // ── Submit Job ───────────────────────────────────────────

    async fn submit_job(
        &self,
        request: Request<proto::PipelineRequest>,
    ) -> Result<Response<proto::SubmitJobResponse>, Status> {
        let req = request.into_inner();
        let variables: BTreeMap<String, String> = req.variables.into_iter().collect();

        // Validate first
        if let Err(e) = teckel_api::validate(&req.yaml, &variables) {
            return Err(Status::invalid_argument(format!("validation failed: {e}")));
        }

        let job_id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let cancel = tokio_util::sync::CancellationToken::new();

        let job = JobState {
            id: job_id.clone(),
            status: "queued".to_string(),
            error: String::new(),
            created_at: now,
            started_at: String::new(),
            completed_at: String::new(),
            duration_ms: -1,
            yaml: req.yaml,
            variables,
            cancel: cancel.clone(),
        };

        self.jobs.insert(job_id.clone(), job);

        // Spawn execution
        let jobs = Arc::clone(&self.jobs);
        let semaphore = Arc::clone(&self.semaphore);
        let jid = job_id.clone();

        tokio::spawn(async move {
            let _permit = semaphore.acquire().await.expect("semaphore closed");

            if cancel.is_cancelled() {
                if let Some(mut job) = jobs.get_mut(&jid) {
                    job.status = "cancelled".to_string();
                    job.completed_at = Utc::now().to_rfc3339();
                }
                return;
            }

            let (yaml, variables) = {
                let job = jobs.get(&jid).unwrap();
                (job.yaml.clone(), job.variables.clone())
            };

            // Mark running
            if let Some(mut job) = jobs.get_mut(&jid) {
                job.status = "running".to_string();
                job.started_at = Utc::now().to_rfc3339();
            }

            let start = Instant::now();

            let result = tokio::select! {
                _ = cancel.cancelled() => {
                    if let Some(mut job) = jobs.get_mut(&jid) {
                        job.status = "cancelled".to_string();
                        job.completed_at = Utc::now().to_rfc3339();
                        job.duration_ms = start.elapsed().as_millis() as i64;
                    }
                    return;
                }
                result = teckel_api::etl(&yaml, &variables) => result,
            };

            let duration_ms = start.elapsed().as_millis() as i64;
            let completed_at = Utc::now().to_rfc3339();

            if let Some(mut job) = jobs.get_mut(&jid) {
                job.duration_ms = duration_ms;
                job.completed_at = completed_at;
                match result {
                    Ok(()) => job.status = "completed".to_string(),
                    Err(e) => {
                        job.status = "failed".to_string();
                        job.error = format!("{e}");
                    }
                }
            }
        });

        Ok(Response::new(proto::SubmitJobResponse {
            job_id,
            status: "queued".to_string(),
        }))
    }

    // ── Get Job ──────────────────────────────────────────────

    async fn get_job(
        &self,
        request: Request<proto::GetJobRequest>,
    ) -> Result<Response<proto::JobResponse>, Status> {
        let req = request.into_inner();
        match self.jobs.get(&req.job_id) {
            Some(job) => Ok(Response::new(job.to_proto())),
            None => Err(Status::not_found("job not found")),
        }
    }

    // ── Wait For Job ─────────────────────────────────────────

    async fn wait_for_job(
        &self,
        request: Request<proto::WaitForJobRequest>,
    ) -> Result<Response<proto::JobResponse>, Status> {
        let req = request.into_inner();
        let timeout = std::time::Duration::from_secs(if req.timeout_seconds == 0 {
            300
        } else {
            req.timeout_seconds as u64
        });
        let start = Instant::now();

        loop {
            if let Some(job) = self.jobs.get(&req.job_id) {
                match job.status.as_str() {
                    "completed" | "failed" | "cancelled" => {
                        return Ok(Response::new(job.to_proto()));
                    }
                    _ => {}
                }
            } else {
                return Err(Status::not_found("job not found"));
            }
            if start.elapsed() >= timeout {
                return match self.jobs.get(&req.job_id) {
                    Some(job) => Ok(Response::new(job.to_proto())),
                    None => Err(Status::not_found("job not found")),
                };
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    // ── Cancel Job ───────────────────────────────────────────

    async fn cancel_job(
        &self,
        request: Request<proto::CancelJobRequest>,
    ) -> Result<Response<proto::CancelJobResponse>, Status> {
        let req = request.into_inner();
        match self.jobs.get(&req.job_id) {
            Some(job) => {
                job.cancel.cancel();
                let status = if job.status == "queued" {
                    drop(job);
                    if let Some(mut j) = self.jobs.get_mut(&req.job_id) {
                        j.status = "cancelled".to_string();
                        j.completed_at = Utc::now().to_rfc3339();
                    }
                    "cancelled"
                } else {
                    "cancelling"
                };
                Ok(Response::new(proto::CancelJobResponse {
                    cancelled: true,
                    status: status.to_string(),
                }))
            }
            None => Err(Status::not_found("job not found")),
        }
    }

    // ── List Jobs ────────────────────────────────────────────

    async fn list_jobs(
        &self,
        _request: Request<proto::ListJobsRequest>,
    ) -> Result<Response<proto::ListJobsResponse>, Status> {
        let jobs: Vec<proto::JobResponse> = self.jobs.iter().map(|j| j.to_proto()).collect();
        Ok(Response::new(proto::ListJobsResponse { jobs }))
    }

    // ── Inspect Source ───────────────────────────────────────

    async fn inspect_source(
        &self,
        request: Request<proto::InspectSourceRequest>,
    ) -> Result<Response<proto::InspectSourceResponse>, Status> {
        let req = request.into_inner();
        let options: BTreeMap<String, String> = req.options.into_iter().collect();

        match teckel_api::inspect(&req.format, &req.path, &options).await {
            Ok(result) => {
                let fields = result
                    .fields
                    .into_iter()
                    .map(|f| proto::FieldInfo {
                        name: f.name,
                        data_type: f.data_type,
                        nullable: f.nullable,
                    })
                    .collect();
                Ok(Response::new(proto::InspectSourceResponse {
                    fields,
                    row_count: result.row_count as i64,
                }))
            }
            Err(e) => Err(Status::invalid_argument(format!("{e}"))),
        }
    }

    // ── Low-level: Sessions ──────────────────────────────────

    async fn create_session(
        &self,
        request: Request<proto::CreateSessionRequest>,
    ) -> Result<Response<proto::CreateSessionResponse>, Status> {
        let req = request.into_inner();
        let session_id = Uuid::new_v4().to_string();
        self.sessions.insert(
            session_id.clone(),
            SessionState {
                backend: req.backend,
                created_at: Instant::now(),
            },
        );
        tracing::info!(session_id = %session_id, "session created");
        Ok(Response::new(proto::CreateSessionResponse { session_id }))
    }

    async fn close_session(
        &self,
        request: Request<proto::CloseSessionRequest>,
    ) -> Result<Response<proto::CloseSessionResponse>, Status> {
        let req = request.into_inner();
        if self.sessions.remove(&req.session_id).is_some() {
            Ok(Response::new(proto::CloseSessionResponse {}))
        } else {
            Err(Status::not_found("session not found"))
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
        Ok(Response::new(proto::DataFrameResponse {
            session_id: req.session_id,
            handle_id: Uuid::new_v4().to_string(),
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
        Ok(Response::new(proto::DataFrameResponse {
            session_id: req.session_id,
            handle_id: Uuid::new_v4().to_string(),
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
        Ok(Response::new(proto::WriteOutputResponse {
            rows_written: 0,
        }))
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
        let start = Instant::now();
        let variables: BTreeMap<String, String> = req.variables.into_iter().collect();

        match teckel_api::etl(&req.yaml, &variables).await {
            Ok(()) => Ok(Response::new(proto::ExecutePipelineResponse {
                job_id,
                status: "completed".to_string(),
                error: String::new(),
                duration_ms: start.elapsed().as_millis() as i64,
            })),
            Err(e) => Ok(Response::new(proto::ExecutePipelineResponse {
                job_id,
                status: "failed".to_string(),
                error: format!("{e}"),
                duration_ms: start.elapsed().as_millis() as i64,
            })),
        }
    }
}

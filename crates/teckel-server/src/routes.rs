use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::job::{Job, JobStatus};
#[allow(unused_imports)]
use crate::AppState;

// --- Request / Response types ---

#[derive(Deserialize)]
pub struct PipelineRequest {
    pub yaml: String,
    #[serde(default)]
    pub variables: BTreeMap<String, String>,
}

#[derive(Serialize)]
pub struct ValidateResponse {
    pub valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
}

// --- Handlers ---

/// GET /api/health
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// POST /api/validate — synchronous validation
pub async fn validate(Json(body): Json<PipelineRequest>) -> impl IntoResponse {
    match teckel_api::validate(&body.yaml, &body.variables) {
        Ok(()) => (
            StatusCode::OK,
            Json(ValidateResponse {
                valid: true,
                error: None,
            }),
        ),
        Err(e) => (
            StatusCode::OK,
            Json(ValidateResponse {
                valid: false,
                error: Some(format!("{e}")),
            }),
        ),
    }
}

/// POST /api/explain — synchronous execution plan
pub async fn explain(Json(body): Json<PipelineRequest>) -> impl IntoResponse {
    match teckel_api::explain(&body.yaml, &body.variables) {
        Ok(plan) => (StatusCode::OK, Json(serde_json::json!({ "plan": plan }))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": format!("{e}") })),
        ),
    }
}

/// POST /api/jobs — submit pipeline for async execution
pub async fn submit_job(
    State(state): State<AppState>,
    Json(body): Json<PipelineRequest>,
) -> impl IntoResponse {
    // Validate first to fail fast
    if let Err(e) = teckel_api::validate(&body.yaml, &body.variables) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!("validation failed: {e}")
            })),
        );
    }

    let job = Job::new(body.yaml, body.variables);
    let job_id = job.id.clone();
    state.store.insert(job);
    state.pool.submit(job_id.clone()).await;

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "job_id": job_id,
            "status": "queued"
        })),
    )
}

/// GET /api/jobs/:id — poll job status
pub async fn get_job(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match state.store.get_response(&id) {
        Some(resp) => (StatusCode::OK, Json(serde_json::to_value(resp).unwrap())),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "job not found" })),
        ),
    }
}

/// DELETE /api/jobs/:id — cancel a running/queued job
pub async fn cancel_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let token = state.store.get_cancel_token(&id);
    match token {
        Some(t) => {
            t.cancel();
            // If still queued, mark as cancelled immediately
            if let Some(resp) = state.store.get_response(&id) {
                if resp.status == JobStatus::Queued {
                    state.store.update_status(&id, JobStatus::Cancelled);
                }
            }
            (
                StatusCode::OK,
                Json(serde_json::json!({ "status": "cancelling" })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "job not found" })),
        ),
    }
}

/// GET /api/jobs — list all jobs
pub async fn list_jobs(State(state): State<AppState>) -> impl IntoResponse {
    let jobs = state.store.list_jobs();
    (StatusCode::OK, Json(serde_json::json!({ "jobs": jobs })))
}

#[derive(Deserialize)]
pub struct WaitParams {
    /// Timeout in seconds (default: 300)
    #[serde(default = "default_wait_timeout")]
    pub timeout: u64,
}

fn default_wait_timeout() -> u64 {
    300
}

/// GET /api/jobs/:id/wait?timeout=300 — block until job completes
pub async fn wait_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
    axum::extract::Query(params): axum::extract::Query<WaitParams>,
) -> impl IntoResponse {
    let timeout = std::time::Duration::from_secs(params.timeout);
    match state.store.wait_for_completion(&id, timeout).await {
        Some(resp) => (StatusCode::OK, Json(serde_json::to_value(resp).unwrap())),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "job not found" })),
        ),
    }
}

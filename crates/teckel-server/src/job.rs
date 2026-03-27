use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResponse {
    pub id: String,
    pub status: JobStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct Job {
    pub id: String,
    pub yaml: String,
    pub variables: BTreeMap<String, String>,
    pub status: JobStatus,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub cancel_token: CancellationToken,
}

impl Job {
    pub fn new(yaml: String, variables: BTreeMap<String, String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            yaml,
            variables,
            status: JobStatus::Queued,
            error: None,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            cancel_token: CancellationToken::new(),
        }
    }

    pub fn to_response(&self) -> JobResponse {
        JobResponse {
            id: self.id.clone(),
            status: self.status,
            error: self.error.clone(),
            created_at: self.created_at,
            started_at: self.started_at,
            completed_at: self.completed_at,
        }
    }
}

/// Thread-safe in-memory job store.
#[derive(Clone)]
pub struct JobStore {
    jobs: Arc<DashMap<String, Job>>,
}

impl JobStore {
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(DashMap::new()),
        }
    }

    pub fn insert(&self, job: Job) -> String {
        let id = job.id.clone();
        self.jobs.insert(id.clone(), job);
        id
    }

    pub fn get_response(&self, id: &str) -> Option<JobResponse> {
        self.jobs.get(id).map(|j| j.to_response())
    }

    pub fn update_status(&self, id: &str, status: JobStatus) {
        if let Some(mut job) = self.jobs.get_mut(id) {
            job.status = status;
            match status {
                JobStatus::Running => job.started_at = Some(Utc::now()),
                JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled => {
                    job.completed_at = Some(Utc::now());
                }
                _ => {}
            }
        }
    }

    pub fn set_error(&self, id: &str, error: String) {
        if let Some(mut job) = self.jobs.get_mut(id) {
            job.error = Some(error);
            job.status = JobStatus::Failed;
            job.completed_at = Some(Utc::now());
        }
    }

    pub fn get_cancel_token(&self, id: &str) -> Option<CancellationToken> {
        self.jobs.get(id).map(|j| j.cancel_token.clone())
    }

    pub fn get_yaml_and_vars(&self, id: &str) -> Option<(String, BTreeMap<String, String>)> {
        self.jobs
            .get(id)
            .map(|j| (j.yaml.clone(), j.variables.clone()))
    }

    pub fn list_jobs(&self) -> Vec<JobResponse> {
        self.jobs.iter().map(|j| j.to_response()).collect()
    }
}

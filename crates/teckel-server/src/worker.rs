use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};

use crate::job::{JobStatus, JobStore};

/// Message sent to the worker pool.
pub struct JobMessage {
    pub job_id: String,
}

/// Non-blocking worker pool backed by a tokio semaphore.
///
/// Jobs are submitted via an mpsc channel. A dispatcher task reads from the
/// channel and spawns a new tokio task for each job, bounded by the semaphore
/// to limit concurrent executions.
#[derive(Clone)]
pub struct WorkerPool {
    sender: mpsc::Sender<JobMessage>,
}

impl WorkerPool {
    /// Create a new worker pool with the given concurrency limit.
    pub fn new(store: JobStore, max_concurrency: usize) -> Self {
        let (sender, receiver) = mpsc::channel::<JobMessage>(1024);
        let semaphore = Arc::new(Semaphore::new(max_concurrency));

        tokio::spawn(Self::dispatcher(receiver, store, semaphore));

        Self { sender }
    }

    /// Submit a job for execution.
    pub async fn submit(&self, job_id: String) {
        if let Err(e) = self.sender.send(JobMessage { job_id }).await {
            tracing::error!("failed to submit job: {}", e);
        }
    }

    /// Dispatcher loop: reads jobs from channel and spawns bounded tasks.
    async fn dispatcher(
        mut receiver: mpsc::Receiver<JobMessage>,
        store: JobStore,
        semaphore: Arc<Semaphore>,
    ) {
        while let Some(msg) = receiver.recv().await {
            let store = store.clone();
            let semaphore = semaphore.clone();

            tokio::spawn(async move {
                // Acquire semaphore permit (blocks if at max concurrency)
                let _permit = semaphore.acquire().await.expect("semaphore closed");

                let job_id = msg.job_id;

                // Get cancel token before starting
                let cancel_token = match store.get_cancel_token(&job_id) {
                    Some(t) => t,
                    None => {
                        tracing::warn!(job_id, "job not found in store, skipping");
                        return;
                    }
                };

                // Check if already cancelled before starting
                if cancel_token.is_cancelled() {
                    store.update_status(&job_id, JobStatus::Cancelled);
                    tracing::info!(job_id, "job cancelled before execution");
                    return;
                }

                // Get YAML and variables
                let (yaml, variables) = match store.get_yaml_and_vars(&job_id) {
                    Some(data) => data,
                    None => {
                        tracing::warn!(job_id, "job data not found");
                        return;
                    }
                };

                store.update_status(&job_id, JobStatus::Running);
                tracing::info!(job_id, "job started");

                // Execute with cancellation support
                let result = tokio::select! {
                    _ = cancel_token.cancelled() => {
                        store.update_status(&job_id, JobStatus::Cancelled);
                        tracing::info!(job_id, "job cancelled during execution");
                        return;
                    }
                    result = teckel_api::etl(&yaml, &variables) => result,
                };

                match result {
                    Ok(()) => {
                        store.update_status(&job_id, JobStatus::Completed);
                        tracing::info!(job_id, "job completed successfully");
                    }
                    Err(e) => {
                        let error_msg = format!("{e}");
                        tracing::error!(job_id, error = %error_msg, "job failed");
                        store.set_error(&job_id, error_msg);
                    }
                }
            });
        }
    }
}

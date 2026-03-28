//! RemoteBackend: implements the Backend trait by sending operations to a gRPC worker.
//!
//! Each DataFrame is represented as a `RemoteRef` (opaque handle ID on the worker).
//! The actual data never leaves the worker — only handles are exchanged.

use crate::proto::{self, teckel_service_client::TeckelServiceClient};
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};
use teckel_engine::Backend;
use teckel_model::source::{InputSource, OutputSource, Source};
use teckel_model::TeckelError;
use tonic::transport::Channel;
use uuid::Uuid;

/// Opaque reference to a DataFrame living on a remote worker.
#[derive(Debug, Clone)]
pub struct RemoteRef {
    pub handle_id: String,
    pub session_id: String,
}

/// Backend that delegates execution to a remote gRPC worker.
pub struct RemoteBackend {
    client: TeckelServiceClient<Channel>,
    session_id: String,
}

impl RemoteBackend {
    /// Connect to a worker and create a session.
    pub async fn connect(endpoint: &str) -> Result<Self, TeckelError> {
        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|e| TeckelError::Execution(format!("invalid endpoint: {e}")))?
            .connect()
            .await
            .map_err(|e| TeckelError::Execution(format!("failed to connect to worker: {e}")))?;

        let mut client = TeckelServiceClient::new(channel);

        let response = client
            .create_session(proto::CreateSessionRequest {
                backend: "datafusion".to_string(),
            })
            .await
            .map_err(|e| TeckelError::Execution(format!("create session: {e}")))?;

        let session_id = response.into_inner().session_id;
        tracing::info!(session_id = %session_id, endpoint, "remote session created");

        Ok(Self { client, session_id })
    }

    /// Execute a full pipeline on the worker (simpler alternative to per-op mode).
    pub async fn execute_pipeline(
        &mut self,
        yaml: &str,
        variables: &BTreeMap<String, String>,
    ) -> Result<(), TeckelError> {
        let response = self
            .client
            .execute_pipeline(proto::ExecutePipelineRequest {
                session_id: self.session_id.clone(),
                yaml: yaml.to_string(),
                variables: variables.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<HashMap<_, _>>(),
            })
            .await
            .map_err(|e| TeckelError::Execution(format!("execute pipeline: {e}")))?;

        let resp = response.into_inner();
        if resp.status == "failed" {
            return Err(TeckelError::Execution(format!(
                "remote pipeline failed: {}",
                resp.error
            )));
        }

        tracing::info!(
            job_id = %resp.job_id,
            duration_ms = resp.duration_ms,
            "remote pipeline completed"
        );
        Ok(())
    }

    /// Close the session and release worker resources.
    pub async fn close(mut self) -> Result<(), TeckelError> {
        self.client
            .close_session(proto::CloseSessionRequest {
                session_id: self.session_id.clone(),
            })
            .await
            .map_err(|e| TeckelError::Execution(format!("close session: {e}")))?;
        tracing::info!(session_id = %self.session_id, "remote session closed");
        Ok(())
    }
}

#[async_trait]
impl Backend for RemoteBackend {
    type DataFrame = RemoteRef;

    fn name(&self) -> &str {
        "remote"
    }

    async fn read_input(&self, input: &InputSource) -> Result<RemoteRef, TeckelError> {
        let options: HashMap<String, String> = input
            .options
            .iter()
            .map(|(k, v)| (k.clone(), format!("{v:?}")))
            .collect();

        let response = self
            .client
            .clone()
            .read_input(proto::ReadInputRequest {
                session_id: self.session_id.clone(),
                asset_name: Uuid::new_v4().to_string(),
                format: input.format.clone(),
                path: input.path.clone(),
                options,
            })
            .await
            .map_err(|e| TeckelError::Execution(format!("remote read_input: {e}")))?;

        let resp = response.into_inner();
        Ok(RemoteRef {
            handle_id: resp.handle_id,
            session_id: resp.session_id,
        })
    }

    async fn write_output(
        &self,
        df: RemoteRef,
        output: &OutputSource,
    ) -> Result<(), TeckelError> {
        let mode = format!("{:?}", output.mode).to_lowercase();
        let options: HashMap<String, String> = output
            .options
            .iter()
            .map(|(k, v)| (k.clone(), format!("{v:?}")))
            .collect();

        self.client
            .clone()
            .write_output(proto::WriteOutputRequest {
                session_id: self.session_id.clone(),
                handle_id: df.handle_id,
                format: output.format.clone(),
                path: output.path.clone(),
                mode,
                options,
            })
            .await
            .map_err(|e| TeckelError::Execution(format!("remote write_output: {e}")))?;

        Ok(())
    }

    async fn apply(
        &self,
        _source: &Source,
        _inputs: &BTreeMap<String, RemoteRef>,
    ) -> Result<RemoteRef, TeckelError> {
        // Per-operation mode requires Source serialization (planned for a future version).
        // For now, use RemoteBackend::execute_pipeline() which sends the full YAML
        // to the worker for server-side parsing and execution.
        Err(TeckelError::Execution(
            "per-operation remote execution not yet supported. \
             Use RemoteBackend::execute_pipeline() instead."
                .to_string(),
        ))
    }
}

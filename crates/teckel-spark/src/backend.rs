use async_trait::async_trait;
use spark_connect_rs::{SparkSession, SparkSessionBuilder};
use std::collections::BTreeMap;
use std::time::Duration;
use teckel_engine::Backend;
use teckel_model::source::{InputSource, OutputSource, Source};
use teckel_model::TeckelError;

use crate::reader;
use crate::transforms;
use crate::writer;

/// Apache Spark Connect remote backend.
///
/// Executes Teckel pipelines on a remote Spark cluster via the Spark Connect
/// gRPC protocol. Uses `spark-connect-rs` as the client library.
pub struct SparkConnectBackend {
    session: SparkSession,
}

impl SparkConnectBackend {
    /// Create a new backend connecting to a Spark Connect server.
    ///
    /// # Arguments
    /// * `connection` - Spark Connect URL (e.g., `"sc://127.0.0.1:15002/"`)
    pub async fn new(connection: &str) -> Result<Self, TeckelError> {
        let session = SparkSessionBuilder::remote(connection)
            .app_name("teckel")
            .connect_timeout(Duration::from_secs(30))
            .request_timeout(Duration::from_secs(300))
            .build()
            .await
            .map_err(|e| TeckelError::Execution(format!("spark connect: {e}")))?;

        Ok(Self { session })
    }

    /// Create a backend from an existing SparkSession.
    pub fn with_session(session: SparkSession) -> Self {
        Self { session }
    }

    /// Access the underlying SparkSession.
    pub fn session(&self) -> &SparkSession {
        &self.session
    }
}

#[async_trait]
impl Backend for SparkConnectBackend {
    type DataFrame = spark_connect_rs::DataFrame;

    fn name(&self) -> &str {
        "spark"
    }

    async fn read_input(&self, input: &InputSource) -> Result<Self::DataFrame, TeckelError> {
        reader::read_input(&self.session, input).await
    }

    async fn write_output(
        &self,
        df: Self::DataFrame,
        output: &OutputSource,
    ) -> Result<(), TeckelError> {
        writer::write_output(df, output).await
    }

    async fn apply(
        &self,
        source: &Source,
        inputs: &BTreeMap<String, Self::DataFrame>,
    ) -> Result<Self::DataFrame, TeckelError> {
        transforms::apply(&self.session, source, inputs).await
    }
}

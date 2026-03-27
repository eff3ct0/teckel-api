use async_trait::async_trait;
use datafusion::prelude::*;
use std::collections::BTreeMap;
use teckel_engine::Backend;
use teckel_model::source::{InputSource, OutputSource, Source};
use teckel_model::TeckelError;

use crate::reader;
use crate::transforms;
use crate::writer;

/// Apache DataFusion local backend.
///
/// Executes pipelines in-process using DataFusion's query engine.
/// This is the Phase 1 backend; a future `RemoteBackend` will implement
/// the same `Backend` trait but delegate to gRPC workers.
pub struct DataFusionBackend {
    ctx: SessionContext,
}

impl DataFusionBackend {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    pub fn with_session(ctx: SessionContext) -> Self {
        Self { ctx }
    }

    pub fn session(&self) -> &SessionContext {
        &self.ctx
    }
}

impl Default for DataFusionBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Backend for DataFusionBackend {
    type DataFrame = DataFrame;

    fn name(&self) -> &str {
        "datafusion"
    }

    async fn read_input(&self, input: &InputSource) -> Result<DataFrame, TeckelError> {
        reader::read_input(&self.ctx, input).await
    }

    async fn write_output(&self, df: DataFrame, output: &OutputSource) -> Result<(), TeckelError> {
        writer::write_output(df, output).await
    }

    async fn apply(
        &self,
        source: &Source,
        inputs: &BTreeMap<String, DataFrame>,
    ) -> Result<DataFrame, TeckelError> {
        transforms::apply(&self.ctx, source, inputs).await
    }
}

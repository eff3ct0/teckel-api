use async_trait::async_trait;
use std::collections::BTreeMap;
use teckel_engine::Backend;
use teckel_model::source::{InputSource, OutputSource, Source};
use teckel_model::TeckelError;

use crate::reader;
use crate::transforms;
use crate::writer;

/// Polars backend for high-performance single-machine pipeline execution.
///
/// Uses Polars lazy API for optimized query planning and execution.
pub struct PolarsBackend;

impl PolarsBackend {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PolarsBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Backend for PolarsBackend {
    type DataFrame = polars::frame::DataFrame;

    fn name(&self) -> &str {
        "polars"
    }

    async fn read_input(
        &self,
        input: &InputSource,
    ) -> Result<polars::frame::DataFrame, TeckelError> {
        reader::read_input(input)
    }

    async fn write_output(
        &self,
        df: polars::frame::DataFrame,
        output: &OutputSource,
    ) -> Result<(), TeckelError> {
        writer::write_output(df, output)
    }

    async fn apply(
        &self,
        source: &Source,
        inputs: &BTreeMap<String, polars::frame::DataFrame>,
    ) -> Result<polars::frame::DataFrame, TeckelError> {
        transforms::apply(source, inputs)
    }
}

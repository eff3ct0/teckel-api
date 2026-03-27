use async_trait::async_trait;
use std::collections::BTreeMap;
use teckel_model::source::{InputSource, OutputSource, Source};
use teckel_model::TeckelError;

/// Core trait that execution backends implement.
///
/// Designed for evolution: Phase 1 runs DataFusion in-process, but the trait
/// shape supports a future `RemoteBackend` that serializes the plan and sends
/// it to a worker service (gRPC, message queue) without changing the API.
///
/// The associated type `DataFrame` is opaque to the engine — each backend
/// defines its own handle (DataFusion's `DataFrame`, a remote `JobHandle`, etc.).
#[async_trait]
pub trait Backend: Send + Sync {
    /// The backend's representation of a dataset.
    type DataFrame: Clone + Send + Sync;

    /// Backend identifier (e.g., "datafusion", "polars", "remote").
    fn name(&self) -> &str;

    /// Read an input source into a DataFrame.
    async fn read_input(&self, input: &InputSource) -> Result<Self::DataFrame, TeckelError>;

    /// Write a DataFrame to an output destination.
    async fn write_output(
        &self,
        df: Self::DataFrame,
        output: &OutputSource,
    ) -> Result<(), TeckelError>;

    /// Apply a transformation, given the upstream DataFrames keyed by AssetRef.
    async fn apply(
        &self,
        source: &Source,
        inputs: &BTreeMap<String, Self::DataFrame>,
    ) -> Result<Self::DataFrame, TeckelError>;
}

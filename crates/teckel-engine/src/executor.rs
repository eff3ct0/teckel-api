use crate::backend::Backend;
use crate::dag::PipelineDag;
use std::collections::BTreeMap;
use teckel_model::asset::Context;
use teckel_model::source::Source;
use teckel_model::TeckelError;

/// Walks the DAG in topological order, executing each asset via the backend.
///
/// Maintains a cache of computed DataFrames so that shared dependencies
/// are only computed once (equivalent to the Scala `Mutex[DataFrame]` pattern).
pub struct PipelineExecutor<B: Backend> {
    backend: B,
}

impl<B: Backend> PipelineExecutor<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    /// Execute the full pipeline: read inputs, apply transforms, write outputs.
    pub async fn execute(&self, context: &Context) -> Result<(), TeckelError> {
        let dag = PipelineDag::from_context(context)?;
        let order = dag.topological_order()?;

        tracing::info!(
            backend = self.backend.name(),
            assets = context.len(),
            "executing pipeline"
        );

        let mut cache: BTreeMap<String, B::DataFrame> = BTreeMap::new();

        for asset_name in &order {
            let asset = &context[asset_name];

            match &asset.source {
                Source::Input(input) => {
                    tracing::debug!(asset = %asset_name, "reading input");
                    let df = self.backend.read_input(input).await?;
                    cache.insert(asset_name.clone(), df);
                }
                Source::Output(output) => {
                    tracing::debug!(asset = %asset_name, source = %output.asset_ref, "writing output");
                    let df = cache.get(&output.asset_ref).ok_or_else(|| {
                        TeckelError::Execution(format!(
                            "output \"{}\" references asset \"{}\" which has no computed result",
                            asset_name, output.asset_ref
                        ))
                    })?;
                    self.backend.write_output(df.clone(), output).await?;
                }
                source => {
                    tracing::debug!(asset = %asset_name, "applying transformation");
                    let df = self.backend.apply(source, &cache).await?;
                    cache.insert(asset_name.clone(), df);
                }
            }
        }

        tracing::info!("pipeline execution completed");
        Ok(())
    }

    /// Access the underlying backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }
}

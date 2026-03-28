use crate::backend::Backend;
use crate::dag::PipelineDag;
use std::collections::BTreeMap;
use std::sync::Arc;
use teckel_model::asset::Context;
use teckel_model::source::Source;
use teckel_model::TeckelError;
use tokio::sync::RwLock;
use tokio::task::JoinSet;

/// Walks the DAG in wave-parallel order, executing each wave concurrently.
///
/// Assets within the same wave have no mutual dependencies and are
/// executed concurrently via `tokio::spawn`. Waves are processed
/// sequentially — all assets in wave N complete before wave N+1 starts.
///
/// Maintains a shared cache of computed DataFrames so that shared
/// dependencies are only computed once.
pub struct PipelineExecutor<B: Backend + 'static> {
    backend: Arc<B>,
}

impl<B: Backend + 'static> PipelineExecutor<B> {
    pub fn new(backend: B) -> Self {
        Self {
            backend: Arc::new(backend),
        }
    }

    /// Execute the full pipeline with wave-parallel scheduling.
    pub async fn execute(&self, context: &Context) -> Result<(), TeckelError> {
        let dag = PipelineDag::from_context(context)?;
        let waves = dag.parallel_schedule()?;

        tracing::info!(
            backend = self.backend.name(),
            assets = context.len(),
            waves = waves.len(),
            "executing pipeline"
        );

        let cache: Arc<RwLock<BTreeMap<String, B::DataFrame>>> =
            Arc::new(RwLock::new(BTreeMap::new()));

        for (wave_idx, wave) in waves.iter().enumerate() {
            if wave.len() == 1 {
                // Single asset in wave — run directly without spawn overhead
                let asset_name = &wave[0];
                self.execute_asset(asset_name, context, &cache).await?;
            } else {
                tracing::debug!(
                    wave = wave_idx,
                    assets = wave.len(),
                    "executing wave concurrently"
                );

                let mut join_set = JoinSet::new();

                for asset_name in wave {
                    let name = asset_name.clone();
                    let backend = Arc::clone(&self.backend);
                    let cache = Arc::clone(&cache);
                    let context = context.clone();

                    join_set.spawn(async move {
                        execute_asset_inner(&name, &context, backend.as_ref(), &cache).await
                    });
                }

                // Wait for all tasks in this wave; fail fast on first error
                while let Some(result) = join_set.join_next().await {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            // Abort remaining tasks in this wave
                            join_set.abort_all();
                            return Err(e);
                        }
                        Err(join_err) => {
                            join_set.abort_all();
                            return Err(TeckelError::Execution(format!(
                                "task panicked: {join_err}"
                            )));
                        }
                    }
                }
            }
        }

        tracing::info!("pipeline execution completed");
        Ok(())
    }

    /// Execute a single asset (used for single-asset waves to avoid spawn overhead).
    async fn execute_asset(
        &self,
        asset_name: &str,
        context: &Context,
        cache: &Arc<RwLock<BTreeMap<String, B::DataFrame>>>,
    ) -> Result<(), TeckelError> {
        execute_asset_inner(asset_name, context, self.backend.as_ref(), cache).await
    }

    /// Access the underlying backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }
}

/// Inner function that can be called from both direct execution and spawned tasks.
async fn execute_asset_inner<B: Backend>(
    asset_name: &str,
    context: &Context,
    backend: &B,
    cache: &Arc<RwLock<BTreeMap<String, B::DataFrame>>>,
) -> Result<(), TeckelError> {
    let asset = context.get(asset_name).ok_or_else(|| {
        TeckelError::Execution(format!("asset \"{asset_name}\" not found in context"))
    })?;

    match &asset.source {
        Source::Input(input) => {
            tracing::debug!(asset = %asset_name, "reading input");
            let df = backend.read_input(input).await?;
            cache.write().await.insert(asset_name.to_string(), df);
        }
        Source::Output(output) => {
            tracing::debug!(asset = %asset_name, source = %output.asset_ref, "writing output");
            let df = {
                let c = cache.read().await;
                c.get(&output.asset_ref)
                    .cloned()
                    .ok_or_else(|| {
                        TeckelError::Execution(format!(
                            "output \"{}\" references asset \"{}\" which has no computed result",
                            asset_name, output.asset_ref
                        ))
                    })?
            };
            backend.write_output(df, output).await?;
        }
        source => {
            tracing::debug!(asset = %asset_name, "applying transformation");
            let inputs = cache.read().await.clone();
            let df = backend.apply(source, &inputs).await?;
            cache.write().await.insert(asset_name.to_string(), df);
        }
    }

    Ok(())
}

use std::collections::BTreeMap;
use teckel_datafusion::DataFusionBackend;
use teckel_engine::backend::Backend;
use teckel_engine::dry_run;
use teckel_engine::executor::PipelineExecutor;
use teckel_model::TeckelError;

/// Parse a Teckel YAML pipeline and execute it with the default DataFusion backend.
///
/// ```ignore
/// teckel_api::etl(yaml, &BTreeMap::new()).await?;
/// ```
pub async fn etl(yaml: &str, variables: &BTreeMap<String, String>) -> Result<(), TeckelError> {
    etl_with(yaml, variables, DataFusionBackend::new()).await
}

/// Parse a Teckel YAML pipeline and execute it with a custom backend.
///
/// This is the extension point: pass a `DataFusionBackend` for local execution,
/// or a future `RemoteBackend` for distributed execution via gRPC/K8s workers.
pub async fn etl_with<B: Backend + 'static>(
    yaml: &str,
    variables: &BTreeMap<String, String>,
    backend: B,
) -> Result<(), TeckelError> {
    let pipeline = teckel_parser::parse(yaml, variables)?;

    // Pre-execution hooks (§16)
    teckel_engine::hooks::run_pre_hooks(&pipeline.hooks, None).await?;

    let result = {
        let executor = PipelineExecutor::new(backend);
        executor.execute(&pipeline.context).await
    };

    // Post-execution hooks (§16) — always run, regardless of pipeline result
    let status = if result.is_ok() { "completed" } else { "failed" };
    teckel_engine::hooks::run_post_hooks(&pipeline.hooks, status, None).await;

    // Quality suites (§17) — run after successful execution
    if result.is_ok() && !pipeline.quality.is_empty() {
        tracing::info!(suites = pipeline.quality.len(), "running quality suites");
        // Quality checks require a query function — for now, log that suites
        // are defined. Full DataFusion integration requires registering
        // computed DataFrames as views and running SQL queries against them.
        tracing::info!("quality suite execution requires backend-specific query function");
    }

    result
}

/// Parse a Teckel YAML pipeline and return a human-readable execution plan.
///
/// Does not execute anything — useful for debugging and visualization.
pub fn explain(yaml: &str, variables: &BTreeMap<String, String>) -> Result<String, TeckelError> {
    let pipeline = teckel_parser::parse(yaml, variables)?;
    dry_run::explain(&pipeline.context)
}

/// Parse and validate a Teckel YAML pipeline without executing.
///
/// Returns `Ok(())` if the pipeline is valid, or a validation error.
pub fn validate(yaml: &str, variables: &BTreeMap<String, String>) -> Result<(), TeckelError> {
    let _pipeline = teckel_parser::parse(yaml, variables)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_valid_pipeline() {
        let yaml = r#"
version: "2.0"
input:
  - name: src
    format: csv
    path: "data.csv"
    options:
      header: true
transformation:
  - name: filtered
    where:
      from: src
      filter: "x > 0"
output:
  - name: filtered
    format: parquet
    path: "output/"
    mode: overwrite
"#;
        assert!(validate(yaml, &BTreeMap::new()).is_ok());
    }

    #[test]
    fn validate_rejects_invalid() {
        let yaml = r#"
version: "2.0"
input:
  - name: src
    format: csv
    path: "data.csv"
transformation:
  - name: bad
    where:
      from: nonexistent
      filter: "x > 0"
output:
  - name: bad
    format: csv
    path: "out.csv"
"#;
        assert!(validate(yaml, &BTreeMap::new()).is_err());
    }

    #[test]
    fn explain_produces_plan() {
        let yaml = r#"
version: "2.0"
input:
  - name: orders
    format: csv
    path: "orders.csv"
  - name: customers
    format: csv
    path: "customers.csv"
transformation:
  - name: enriched
    join:
      left: orders
      right:
        - name: customers
          type: inner
          on: ["orders.id = customers.id"]
output:
  - name: enriched
    format: parquet
    path: "output/enriched"
    mode: overwrite
"#;
        let plan = explain(yaml, &BTreeMap::new()).unwrap();
        assert!(plan.contains("Inputs"));
        assert!(plan.contains("Transformations"));
        assert!(plan.contains("Outputs"));
        assert!(plan.contains("JOIN"));
        assert!(plan.contains("2 inputs"));
    }
}

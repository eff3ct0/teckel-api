use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use teckel_model::source::OutputSource;
use teckel_model::types::WriteMode;
use teckel_model::TeckelError;

pub async fn write_output(df: DataFrame, output: &OutputSource) -> Result<(), TeckelError> {
    let path = &output.path;

    match output.mode {
        WriteMode::Error => {
            if std::path::Path::new(path).exists() {
                return Err(TeckelError::spec(
                    teckel_model::TeckelErrorCode::EIo002,
                    format!("output destination \"{path}\" already exists (mode=error)"),
                ));
            }
        }
        WriteMode::Overwrite => {
            if std::path::Path::new(path).exists() {
                std::fs::remove_dir_all(path)
                    .or_else(|_| std::fs::remove_file(path))
                    .map_err(|e| {
                        TeckelError::Execution(format!(
                            "failed to remove existing output \"{path}\": {e}"
                        ))
                    })?;
            }
        }
        WriteMode::Ignore => {
            if std::path::Path::new(path).exists() {
                tracing::info!(path, "output already exists, skipping (mode=ignore)");
                return Ok(());
            }
        }
        WriteMode::Append => {
            tracing::warn!("append mode not fully supported for file outputs; writing as new");
        }
    }

    let write_opts = DataFrameWriteOptions::new();

    match output.format.as_str() {
        "parquet" => {
            df.write_parquet(path, write_opts, None)
                .await
                .map_err(|e| {
                    TeckelError::Execution(format!("failed to write Parquet \"{path}\": {e}"))
                })?;
        }
        "csv" => {
            df.write_csv(path, write_opts, None).await.map_err(|e| {
                TeckelError::Execution(format!("failed to write CSV \"{path}\": {e}"))
            })?;
        }
        "json" | "ndjson" => {
            df.write_json(path, write_opts, None).await.map_err(|e| {
                TeckelError::Execution(format!("failed to write JSON \"{path}\": {e}"))
            })?;
        }
        other => {
            return Err(TeckelError::spec(
                teckel_model::TeckelErrorCode::EFmt001,
                format!("unsupported output format \"{other}\""),
            ));
        }
    }

    Ok(())
}

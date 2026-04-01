use polars::prelude::*;
use std::path::Path;
use teckel_model::source::OutputSource;
use teckel_model::types::WriteMode;
use teckel_model::TeckelError;

pub fn write_output(mut df: DataFrame, output: &OutputSource) -> Result<(), TeckelError> {
    let path = Path::new(&output.path);

    match output.mode {
        WriteMode::Error => {
            if path.exists() {
                return Err(TeckelError::spec(
                    teckel_model::TeckelErrorCode::EIo002,
                    format!(
                        "output path \"{}\" already exists (mode: error)",
                        output.path
                    ),
                ));
            }
        }
        WriteMode::Overwrite => {
            if path.exists() {
                if path.is_dir() {
                    std::fs::remove_dir_all(path)
                        .map_err(|e| TeckelError::Execution(format!("remove dir: {e}")))?;
                } else {
                    std::fs::remove_file(path)
                        .map_err(|e| TeckelError::Execution(format!("remove file: {e}")))?;
                }
            }
        }
        WriteMode::Ignore => {
            if path.exists() {
                tracing::info!(path = %output.path, "output exists, skipping (mode: ignore)");
                return Ok(());
            }
        }
        WriteMode::Append => {
            tracing::warn!("append mode not fully supported for file outputs in polars backend");
        }
    }

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| TeckelError::Execution(format!("create dir: {e}")))?;
    }

    match output.format.as_str() {
        "csv" => {
            let file = std::fs::File::create(path)
                .map_err(|e| TeckelError::Execution(format!("create csv: {e}")))?;
            CsvWriter::new(file)
                .include_header(true)
                .finish(&mut df)
                .map_err(|e| TeckelError::Execution(format!("polars csv write: {e}")))
        }
        "parquet" => {
            let file = std::fs::File::create(path)
                .map_err(|e| TeckelError::Execution(format!("create parquet: {e}")))?;
            ParquetWriter::new(file)
                .finish(&mut df)
                .map(|_| ())
                .map_err(|e| TeckelError::Execution(format!("polars parquet write: {e}")))
        }
        "json" | "ndjson" => {
            let file = std::fs::File::create(path)
                .map_err(|e| TeckelError::Execution(format!("create json: {e}")))?;
            JsonWriter::new(file)
                .finish(&mut df)
                .map_err(|e| TeckelError::Execution(format!("polars json write: {e}")))
        }
        other => Err(TeckelError::spec(
            teckel_model::TeckelErrorCode::EFmt001,
            format!("unsupported output format \"{other}\" for polars backend"),
        )),
    }
}

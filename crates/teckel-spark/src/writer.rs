use spark_connect_rs::dataframe::{DataFrame, SaveMode};
use teckel_model::source::OutputSource;
use teckel_model::types::{Primitive, WriteMode};
use teckel_model::TeckelError;

fn primitive_to_string(p: &Primitive) -> String {
    match p {
        Primitive::Bool(v) => v.to_string(),
        Primitive::Int(v) => v.to_string(),
        Primitive::Float(v) => v.to_string(),
        Primitive::String(v) => v.clone(),
    }
}

pub async fn write_output(df: DataFrame, output: &OutputSource) -> Result<(), TeckelError> {
    let mode = match output.mode {
        WriteMode::Error => SaveMode::ErrorIfExists,
        WriteMode::Overwrite => SaveMode::Overwrite,
        WriteMode::Append => SaveMode::Append,
        WriteMode::Ignore => SaveMode::Ignore,
    };

    let format = output.format.as_str();
    let path = output.path.as_str();

    let mut writer = df.write().format(format).mode(mode);

    for (key, value) in &output.options {
        writer = writer.option(key, &primitive_to_string(value));
    }

    writer
        .save(path)
        .await
        .map_err(|e| TeckelError::Execution(format!("write {format} to {path}: {e}")))
}

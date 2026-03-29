use spark_connect_rs::dataframe::DataFrame;
use spark_connect_rs::SparkSession;
use teckel_model::source::InputSource;
use teckel_model::types::Primitive;
use teckel_model::TeckelError;

fn primitive_to_string(p: &Primitive) -> String {
    match p {
        Primitive::Bool(v) => v.to_string(),
        Primitive::Int(v) => v.to_string(),
        Primitive::Float(v) => v.to_string(),
        Primitive::String(v) => v.clone(),
    }
}

pub async fn read_input(
    session: &SparkSession,
    input: &InputSource,
) -> Result<DataFrame, TeckelError> {
    let format = input.format.as_str();
    let path = input.path.as_str();

    let mut reader = session.read().format(format);

    for (key, value) in &input.options {
        reader = reader.option(key, &primitive_to_string(value));
    }

    // Default header=true for CSV (teckel convention)
    if format == "csv" && !input.options.contains_key("header") {
        reader = reader.option("header", "true");
    }

    reader
        .load([path])
        .map_err(|e| TeckelError::Execution(format!("read {format} from {path}: {e}")))
}

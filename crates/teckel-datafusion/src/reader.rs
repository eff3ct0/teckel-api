use datafusion::prelude::*;
use teckel_model::source::InputSource;
use teckel_model::types::Primitive;
use teckel_model::TeckelError;

pub async fn read_input(
    ctx: &SessionContext,
    input: &InputSource,
) -> Result<DataFrame, TeckelError> {
    let path = &input.path;

    let has_header = input
        .options
        .get("header")
        .map(|v| matches!(v, Primitive::Bool(true)))
        .unwrap_or(false);

    let delimiter = input
        .options
        .get("sep")
        .and_then(|v| match v {
            Primitive::String(s) => s.as_bytes().first().copied(),
            _ => None,
        })
        .unwrap_or(b',');

    match input.format.as_str() {
        "csv" => {
            let options = CsvReadOptions::new()
                .has_header(has_header)
                .delimiter(delimiter);
            ctx.read_csv(path, options)
                .await
                .map_err(|e| TeckelError::Execution(format!("failed to read CSV \"{path}\": {e}")))
        }
        "parquet" => ctx
            .read_parquet(path, ParquetReadOptions::default())
            .await
            .map_err(|e| TeckelError::Execution(format!("failed to read Parquet \"{path}\": {e}"))),
        "json" | "ndjson" => ctx
            .read_json(path, NdJsonReadOptions::default())
            .await
            .map_err(|e| TeckelError::Execution(format!("failed to read JSON \"{path}\": {e}"))),
        other => Err(TeckelError::spec(
            teckel_model::TeckelErrorCode::EFmt001,
            format!("unsupported input format \"{other}\""),
        )),
    }
}

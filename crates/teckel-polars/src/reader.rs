use polars::prelude::*;
use teckel_model::source::InputSource;
use teckel_model::TeckelError;

pub fn read_input(input: &InputSource) -> Result<DataFrame, TeckelError> {
    match input.format.as_str() {
        "csv" => {
            let mut reader = CsvReadOptions::default();

            let has_header = input
                .options
                .get("header")
                .map(|v| format!("{v:?}").contains("true"))
                .unwrap_or(false);
            reader = reader.with_has_header(has_header);

            if let Some(sep) = input.options.get("sep") {
                let sep_char = format!("{sep:?}")
                    .trim_matches('"')
                    .chars()
                    .next()
                    .unwrap_or(',');
                reader = reader.with_parse_options(
                    CsvParseOptions::default().with_separator(sep_char as u8),
                );
            }

            reader
                .try_into_reader_with_file_path(Some(input.path.clone().into()))
                .map_err(|e| TeckelError::Execution(format!("polars csv reader init: {e}")))?
                .finish()
                .map_err(|e| TeckelError::Execution(format!("polars csv read: {e}")))
        }
        "parquet" => ParquetReader::new(
            std::fs::File::open(&input.path)
                .map_err(|e| TeckelError::Execution(format!("parquet open: {e}")))?,
        )
        .finish()
        .map_err(|e| TeckelError::Execution(format!("polars parquet read: {e}"))),
        "json" | "ndjson" => JsonReader::new(
            std::fs::File::open(&input.path)
                .map_err(|e| TeckelError::Execution(format!("json open: {e}")))?,
        )
        .finish()
        .map_err(|e| TeckelError::Execution(format!("polars json read: {e}"))),
        other => Err(TeckelError::spec(
            teckel_model::TeckelErrorCode::EFmt001,
            format!("unsupported format \"{other}\" for polars backend"),
        )),
    }
}

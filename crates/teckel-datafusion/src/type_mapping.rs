use arrow::datatypes::DataType as ArrowDataType;
use teckel_model::types::TeckelDataType;

#[allow(dead_code)]
pub fn teckel_to_arrow(dt: &TeckelDataType) -> ArrowDataType {
    match dt {
        TeckelDataType::String => ArrowDataType::Utf8,
        TeckelDataType::Integer => ArrowDataType::Int32,
        TeckelDataType::Long => ArrowDataType::Int64,
        TeckelDataType::Float => ArrowDataType::Float32,
        TeckelDataType::Double => ArrowDataType::Float64,
        TeckelDataType::Boolean => ArrowDataType::Boolean,
        TeckelDataType::Date => ArrowDataType::Date32,
        TeckelDataType::Timestamp => {
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        TeckelDataType::Binary => ArrowDataType::Binary,
        TeckelDataType::Decimal { precision, scale } => {
            ArrowDataType::Decimal128(*precision, *scale as i8)
        }
        TeckelDataType::Array(inner) => {
            let field = arrow::datatypes::Field::new("item", teckel_to_arrow(inner), true);
            ArrowDataType::List(std::sync::Arc::new(field))
        }
        TeckelDataType::Map(key, value) => {
            let entries = arrow::datatypes::Field::new(
                "entries",
                ArrowDataType::Struct(
                    vec![
                        arrow::datatypes::Field::new("key", teckel_to_arrow(key), false),
                        arrow::datatypes::Field::new("value", teckel_to_arrow(value), true),
                    ]
                    .into(),
                ),
                false,
            );
            ArrowDataType::Map(std::sync::Arc::new(entries), false)
        }
        TeckelDataType::Struct(fields) => {
            let arrow_fields: Vec<arrow::datatypes::Field> = fields
                .iter()
                .map(|f| {
                    arrow::datatypes::Field::new(&f.name, teckel_to_arrow(&f.data_type), f.nullable)
                })
                .collect();
            ArrowDataType::Struct(arrow_fields.into())
        }
    }
}

#[allow(dead_code)]
pub fn arrow_to_teckel(dt: &ArrowDataType) -> TeckelDataType {
    match dt {
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => TeckelDataType::String,
        ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32 => {
            TeckelDataType::Integer
        }
        ArrowDataType::Int64 => TeckelDataType::Long,
        ArrowDataType::Float32 => TeckelDataType::Float,
        ArrowDataType::Float64 => TeckelDataType::Double,
        ArrowDataType::Boolean => TeckelDataType::Boolean,
        ArrowDataType::Date32 | ArrowDataType::Date64 => TeckelDataType::Date,
        ArrowDataType::Timestamp(_, _) => TeckelDataType::Timestamp,
        ArrowDataType::Binary | ArrowDataType::LargeBinary => TeckelDataType::Binary,
        ArrowDataType::Decimal128(p, s) => TeckelDataType::Decimal {
            precision: *p,
            scale: *s as u8,
        },
        _ => TeckelDataType::String, // fallback
    }
}

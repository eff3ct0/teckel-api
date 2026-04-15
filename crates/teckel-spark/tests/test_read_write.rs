#![cfg(feature = "integration")]

mod common;

use std::collections::BTreeMap;
use teckel_engine::Backend;
use teckel_model::source::{InputSource, OutputSource};
use teckel_model::types::WriteMode;

#[tokio::test]
async fn read_csv() {
    let backend = common::backend().await;
    let input = common::employees_input();

    let df = backend.read_input(&input).await.unwrap();
    let count = df.count().await.unwrap();
    assert_eq!(count, 10, "employees.csv should have 10 rows");
}

#[tokio::test]
async fn read_csv_with_schema_inference() {
    let backend = common::backend().await;
    let input = common::employees_input();

    let df = backend.read_input(&input).await.unwrap();
    let cols = df.columns().await.unwrap();
    assert_eq!(cols, vec!["id", "name", "department", "salary"]);
}

#[tokio::test]
async fn write_parquet_and_read_back() {
    let backend = common::backend().await;
    let input = common::employees_input();
    let df = backend.read_input(&input).await.unwrap();

    let output = OutputSource {
        asset_ref: "employees_parquet".to_string(),
        format: "parquet".to_string(),
        path: "/tmp/teckel_integration/employees_parquet".to_string(),
        mode: WriteMode::Overwrite,
        options: BTreeMap::new(),
    };
    backend.write_output(df, &output).await.unwrap();

    // Read back the parquet output
    let read_back = InputSource {
        format: "parquet".to_string(),
        path: "/tmp/teckel_integration/employees_parquet".to_string(),
        options: BTreeMap::new(),
    };
    let df2 = backend.read_input(&read_back).await.unwrap();
    let count = df2.count().await.unwrap();
    assert_eq!(count, 10, "parquet read-back should have 10 rows");
}

#[tokio::test]
async fn write_json_and_read_back() {
    let backend = common::backend().await;
    let input = common::employees_input();
    let df = backend.read_input(&input).await.unwrap();

    let output = OutputSource {
        asset_ref: "employees_json".to_string(),
        format: "json".to_string(),
        path: "/tmp/teckel_integration/employees_json".to_string(),
        mode: WriteMode::Overwrite,
        options: BTreeMap::new(),
    };
    backend.write_output(df, &output).await.unwrap();

    let read_back = InputSource {
        format: "json".to_string(),
        path: "/tmp/teckel_integration/employees_json".to_string(),
        options: BTreeMap::new(),
    };
    let df2 = backend.read_input(&read_back).await.unwrap();
    let count = df2.count().await.unwrap();
    assert_eq!(count, 10, "json read-back should have 10 rows");
}

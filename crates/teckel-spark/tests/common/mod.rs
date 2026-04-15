#![cfg(feature = "integration")]

use spark_connect_rs::dataframe::DataFrame;
use std::collections::BTreeMap;
use teckel_engine::Backend;
use teckel_model::source::InputSource;
use teckel_model::types::Primitive;
use teckel_spark::SparkConnectBackend;

/// Read the Spark Connect URL from env, defaulting to local.
pub fn connect_url() -> String {
    std::env::var("SPARK_CONNECT_URL").unwrap_or_else(|_| "sc://127.0.0.1:15002/".to_string())
}

/// Read the test data directory from env.
pub fn test_data_dir() -> String {
    std::env::var("TEST_DATA_DIR").unwrap_or_else(|_| "/data".to_string())
}

/// Create a connected backend, panicking on failure.
pub async fn backend() -> SparkConnectBackend {
    SparkConnectBackend::new(&connect_url())
        .await
        .expect("Failed to connect to Spark Connect server")
}

/// Build an InputSource for employees.csv.
pub fn employees_input() -> InputSource {
    InputSource {
        format: "csv".to_string(),
        path: format!("{}/employees.csv", test_data_dir()),
        options: BTreeMap::from([
            ("header".to_string(), Primitive::String("true".to_string())),
            (
                "inferSchema".to_string(),
                Primitive::String("true".to_string()),
            ),
        ]),
    }
}

/// Build an InputSource for departments.csv.
pub fn departments_input() -> InputSource {
    InputSource {
        format: "csv".to_string(),
        path: format!("{}/departments.csv", test_data_dir()),
        options: BTreeMap::from([
            ("header".to_string(), Primitive::String("true".to_string())),
            (
                "inferSchema".to_string(),
                Primitive::String("true".to_string()),
            ),
        ]),
    }
}

/// Read an input and insert it into a cache map.
pub async fn read_to_cache(
    backend: &SparkConnectBackend,
    name: &str,
    input: &InputSource,
) -> BTreeMap<String, DataFrame> {
    let df = backend.read_input(input).await.expect("read_input failed");
    let mut cache = BTreeMap::new();
    cache.insert(name.to_string(), df);
    cache
}

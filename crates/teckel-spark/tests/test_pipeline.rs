#![cfg(feature = "integration")]

mod common;

use std::collections::BTreeMap;
use teckel_engine::Backend;
use teckel_model::source::InputSource;

#[tokio::test]
async fn pipeline_filter_and_write() {
    let data_dir = common::test_data_dir();

    let yaml = format!(
        r#"
version: "2.0"
input:
  - name: employees
    format: csv
    path: "{data_dir}/employees.csv"
    options:
      header: true
      inferSchema: true
transformation:
  - name: high_earners
    where:
      from: employees
      filter: "salary > 100000"
  - name: summary
    select:
      from: high_earners
      columns:
        - name
        - salary
output:
  - name: summary
    format: parquet
    path: "/tmp/teckel_integration/pipeline_filter"
    mode: overwrite
"#
    );

    let backend = common::backend().await;
    teckel_api::etl_with(&yaml, &BTreeMap::new(), backend)
        .await
        .expect("pipeline_filter_and_write should succeed");

    // Verify output
    let verify = common::backend().await;
    let input = InputSource {
        format: "parquet".to_string(),
        path: "/tmp/teckel_integration/pipeline_filter".to_string(),
        options: BTreeMap::new(),
    };
    let df = verify.read_input(&input).await.unwrap();
    let count = df.count().await.unwrap();
    assert_eq!(count, 4, "4 employees with salary > 100k");
}

#[tokio::test]
async fn pipeline_join_and_group() {
    let data_dir = common::test_data_dir();

    let yaml = format!(
        r#"
version: "2.0"
input:
  - name: employees
    format: csv
    path: "{data_dir}/employees.csv"
    options:
      header: true
      inferSchema: true
  - name: departments
    format: csv
    path: "{data_dir}/departments.csv"
    options:
      header: true
      inferSchema: true
transformation:
  - name: enriched
    join:
      left: employees
      right:
        - name: departments
          type: inner
          on: ["employees.department = departments.id"]
  - name: dept_stats
    groupBy:
      from: enriched
      by: [department]
      agg: ["COUNT(*) AS headcount", "AVG(salary) AS avg_salary"]
output:
  - name: dept_stats
    format: parquet
    path: "/tmp/teckel_integration/pipeline_join_group"
    mode: overwrite
"#
    );

    let backend = common::backend().await;
    teckel_api::etl_with(&yaml, &BTreeMap::new(), backend)
        .await
        .expect("pipeline_join_and_group should succeed");

    // Verify output
    let verify = common::backend().await;
    let input = InputSource {
        format: "parquet".to_string(),
        path: "/tmp/teckel_integration/pipeline_join_group".to_string(),
        options: BTreeMap::new(),
    };
    let df = verify.read_input(&input).await.unwrap();
    let count = df.count().await.unwrap();
    assert_eq!(count, 3, "3 department stats rows");
}

#[tokio::test]
async fn pipeline_multi_transform() {
    let data_dir = common::test_data_dir();

    let yaml = format!(
        r#"
version: "2.0"
input:
  - name: employees
    format: csv
    path: "{data_dir}/employees.csv"
    options:
      header: true
      inferSchema: true
transformation:
  - name: with_bonus
    addColumns:
      from: employees
      columns:
        - name: bonus
          expression: "salary * 0.1"
  - name: sorted
    orderBy:
      from: with_bonus
      columns:
        - column: salary
          direction: desc
          nulls: last
  - name: top5
    limit:
      from: sorted
      count: 5
output:
  - name: top5
    format: parquet
    path: "/tmp/teckel_integration/pipeline_multi"
    mode: overwrite
"#
    );

    let backend = common::backend().await;
    teckel_api::etl_with(&yaml, &BTreeMap::new(), backend)
        .await
        .expect("pipeline_multi_transform should succeed");

    // Verify output
    let verify = common::backend().await;
    let input = InputSource {
        format: "parquet".to_string(),
        path: "/tmp/teckel_integration/pipeline_multi".to_string(),
        options: BTreeMap::new(),
    };
    let df = verify.read_input(&input).await.unwrap();
    let count = df.clone().count().await.unwrap();
    assert_eq!(count, 5, "top 5 employees");

    let cols = df.columns().await.unwrap();
    assert!(
        cols.contains(&"bonus".to_string()),
        "bonus column should exist"
    );
}

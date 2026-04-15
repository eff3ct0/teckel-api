#![cfg(feature = "integration")]

mod common;

use std::collections::BTreeMap;
use teckel_engine::Backend;
use teckel_model::source::*;
use teckel_model::types::*;

// ── Helpers ─────────────────────────────────────────────────────

async fn employees_cache() -> (
    teckel_spark::SparkConnectBackend,
    BTreeMap<String, spark_connect_rs::dataframe::DataFrame>,
) {
    let backend = common::backend().await;
    let cache = common::read_to_cache(&backend, "employees", &common::employees_input()).await;
    (backend, cache)
}

async fn two_table_cache() -> (
    teckel_spark::SparkConnectBackend,
    BTreeMap<String, spark_connect_rs::dataframe::DataFrame>,
) {
    let backend = common::backend().await;
    let mut cache = common::read_to_cache(&backend, "employees", &common::employees_input()).await;
    let dept_df = backend
        .read_input(&common::departments_input())
        .await
        .expect("read departments");
    cache.insert("departments".to_string(), dept_df);
    (backend, cache)
}

// ── Select ──────────────────────────────────────────────────────

#[tokio::test]
async fn transform_select() {
    let (backend, cache) = employees_cache().await;
    let source = Source::Select(SelectTransform {
        from: "employees".to_string(),
        columns: vec!["name".to_string(), "salary".to_string()],
    });
    let result = backend.apply(&source, &cache).await.unwrap();
    let cols = result.columns().await.unwrap();
    assert_eq!(cols, vec!["name", "salary"]);
}

// ── Where ───────────────────────────────────────────────────────

#[tokio::test]
async fn transform_where() {
    let (backend, cache) = employees_cache().await;
    let source = Source::Where(WhereTransform {
        from: "employees".to_string(),
        filter: "salary > 100000".to_string(),
    });
    let result = backend.apply(&source, &cache).await.unwrap();
    // Alice(120k), Eve(105k), Frank(130k), Ivan(115k) = 4
    let count = result.count().await.unwrap();
    assert_eq!(count, 4);
}

// ── GroupBy ─────────────────────────────────────────────────────

#[tokio::test]
async fn transform_group_by() {
    let (backend, cache) = employees_cache().await;
    let source = Source::GroupBy(GroupByTransform {
        from: "employees".to_string(),
        by: vec!["department".to_string()],
        agg: vec!["COUNT(*) AS headcount".to_string()],
    });
    let result = backend.apply(&source, &cache).await.unwrap();
    // 3 departments: Engineering, Sales, Marketing
    let count = result.count().await.unwrap();
    assert_eq!(count, 3);
}

// ── OrderBy ─────────────────────────────────────────────────────

#[tokio::test]
async fn transform_order_by() {
    let (backend, cache) = employees_cache().await;
    let source = Source::OrderBy(OrderByTransform {
        from: "employees".to_string(),
        columns: vec![SortColumn::Explicit {
            column: "salary".to_string(),
            direction: SortDirection::Desc,
            nulls: NullOrdering::Last,
        }],
    });
    let result = backend.apply(&source, &cache).await.unwrap();
    // First row should be Frank with salary 130000
    let first = result.head(Some(1)).await.unwrap();
    assert_eq!(first.num_rows(), 1);
}

// ── Join ────────────────────────────────────────────────────────

#[tokio::test]
async fn transform_join() {
    let (backend, cache) = two_table_cache().await;
    let source = Source::Join(JoinTransform {
        left: "employees".to_string(),
        right: vec![JoinTarget {
            name: "departments".to_string(),
            join_type: JoinType::Inner,
            on: vec!["employees.department = departments.id".to_string()],
        }],
    });
    let result = backend.apply(&source, &cache).await.unwrap();
    // All 10 employees should match (every department exists)
    let count = result.clone().count().await.unwrap();
    assert_eq!(count, 10);
    // Should have columns from both tables
    let cols = result.columns().await.unwrap();
    assert!(cols.contains(&"dept_name".to_string()));
    assert!(cols.contains(&"location".to_string()));
}

// ── Union ───────────────────────────────────────────────────────

#[tokio::test]
async fn transform_union() {
    let (backend, cache) = employees_cache().await;
    let source = Source::Union(UnionTransform {
        sources: vec!["employees".to_string(), "employees".to_string()],
        all: true,
        by_name: false,
        allow_missing_columns: false,
    });
    let result = backend.apply(&source, &cache).await.unwrap();
    let count = result.count().await.unwrap();
    assert_eq!(count, 20, "union all should double the rows");
}

// ── Distinct ────────────────────────────────────────────────────

#[tokio::test]
async fn transform_distinct() {
    let (backend, cache) = employees_cache().await;
    // First select just department column, then distinct
    let select_source = Source::Select(SelectTransform {
        from: "employees".to_string(),
        columns: vec!["department".to_string()],
    });
    let dept_df = backend.apply(&select_source, &cache).await.unwrap();
    let mut dept_cache = BTreeMap::new();
    dept_cache.insert("depts".to_string(), dept_df);

    let source = Source::Distinct(DistinctTransform {
        from: "depts".to_string(),
        columns: None,
    });
    let result = backend.apply(&source, &dept_cache).await.unwrap();
    let count = result.count().await.unwrap();
    assert_eq!(count, 3, "3 distinct departments");
}

// ── Limit ───────────────────────────────────────────────────────

#[tokio::test]
async fn transform_limit() {
    let (backend, cache) = employees_cache().await;
    let source = Source::Limit(LimitTransform {
        from: "employees".to_string(),
        count: 5,
    });
    let result = backend.apply(&source, &cache).await.unwrap();
    let count = result.count().await.unwrap();
    assert_eq!(count, 5);
}

// ── AddColumns ──────────────────────────────────────────────────

#[tokio::test]
async fn transform_add_columns() {
    let (backend, cache) = employees_cache().await;
    let source = Source::AddColumns(AddColumnsTransform {
        from: "employees".to_string(),
        columns: vec![ColumnDef {
            name: "salary_k".to_string(),
            expression: "salary / 1000".to_string(),
        }],
    });
    let result = backend.apply(&source, &cache).await.unwrap();
    let cols = result.clone().columns().await.unwrap();
    assert!(cols.contains(&"salary_k".to_string()));
    let count = result.count().await.unwrap();
    assert_eq!(count, 10);
}

// ── DropColumns ─────────────────────────────────────────────────

#[tokio::test]
async fn transform_drop_columns() {
    let (backend, cache) = employees_cache().await;
    let source = Source::DropColumns(DropColumnsTransform {
        from: "employees".to_string(),
        columns: vec!["salary".to_string()],
    });
    let result = backend.apply(&source, &cache).await.unwrap();
    let cols = result.columns().await.unwrap();
    assert!(!cols.contains(&"salary".to_string()));
    assert!(cols.contains(&"name".to_string()));
}

// ── RenameColumns ───────────────────────────────────────────────

#[tokio::test]
async fn transform_rename_columns() {
    let (backend, cache) = employees_cache().await;
    let source = Source::RenameColumns(RenameColumnsTransform {
        from: "employees".to_string(),
        mappings: BTreeMap::from([("name".to_string(), "employee_name".to_string())]),
    });
    let result = backend.apply(&source, &cache).await.unwrap();
    let cols = result.columns().await.unwrap();
    assert!(cols.contains(&"employee_name".to_string()));
    assert!(!cols.contains(&"name".to_string()));
}

// ── Sql ─────────────────────────────────────────────────────────

#[tokio::test]
async fn transform_sql() {
    let (backend, cache) = employees_cache().await;
    let source = Source::Sql(SqlTransform {
        query: "SELECT department, AVG(salary) AS avg_salary FROM employees GROUP BY department"
            .to_string(),
        views: vec!["employees".to_string()],
    });
    let result = backend.apply(&source, &cache).await.unwrap();
    let count = result.count().await.unwrap();
    assert_eq!(count, 3, "3 departments in SQL result");
}

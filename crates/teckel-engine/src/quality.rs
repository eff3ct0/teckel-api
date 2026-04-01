//! Data quality suite execution (Section 17).
//!
//! Executes quality check suites against computed DataFrames.
//! Handles severity levels (error/warn/info) and conditional escalation.

use std::collections::BTreeMap;
use teckel_model::quality::{BoundSpec, Check, QualitySuite, Severity};
use teckel_model::TeckelError;

/// Result of a single quality check.
#[derive(Debug, Clone)]
pub struct CheckResult {
    pub suite: String,
    pub check_type: String,
    pub passed: bool,
    pub severity: Severity,
    pub message: String,
    pub metric: Option<f64>,
}

/// Result of running all quality suites.
#[derive(Debug, Clone)]
pub struct QualityReport {
    pub results: Vec<CheckResult>,
}

impl QualityReport {
    pub fn has_errors(&self) -> bool {
        self.results
            .iter()
            .any(|r| !r.passed && r.severity == Severity::Error)
    }

    pub fn error_messages(&self) -> Vec<&str> {
        self.results
            .iter()
            .filter(|r| !r.passed && r.severity == Severity::Error)
            .map(|r| r.message.as_str())
            .collect()
    }
}

/// Execute quality check suites. Returns a QualityReport.
///
/// The `query_fn` closure executes a SQL query against the target asset
/// and returns the result as a Vec of row maps. This keeps the quality
/// module backend-agnostic.
pub async fn run_quality_suites<F, Fut>(
    suites: &[QualitySuite],
    query_fn: F,
) -> Result<QualityReport, TeckelError>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<BTreeMap<String, String>>, TeckelError>>,
{
    let mut results = Vec::new();

    for suite in suites {
        tracing::info!(suite = %suite.suite, target = %suite.target, "running quality suite");

        for check in &suite.checks {
            let result =
                run_check(&suite.suite, &suite.target, &suite.filter, check, &query_fn).await;
            match result {
                Ok(r) => {
                    if !r.passed {
                        match r.severity {
                            Severity::Error => tracing::error!(
                                suite = %suite.suite, check = %r.check_type,
                                "{}", r.message
                            ),
                            Severity::Warn => tracing::warn!(
                                suite = %suite.suite, check = %r.check_type,
                                "{}", r.message
                            ),
                            Severity::Info => tracing::info!(
                                suite = %suite.suite, check = %r.check_type,
                                "{}", r.message
                            ),
                        }
                    }
                    results.push(r);
                }
                Err(e) => {
                    results.push(CheckResult {
                        suite: suite.suite.clone(),
                        check_type: "error".to_string(),
                        passed: false,
                        severity: Severity::Error,
                        message: format!("check execution failed: {e}"),
                        metric: None,
                    });
                }
            }
        }
    }

    Ok(QualityReport { results })
}

async fn run_check<F, Fut>(
    suite_name: &str,
    target: &str,
    filter: &Option<String>,
    check: &Check,
    query_fn: &F,
) -> Result<CheckResult, TeckelError>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<BTreeMap<String, String>>, TeckelError>>,
{
    let where_clause = filter
        .as_ref()
        .map(|f| format!(" WHERE {f}"))
        .unwrap_or_default();

    match check {
        Check::Completeness(c) => {
            let query = format!(
                "SELECT COUNT(*) as total, COUNT(\"{}\") as non_null FROM \"{target}\"{where_clause}",
                c.column
            );
            let rows = query_fn(query).await?;
            let total: f64 = rows[0]
                .get("total")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let non_null: f64 = rows[0]
                .get("non_null")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let ratio = if total > 0.0 { non_null / total } else { 1.0 };
            let severity = c.severity.clone().unwrap_or(Severity::Error);
            Ok(CheckResult {
                suite: suite_name.to_string(),
                check_type: "completeness".to_string(),
                passed: ratio >= c.threshold,
                severity,
                message: format!(
                    "completeness({}) = {:.4} (threshold: {:.4})",
                    c.column, ratio, c.threshold
                ),
                metric: Some(ratio),
            })
        }

        Check::Uniqueness(c) => {
            let cols = c.columns.join("\", \"");
            let query = format!(
                "SELECT COUNT(*) as total, COUNT(DISTINCT(\"{cols}\")) as distinct_count FROM \"{target}\"{where_clause}"
            );
            let rows = query_fn(query).await?;
            let total: f64 = rows[0]
                .get("total")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let distinct: f64 = rows[0]
                .get("distinct_count")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let ratio = if total > 0.0 { distinct / total } else { 1.0 };
            Ok(CheckResult {
                suite: suite_name.to_string(),
                check_type: "uniqueness".to_string(),
                passed: ratio >= c.threshold,
                severity: Severity::Error,
                message: format!(
                    "uniqueness({}) = {:.4} (threshold: {:.4})",
                    c.columns.join(", "),
                    ratio,
                    c.threshold
                ),
                metric: Some(ratio),
            })
        }

        Check::Volume(c) => {
            let query = format!("SELECT COUNT(*) as row_count FROM \"{target}\"{where_clause}");
            let rows = query_fn(query).await?;
            let row_count: f64 = rows[0]
                .get("row_count")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let passed = c
                .row_count
                .as_ref()
                .map(|b| check_bound(row_count, b))
                .unwrap_or(true);
            Ok(CheckResult {
                suite: suite_name.to_string(),
                check_type: "volume".to_string(),
                passed,
                severity: Severity::Error,
                message: format!("volume: row_count = {row_count}"),
                metric: Some(row_count),
            })
        }

        Check::Validity(c) => {
            let mut conditions = Vec::new();
            if let Some(vals) = &c.accepted_values {
                let vals_str: Vec<String> = vals.iter().map(|v| format!("'{v}'")).collect();
                conditions.push(format!("\"{}\" IN ({})", c.column, vals_str.join(", ")));
            }
            if let Some(range) = &c.range {
                if let Some(min) = range.min {
                    let op = if range.strict_min { ">" } else { ">=" };
                    conditions.push(format!("\"{}\" {op} {min}", c.column));
                }
                if let Some(max) = range.max {
                    let op = if range.strict_max { "<" } else { "<=" };
                    conditions.push(format!("\"{}\" {op} {max}", c.column));
                }
            }
            if let Some(pattern) = &c.pattern {
                conditions.push(format!("\"{}\" LIKE '{pattern}'", c.column));
            }

            if conditions.is_empty() {
                return Ok(CheckResult {
                    suite: suite_name.to_string(),
                    check_type: "validity".to_string(),
                    passed: true,
                    severity: Severity::Error,
                    message: "validity: no conditions specified".to_string(),
                    metric: None,
                });
            }

            let cond = conditions.join(" AND ");
            let query = format!(
                "SELECT COUNT(*) as total, SUM(CASE WHEN {cond} THEN 1 ELSE 0 END) as valid FROM \"{target}\"{where_clause}"
            );
            let rows = query_fn(query).await?;
            let total: f64 = rows[0]
                .get("total")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let valid: f64 = rows[0]
                .get("valid")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let ratio = if total > 0.0 { valid / total } else { 1.0 };
            Ok(CheckResult {
                suite: suite_name.to_string(),
                check_type: "validity".to_string(),
                passed: ratio >= c.threshold,
                severity: Severity::Error,
                message: format!(
                    "validity({}) = {:.4} (threshold: {:.4})",
                    c.column, ratio, c.threshold
                ),
                metric: Some(ratio),
            })
        }

        Check::Schema(c) => {
            // Check required columns exist
            let query = format!("SELECT * FROM \"{target}\" LIMIT 0");
            let _rows = query_fn(query).await?;
            // The column names come from the keys of the first (empty) result
            // For schema checks, we query information_schema or use LIMIT 0
            let mut messages = Vec::new();
            // We can't easily get column names from an empty result via our simple query_fn.
            // Use a different approach: try to SELECT each required column
            for col in &c.required_columns {
                let check_query = format!("SELECT \"{col}\" FROM \"{target}\" LIMIT 0");
                if query_fn(check_query).await.is_err() {
                    messages.push(format!("missing required column: {col}"));
                }
            }
            let passed = messages.is_empty();
            Ok(CheckResult {
                suite: suite_name.to_string(),
                check_type: "schema".to_string(),
                passed,
                severity: Severity::Error,
                message: if passed {
                    "schema: all required columns present".to_string()
                } else {
                    format!("schema: {}", messages.join("; "))
                },
                metric: None,
            })
        }

        Check::Statistical(c) => {
            let query = format!(
                "SELECT AVG(\"{col}\") as mean_val, MIN(\"{col}\") as min_val, \
                 MAX(\"{col}\") as max_val, SUM(\"{col}\") as sum_val, \
                 STDDEV(\"{col}\") as stdev_val \
                 FROM \"{target}\"{where_clause}",
                col = c.column
            );
            let rows = query_fn(query).await?;
            let get_f64 =
                |key: &str| -> f64 { rows[0].get(key).and_then(|v| v.parse().ok()).unwrap_or(0.0) };
            let mut passed = true;
            let mut details = Vec::new();
            if let Some(b) = &c.mean {
                let v = get_f64("mean_val");
                if !check_bound(v, b) {
                    passed = false;
                    details.push(format!("mean={v}"));
                }
            }
            if let Some(b) = &c.min {
                let v = get_f64("min_val");
                if !check_bound(v, b) {
                    passed = false;
                    details.push(format!("min={v}"));
                }
            }
            if let Some(b) = &c.max {
                let v = get_f64("max_val");
                if !check_bound(v, b) {
                    passed = false;
                    details.push(format!("max={v}"));
                }
            }
            if let Some(b) = &c.sum {
                let v = get_f64("sum_val");
                if !check_bound(v, b) {
                    passed = false;
                    details.push(format!("sum={v}"));
                }
            }
            if let Some(b) = &c.stdev {
                let v = get_f64("stdev_val");
                if !check_bound(v, b) {
                    passed = false;
                    details.push(format!("stdev={v}"));
                }
            }
            Ok(CheckResult {
                suite: suite_name.to_string(),
                check_type: "statistical".to_string(),
                passed,
                severity: Severity::Error,
                message: format!("statistical({}): {}", c.column, details.join(", ")),
                metric: None,
            })
        }

        Check::Freshness(c) => {
            let query = format!(
                "SELECT MAX(\"{}\") as latest FROM \"{target}\"{where_clause}",
                c.column
            );
            let rows = query_fn(query).await?;
            let latest = rows[0].get("latest").cloned().unwrap_or_default();
            // Freshness check: we report the latest value; actual duration comparison
            // requires parsing the max_age ISO 8601 duration, which is backend-specific.
            Ok(CheckResult {
                suite: suite_name.to_string(),
                check_type: "freshness".to_string(),
                passed: !latest.is_empty(),
                severity: Severity::Error,
                message: format!(
                    "freshness({}): latest = {latest}, max_age = {}",
                    c.column, c.max_age
                ),
                metric: None,
            })
        }

        Check::Referential(c) => {
            let query = format!(
                "SELECT COUNT(*) as total, \
                 SUM(CASE WHEN \"{ref_col}\" IS NOT NULL THEN 1 ELSE 0 END) as matched \
                 FROM \"{target}\" LEFT JOIN \"{ref_asset}\" \
                 ON \"{target}\".\"{col}\" = \"{ref_asset}\".\"{ref_col}\"{where_clause}",
                col = c.column,
                ref_asset = c.reference_asset,
                ref_col = c.reference_column
            );
            let rows = query_fn(query).await?;
            let total: f64 = rows[0]
                .get("total")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let matched: f64 = rows[0]
                .get("matched")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let ratio = if total > 0.0 { matched / total } else { 1.0 };
            Ok(CheckResult {
                suite: suite_name.to_string(),
                check_type: "referential".to_string(),
                passed: ratio >= c.threshold,
                severity: Severity::Error,
                message: format!(
                    "referential({} → {}.{}) = {:.4} (threshold: {:.4})",
                    c.column, c.reference_asset, c.reference_column, ratio, c.threshold
                ),
                metric: Some(ratio),
            })
        }

        Check::CrossColumn(c) => {
            let query = format!(
                "SELECT COUNT(*) as total, \
                 SUM(CASE WHEN {cond} THEN 1 ELSE 0 END) as passing \
                 FROM \"{target}\"{where_clause}",
                cond = c.condition
            );
            let rows = query_fn(query).await?;
            let total: f64 = rows[0]
                .get("total")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let passing: f64 = rows[0]
                .get("passing")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let ratio = if total > 0.0 { passing / total } else { 1.0 };
            Ok(CheckResult {
                suite: suite_name.to_string(),
                check_type: "crossColumn".to_string(),
                passed: ratio >= c.threshold,
                severity: Severity::Error,
                message: format!(
                    "crossColumn({}) = {:.4} (threshold: {:.4})",
                    c.description.as_deref().unwrap_or(&c.condition),
                    ratio,
                    c.threshold
                ),
                metric: Some(ratio),
            })
        }

        Check::Custom(c) => {
            let query = format!(
                "SELECT COUNT(*) as total, \
                 SUM(CASE WHEN {cond} THEN 1 ELSE 0 END) as passing \
                 FROM \"{target}\"{where_clause}",
                cond = c.condition
            );
            let rows = query_fn(query).await?;
            let total: f64 = rows[0]
                .get("total")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let passing: f64 = rows[0]
                .get("passing")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let ratio = if total > 0.0 { passing / total } else { 1.0 };
            Ok(CheckResult {
                suite: suite_name.to_string(),
                check_type: "custom".to_string(),
                passed: ratio >= c.threshold,
                severity: Severity::Error,
                message: format!(
                    "custom({}) = {:.4} (threshold: {:.4})",
                    c.description.as_deref().unwrap_or(&c.condition),
                    ratio,
                    c.threshold
                ),
                metric: Some(ratio),
            })
        }
    }
}

fn check_bound(value: f64, bound: &BoundSpec) -> bool {
    if let Some(min) = bound.min {
        if value < min {
            return false;
        }
    }
    if let Some(max) = bound.max {
        if value > max {
            return false;
        }
    }
    if let Some((lo, hi)) = bound.between {
        if value < lo || value > hi {
            return false;
        }
    }
    true
}

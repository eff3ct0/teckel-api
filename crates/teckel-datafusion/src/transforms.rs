use datafusion::arrow::array::{self as arrow_array, Array as _};
use datafusion::arrow::datatypes as arrow_types;
use datafusion::common::DFSchema;
use datafusion::logical_expr::SortExpr;
use datafusion::prelude::*;
use std::collections::BTreeMap;
use teckel_model::source::Source;
use teckel_model::TeckelError;

pub async fn apply(
    ctx: &SessionContext,
    source: &Source,
    cache: &BTreeMap<String, DataFrame>,
) -> Result<DataFrame, TeckelError> {
    match source {
        Source::Select(t) => {
            let df = get(cache, &t.from)?;
            let schema = df.schema().clone();
            let cols: Vec<Expr> = t
                .columns
                .iter()
                .map(|c| {
                    parse_expr_with_schema(ctx, c, &schema).unwrap_or_else(|_| col(c.as_str()))
                })
                .collect();
            df.select(cols)
                .map_err(|e| TeckelError::Execution(format!("select: {e}")))
        }
        Source::Where(t) => {
            let df = get(cache, &t.from)?;
            let schema = df.schema().clone();
            let expr = parse_expr_with_schema(ctx, &t.filter, &schema)?;
            df.filter(expr)
                .map_err(|e| TeckelError::Execution(format!("where: {e}")))
        }
        Source::GroupBy(t) => {
            let df = get(cache, &t.from)?;
            let schema = df.schema().clone();
            let group_exprs: Vec<Expr> = t.by.iter().map(|c| col(c.as_str())).collect();
            let agg_exprs: Vec<Expr> = t
                .agg
                .iter()
                .map(|a| {
                    parse_expr_with_schema(ctx, a, &schema).unwrap_or_else(|_| col(a.as_str()))
                })
                .collect();
            df.aggregate(group_exprs, agg_exprs)
                .map_err(|e| TeckelError::Execution(format!("group by: {e}")))
        }
        Source::OrderBy(t) => {
            let df = get(cache, &t.from)?;
            let sort_exprs: Vec<SortExpr> = t
                .columns
                .iter()
                .map(|sc| match sc {
                    teckel_model::types::SortColumn::Simple(name) => SortExpr {
                        expr: col(name.as_str()),
                        asc: true,
                        nulls_first: false,
                    },
                    teckel_model::types::SortColumn::Explicit {
                        column,
                        direction,
                        nulls,
                    } => SortExpr {
                        expr: col(column.as_str()),
                        asc: matches!(direction, teckel_model::types::SortDirection::Asc),
                        nulls_first: matches!(nulls, teckel_model::types::NullOrdering::First),
                    },
                })
                .collect();
            df.sort(sort_exprs)
                .map_err(|e| TeckelError::Execution(format!("order by: {e}")))
        }
        Source::Join(t) => {
            let mut result = get(cache, &t.left)?;
            for target in &t.right {
                let right_df = get(cache, &target.name)?;
                let join_type = match target.join_type {
                    teckel_model::types::JoinType::Inner => JoinType::Inner,
                    teckel_model::types::JoinType::Left => JoinType::Left,
                    teckel_model::types::JoinType::Right => JoinType::Right,
                    teckel_model::types::JoinType::Outer => JoinType::Full,
                    teckel_model::types::JoinType::Cross => JoinType::Inner,
                    teckel_model::types::JoinType::LeftSemi => JoinType::LeftSemi,
                    teckel_model::types::JoinType::LeftAnti => JoinType::LeftAnti,
                };

                if matches!(target.join_type, teckel_model::types::JoinType::Cross) {
                    // Cross join: use join with no columns and no filter
                    result = result
                        .join(
                            right_df,
                            JoinType::Inner,
                            &[] as &[&str],
                            &[] as &[&str],
                            None,
                        )
                        .map_err(|e| TeckelError::Execution(format!("cross join: {e}")))?;
                } else {
                    let filter = target.on.join(" AND ");
                    // Build a combined schema for the join condition
                    let left_schema = result.schema();
                    let right_schema = right_df.schema();
                    let combined = left_schema
                        .join(right_schema)
                        .map_err(|e| TeckelError::Execution(format!("join schema: {e}")))?;
                    let filter_expr = parse_expr_with_schema(ctx, &filter, &combined)?;
                    result = result
                        .join(
                            right_df,
                            join_type,
                            &[] as &[&str],
                            &[] as &[&str],
                            Some(filter_expr),
                        )
                        .map_err(|e| TeckelError::Execution(format!("join: {e}")))?;
                }
            }
            Ok(result)
        }
        Source::Union(t) => {
            let mut dfs = t
                .sources
                .iter()
                .map(|s| get(cache, s))
                .collect::<Result<Vec<_>, _>>()?;
            let mut result = dfs.remove(0);
            for df in dfs {
                result = result
                    .union(df)
                    .map_err(|e| TeckelError::Execution(format!("union: {e}")))?;
            }
            if !t.all {
                result = result
                    .distinct()
                    .map_err(|e| TeckelError::Execution(format!("union distinct: {e}")))?;
            }
            Ok(result)
        }
        Source::Intersect(t) => {
            let mut dfs = t
                .sources
                .iter()
                .map(|s| get(cache, s))
                .collect::<Result<Vec<_>, _>>()?;
            let mut result = dfs.remove(0);
            for df in dfs {
                result = result
                    .intersect(df)
                    .map_err(|e| TeckelError::Execution(format!("intersect: {e}")))?;
            }
            Ok(result)
        }
        Source::Except(t) => {
            let left = get(cache, &t.left)?;
            let right = get(cache, &t.right)?;
            left.except(right)
                .map_err(|e| TeckelError::Execution(format!("except: {e}")))
        }
        Source::Distinct(t) => {
            let df = get(cache, &t.from)?;
            df.distinct()
                .map_err(|e| TeckelError::Execution(format!("distinct: {e}")))
        }
        Source::Limit(t) => {
            let df = get(cache, &t.from)?;
            df.limit(0, Some(t.count as usize))
                .map_err(|e| TeckelError::Execution(format!("limit: {e}")))
        }
        Source::AddColumns(t) => {
            let mut df = get(cache, &t.from)?;
            for col_def in &t.columns {
                let schema = df.schema().clone();
                let expr = parse_expr_with_schema(ctx, &col_def.expression, &schema)?;
                df = df.with_column(&col_def.name, expr).map_err(|e| {
                    TeckelError::Execution(format!("add column \"{}\": {e}", col_def.name))
                })?;
            }
            Ok(df)
        }
        Source::DropColumns(t) => {
            let df = get(cache, &t.from)?;
            let cols: Vec<&str> = t.columns.iter().map(|s| s.as_str()).collect();
            df.drop_columns(&cols)
                .map_err(|e| TeckelError::Execution(format!("drop columns: {e}")))
        }
        Source::RenameColumns(t) => {
            let mut df = get(cache, &t.from)?;
            for (old, new) in &t.mappings {
                df = df
                    .with_column_renamed(old.as_str(), new.as_str())
                    .map_err(|e| {
                        TeckelError::Execution(format!("rename \"{old}\" -> \"{new}\": {e}"))
                    })?;
            }
            Ok(df)
        }
        Source::Sql(t) => {
            for view_name in &t.views {
                let df = get(cache, view_name)?;
                ctx.register_table(view_name, df.into_view()).map_err(|e| {
                    TeckelError::Execution(format!("register view \"{view_name}\": {e}"))
                })?;
            }
            ctx.sql(&t.query)
                .await
                .map_err(|e| TeckelError::Execution(format!("sql: {e}")))
        }
        Source::CastColumns(t) => {
            let df = get(cache, &t.from)?;
            // Build CAST expressions via SQL: SELECT CAST(col AS type) as col, ...
            let view = "__teckel_cast_src";
            ctx.register_table(view, df.into_view())
                .map_err(|e| TeckelError::Execution(format!("cast register: {e}")))?;
            let table_df = ctx
                .table(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("cast table: {e}")))?;
            let schema_fields: Vec<String> = table_df
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let cast_map: BTreeMap<&str, &str> = t
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.target_type.as_str()))
                .collect();
            let projections: Vec<String> = schema_fields
                .iter()
                .map(|name| {
                    if let Some(target_type) = cast_map.get(name.as_str()) {
                        format!("CAST(\"{name}\" AS {target_type}) AS \"{name}\"")
                    } else {
                        format!("\"{name}\"")
                    }
                })
                .collect();
            let query = format!("SELECT {} FROM {view}", projections.join(", "));
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("cast columns: {e}")))
        }
        Source::Window(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_window_src";
            ctx.register_table(view, df.into_view())
                .map_err(|e| TeckelError::Execution(format!("window register: {e}")))?;

            let partition_clause = t.partition_by.join(", ");
            let order_clause = if t.order_by.is_empty() {
                String::new()
            } else {
                let parts: Vec<String> = t
                    .order_by
                    .iter()
                    .map(|sc| match sc {
                        teckel_model::types::SortColumn::Simple(name) => name.clone(),
                        teckel_model::types::SortColumn::Explicit {
                            column,
                            direction,
                            nulls,
                        } => {
                            let dir = match direction {
                                teckel_model::types::SortDirection::Asc => "ASC",
                                teckel_model::types::SortDirection::Desc => "DESC",
                            };
                            let ns = match nulls {
                                teckel_model::types::NullOrdering::First => "NULLS FIRST",
                                teckel_model::types::NullOrdering::Last => "NULLS LAST",
                            };
                            format!("{column} {dir} {ns}")
                        }
                    })
                    .collect();
                format!("ORDER BY {}", parts.join(", "))
            };

            let frame_clause = format!(
                "{} BETWEEN {} AND {}",
                match t.frame.frame_type {
                    teckel_model::types::FrameType::Rows => "ROWS",
                    teckel_model::types::FrameType::Range => "RANGE",
                },
                t.frame.start,
                t.frame.end
            );

            let window_spec =
                format!("PARTITION BY {partition_clause} {order_clause} {frame_clause}");
            let func_exprs: Vec<String> = t
                .functions
                .iter()
                .map(|f| format!("{} OVER ({window_spec}) AS \"{}\"", f.expression, f.alias))
                .collect();

            let query = format!("SELECT *, {} FROM {view}", func_exprs.join(", "));
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("window: {e}")))
        }
        Source::Pivot(t) => {
            // DataFusion doesn't have native PIVOT; use conditional aggregation
            let df = get(cache, &t.from)?;
            let view = "__teckel_pivot_src";
            ctx.register_table(view, df.clone().into_view())
                .map_err(|e| TeckelError::Execution(format!("pivot register: {e}")))?;

            let values = match &t.values {
                Some(vals) => vals.clone(),
                None => {
                    // Query distinct values from the pivot column
                    let distinct_query = format!(
                        "SELECT DISTINCT \"{}\" FROM {view} ORDER BY \"{}\"",
                        t.pivot_column, t.pivot_column
                    );
                    let distinct_df = ctx
                        .sql(&distinct_query)
                        .await
                        .map_err(|e| TeckelError::Execution(format!("pivot distinct: {e}")))?;
                    let batches = distinct_df
                        .collect()
                        .await
                        .map_err(|e| TeckelError::Execution(format!("pivot collect: {e}")))?;
                    let mut vals = Vec::new();
                    for batch in &batches {
                        let arr = batch.column(0);
                        let str_arr = arrow_array::cast::as_string_array(arr);
                        for i in 0..str_arr.len() {
                            if !str_arr.is_null(i) {
                                vals.push(str_arr.value(i).to_string());
                            }
                        }
                    }
                    vals
                }
            };

            let group_cols = t.group_by.join(", ");
            let mut pivot_exprs = Vec::new();
            for agg_expr in &t.agg {
                for val in &values {
                    pivot_exprs.push(format!(
                        "{agg_expr} FILTER (WHERE \"{}\" = '{val}') AS \"{val}\"",
                        t.pivot_column
                    ));
                }
            }

            let query = format!(
                "SELECT {group_cols}, {} FROM {view} GROUP BY {group_cols}",
                pivot_exprs.join(", ")
            );
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("pivot: {e}")))
        }
        Source::Unpivot(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_unpivot_src";
            ctx.register_table(view, df.into_view())
                .map_err(|e| TeckelError::Execution(format!("unpivot register: {e}")))?;

            let id_cols = t.ids.join(", ");
            let unions: Vec<String> = t
                .values
                .iter()
                .map(|col_name| {
                    format!(
                        "SELECT {id_cols}, '{col_name}' AS \"{}\", \"{col_name}\" AS \"{}\" FROM {view}",
                        t.variable_column, t.value_column
                    )
                })
                .collect();

            let query = unions.join(" UNION ALL ");
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("unpivot: {e}")))
        }
        Source::Flatten(_t) => {
            // Flatten requires recursive struct field inspection at the Arrow level.
            // This is complex and deferred — return an informative error.
            Err(TeckelError::Execution(
                "flatten transform requires Arrow-level struct inspection (not yet implemented)"
                    .to_string(),
            ))
        }
        Source::Sample(t) => {
            // DataFusion doesn't have native TABLESAMPLE; approximate with random filter
            let df = get(cache, &t.from)?;
            let seed_expr = match t.seed {
                Some(seed) => format!("random({seed})"),
                None => "random()".to_string(),
            };
            let view = "__teckel_sample_src";
            ctx.register_table(view, df.into_view())
                .map_err(|e| TeckelError::Execution(format!("sample register: {e}")))?;
            let query = format!("SELECT * FROM {view} WHERE {seed_expr} < {}", t.fraction);
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("sample: {e}")))
        }
        Source::Conditional(t) => {
            // Build a CASE WHEN expression and add as column
            let df = get(cache, &t.from)?;
            let mut case_sql = String::from("CASE");
            for branch in &t.branches {
                case_sql.push_str(&format!(" WHEN {} THEN {}", branch.condition, branch.value));
            }
            if let Some(otherwise) = &t.otherwise {
                case_sql.push_str(&format!(" ELSE {otherwise}"));
            }
            case_sql.push_str(" END");

            let schema = df.schema().clone();
            let expr = parse_expr_with_schema(ctx, &case_sql, &schema)?;
            df.with_column(&t.output_column, expr)
                .map_err(|e| TeckelError::Execution(format!("conditional: {e}")))
        }
        Source::Split(_t) => {
            // Split is expanded to 2 WHERE transforms by the parser's rewrite phase.
            // If we reach here, it means the rewrite didn't run — should not happen.
            Err(TeckelError::Execution(
                "split should have been expanded to WHERE transforms during parsing".to_string(),
            ))
        }
        Source::Rollup(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_rollup_src";
            ctx.register_table(view, df.into_view())
                .map_err(|e| TeckelError::Execution(format!("rollup register: {e}")))?;
            let by_cols = t.by.join(", ");
            let agg_exprs = t.agg.join(", ");
            let query =
                format!("SELECT {by_cols}, {agg_exprs} FROM {view} GROUP BY ROLLUP({by_cols})");
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("rollup: {e}")))
        }
        Source::Cube(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_cube_src";
            ctx.register_table(view, df.into_view())
                .map_err(|e| TeckelError::Execution(format!("cube register: {e}")))?;
            let by_cols = t.by.join(", ");
            let agg_exprs = t.agg.join(", ");
            let query =
                format!("SELECT {by_cols}, {agg_exprs} FROM {view} GROUP BY CUBE({by_cols})");
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("cube: {e}")))
        }
        Source::Scd2(t) => {
            // SCD Type 2: complex merge of current and incoming dimension records
            let current = get(cache, &t.current)?;
            let incoming = get(cache, &t.incoming)?;
            ctx.register_table("__teckel_scd2_current", current.into_view())
                .map_err(|e| TeckelError::Execution(format!("scd2 register current: {e}")))?;
            ctx.register_table("__teckel_scd2_incoming", incoming.into_view())
                .map_err(|e| TeckelError::Execution(format!("scd2 register incoming: {e}")))?;

            let key_join = t
                .key_columns
                .iter()
                .map(|k| format!("c.\"{k}\" = i.\"{k}\""))
                .collect::<Vec<_>>()
                .join(" AND ");
            let track_changed = t
                .track_columns
                .iter()
                .map(|tc| format!("c.\"{tc}\" IS DISTINCT FROM i.\"{tc}\""))
                .collect::<Vec<_>>()
                .join(" OR ");

            let query = format!(
                r#"
                -- Unchanged current records (no match or no changes)
                SELECT c.*, c."{start}" AS "{start}", c."{end}" AS "{end}", c."{flag}" AS "{flag}"
                FROM __teckel_scd2_current c
                LEFT JOIN __teckel_scd2_incoming i ON {key_join}
                WHERE i."{first_key}" IS NULL OR NOT ({track_changed})

                UNION ALL

                -- Closed current records (matched with changes)
                SELECT c.*, c."{start}" AS "{start}", now() AS "{end}", false AS "{flag}"
                FROM __teckel_scd2_current c
                INNER JOIN __teckel_scd2_incoming i ON {key_join}
                WHERE {track_changed}

                UNION ALL

                -- New version of changed records
                SELECT i.*, now() AS "{start}", NULL AS "{end}", true AS "{flag}"
                FROM __teckel_scd2_incoming i
                INNER JOIN __teckel_scd2_current c ON {key_join}
                WHERE {track_changed}

                UNION ALL

                -- Brand new records (not in current)
                SELECT i.*, now() AS "{start}", NULL AS "{end}", true AS "{flag}"
                FROM __teckel_scd2_incoming i
                LEFT JOIN __teckel_scd2_current c ON {key_join}
                WHERE c."{first_key}" IS NULL
                "#,
                start = t.start_date_column,
                end = t.end_date_column,
                flag = t.current_flag_column,
                first_key = t.key_columns[0],
                key_join = key_join,
                track_changed = track_changed,
            );
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("scd2: {e}")))
        }
        Source::Enrich(_t) => {
            // Enrich requires an HTTP client (reqwest). Deferred to a feature flag.
            Err(TeckelError::Execution(
                "enrich transform requires HTTP client (not yet implemented — see issue #4)"
                    .to_string(),
            ))
        }
        Source::SchemaEnforce(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_schema_src";
            ctx.register_table(view, df.into_view())
                .map_err(|e| TeckelError::Execution(format!("schema enforce register: {e}")))?;

            let projections: Vec<String> = t
                .columns
                .iter()
                .map(|c| {
                    format!(
                        "CAST(\"{name}\" AS {dtype}) AS \"{name}\"",
                        name = c.name,
                        dtype = c.data_type
                    )
                })
                .collect();

            let query = format!("SELECT {} FROM {view}", projections.join(", "));
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("schema enforce: {e}")))
        }
        Source::Assertion(t) => {
            let df = get(cache, &t.from)?;
            // For assertions, we validate and return the original DataFrame unchanged
            // (or filter/fail based on onFailure mode)
            let view = "__teckel_assert_src";
            ctx.register_table(view, df.clone().into_view())
                .map_err(|e| TeckelError::Execution(format!("assertion register: {e}")))?;

            for check in &t.checks {
                let condition = if check.rule == "not_null" {
                    let col_name = check.column.as_deref().unwrap_or("*");
                    format!("\"{col_name}\" IS NULL")
                } else if check.rule == "unique" {
                    // Check for duplicates
                    let col_name = check.column.as_deref().unwrap_or("*");
                    let count_query = format!(
                        "SELECT COUNT(*) - COUNT(DISTINCT \"{col_name}\") AS dups FROM {view}"
                    );
                    let result = ctx
                        .sql(&count_query)
                        .await
                        .map_err(|e| TeckelError::Execution(format!("assertion unique: {e}")))?;
                    let batches = result
                        .collect()
                        .await
                        .map_err(|e| TeckelError::Execution(format!("assertion collect: {e}")))?;
                    if let Some(batch) = batches.first() {
                        let arr = batch.column(0);
                        let val =
                            arrow_array::cast::as_primitive_array::<arrow_types::Int64Type>(arr)
                                .value(0);
                        if val > 0 {
                            match t.on_failure {
                                teckel_model::types::OnFailure::Fail => {
                                    return Err(TeckelError::spec(
                                        teckel_model::TeckelErrorCode::EQuality001,
                                        format!(
                                            "assertion failed: column \"{}\" has {val} duplicate values",
                                            col_name
                                        ),
                                    ));
                                }
                                teckel_model::types::OnFailure::Warn => {
                                    tracing::warn!(
                                        column = col_name,
                                        duplicates = val,
                                        "uniqueness check warning"
                                    );
                                }
                                teckel_model::types::OnFailure::Drop => {}
                            }
                        }
                    }
                    continue;
                } else {
                    // Custom rule as boolean expression — find failing rows
                    format!("NOT ({})", check.rule)
                };

                let count_query =
                    format!("SELECT COUNT(*) AS failures FROM {view} WHERE {condition}");
                let result = ctx
                    .sql(&count_query)
                    .await
                    .map_err(|e| TeckelError::Execution(format!("assertion check: {e}")))?;
                let batches = result
                    .collect()
                    .await
                    .map_err(|e| TeckelError::Execution(format!("assertion collect: {e}")))?;

                if let Some(batch) = batches.first() {
                    let arr = batch.column(0);
                    let failures =
                        arrow_array::cast::as_primitive_array::<arrow_types::Int64Type>(arr)
                            .value(0);
                    if failures > 0 {
                        let desc = check.description.as_deref().unwrap_or(&check.rule);
                        match t.on_failure {
                            teckel_model::types::OnFailure::Fail => {
                                return Err(TeckelError::spec(
                                    teckel_model::TeckelErrorCode::EQuality001,
                                    format!("assertion failed: {desc} ({failures} failing rows)"),
                                ));
                            }
                            teckel_model::types::OnFailure::Warn => {
                                tracing::warn!(check = desc, failures, "assertion warning");
                            }
                            teckel_model::types::OnFailure::Drop => {
                                // Re-read and filter
                                let filter_query =
                                    format!("SELECT * FROM {view} WHERE NOT ({condition})");
                                let filtered = ctx.sql(&filter_query).await.map_err(|e| {
                                    TeckelError::Execution(format!("assertion drop: {e}"))
                                })?;
                                return Ok(filtered);
                            }
                        }
                    }
                }
            }
            // All checks passed — return original DataFrame
            Ok(df)
        }
        Source::Repartition(t) => {
            let df = get(cache, &t.from)?;
            if t.columns.is_empty() {
                df.repartition(datafusion::logical_expr::Partitioning::RoundRobinBatch(
                    t.num_partitions as usize,
                ))
                .map_err(|e| TeckelError::Execution(format!("repartition: {e}")))
            } else {
                let hash_exprs: Vec<Expr> = t.columns.iter().map(|c| col(c.as_str())).collect();
                df.repartition(datafusion::logical_expr::Partitioning::Hash(
                    hash_exprs,
                    t.num_partitions as usize,
                ))
                .map_err(|e| TeckelError::Execution(format!("repartition hash: {e}")))
            }
        }
        Source::Coalesce(t) => {
            let df = get(cache, &t.from)?;
            df.repartition(datafusion::logical_expr::Partitioning::RoundRobinBatch(
                t.num_partitions as usize,
            ))
            .map_err(|e| TeckelError::Execution(format!("coalesce: {e}")))
        }
        Source::Custom(_t) => Err(TeckelError::spec(
            teckel_model::TeckelErrorCode::EComp001,
            "custom transform requires a registered component (plugin system not yet implemented)",
        )),
        // I/O handled by the executor, not here
        Source::Input(_) | Source::Output(_) => Err(TeckelError::Execution(
            "Input/Output should be handled by the executor, not apply()".to_string(),
        )),
    }
}

fn get(cache: &BTreeMap<String, DataFrame>, name: &str) -> Result<DataFrame, TeckelError> {
    cache.get(name).cloned().ok_or_else(|| {
        TeckelError::Execution(format!(
            "asset \"{name}\" not found (dependency not yet computed?)"
        ))
    })
}

fn parse_expr_with_schema(
    ctx: &SessionContext,
    expr_str: &str,
    schema: &DFSchema,
) -> Result<Expr, TeckelError> {
    ctx.parse_sql_expr(expr_str, schema)
        .map_err(|e| TeckelError::Execution(format!("invalid expression \"{expr_str}\": {e}")))
}

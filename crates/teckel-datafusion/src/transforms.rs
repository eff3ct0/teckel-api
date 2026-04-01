use datafusion::arrow::array::{self as arrow_array, Array as _};
use datafusion::arrow::datatypes as arrow_types;
use datafusion::common::DFSchema;
use datafusion::execution::FunctionRegistry;
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
        Source::Flatten(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_flatten_src";
            ctx.register_table(view, df.into_view())
                .map_err(|e| TeckelError::Execution(format!("flatten register: {e}")))?;

            let table_df = ctx
                .table(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("flatten table: {e}")))?;
            let schema = table_df.schema().inner().clone();

            // Build flattened projections from the Arrow schema
            let mut projections = Vec::new();
            flatten_fields(&schema.fields, "", &t.separator, &mut projections);

            if projections.is_empty() {
                return Ok(table_df);
            }

            let query = format!("SELECT {} FROM {view}", projections.join(", "));
            let mut result = ctx
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("flatten: {e}")))?;

            // Explode arrays if requested
            if t.explode_arrays {
                let result_schema = result.schema().inner().clone();
                for field in result_schema.fields() {
                    if matches!(field.data_type(), arrow_types::DataType::List(_) | arrow_types::DataType::LargeList(_)) {
                        let col_name = field.name();
                        let explode_view = "__teckel_explode_tmp";
                        ctx.register_table(explode_view, result.into_view())
                            .map_err(|e| TeckelError::Execution(format!("explode register: {e}")))?;
                        result = ctx
                            .sql(&format!(
                                "SELECT *, unnest(\"{col_name}\") AS \"{col_name}__unnested\" FROM {explode_view}"
                            ))
                            .await
                            .map_err(|e| TeckelError::Execution(format!("explode: {e}")))?;
                        // Drop original array column, rename unnested
                        result = result
                            .drop_columns(&[col_name.as_str()])
                            .map_err(|e| TeckelError::Execution(format!("explode drop: {e}")))?;
                        result = result
                            .with_column_renamed(
                                format!("{col_name}__unnested").as_str(),
                                col_name.as_str(),
                            )
                            .map_err(|e| TeckelError::Execution(format!("explode rename: {e}")))?;
                    }
                }
            }

            Ok(result)
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
        Source::Enrich(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_enrich_src";
            ctx.register_table(view, df.into_view())
                .map_err(|e| TeckelError::Execution(format!("enrich register: {e}")))?;

            // Collect distinct key values
            let key_query = format!(
                "SELECT DISTINCT \"{}\" FROM {view} WHERE \"{}\" IS NOT NULL",
                t.key_column, t.key_column
            );
            let key_df = ctx
                .sql(&key_query)
                .await
                .map_err(|e| TeckelError::Execution(format!("enrich keys: {e}")))?;
            let key_batches = key_df
                .collect()
                .await
                .map_err(|e| TeckelError::Execution(format!("enrich collect keys: {e}")))?;

            let mut key_values = Vec::new();
            for batch in &key_batches {
                let arr = batch.column(0);
                let str_arr = arrow_array::cast::as_string_array(arr);
                for i in 0..str_arr.len() {
                    if !str_arr.is_null(i) {
                        key_values.push(str_arr.value(i).to_string());
                    }
                }
            }

            // Call API for each distinct key, cache results
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_millis(t.timeout))
                .build()
                .map_err(|e| TeckelError::Execution(format!("enrich http client: {e}")))?;

            let mut response_map: BTreeMap<String, Option<String>> = BTreeMap::new();
            for key in &key_values {
                let url = t.url.replace("${keyColumn}", key);
                let result = call_with_retries(&client, &url, &t.method, &t.headers, t.max_retries).await;
                match result {
                    Ok(body) => { response_map.insert(key.clone(), Some(body)); }
                    Err(e) => match t.on_error {
                        teckel_model::types::OnError::Fail => {
                            return Err(TeckelError::Execution(format!(
                                "enrich API call failed for key \"{key}\": {e}"
                            )));
                        }
                        teckel_model::types::OnError::Null => {
                            tracing::warn!(key = key.as_str(), error = %e, "enrich API call failed, using NULL");
                            response_map.insert(key.clone(), None);
                        }
                        teckel_model::types::OnError::Skip => {
                            // Will be filtered out below
                        }
                    },
                }
            }

            // Build CASE expression to map key → response
            let mut case_sql = String::from("CASE");
            for (key, response) in &response_map {
                if let Some(body) = response {
                    let escaped = body.replace('\'', "''");
                    case_sql.push_str(&format!(
                        " WHEN \"{}\" = '{key}' THEN '{escaped}'",
                        t.key_column
                    ));
                }
            }
            case_sql.push_str(" ELSE NULL END");

            let query = format!(
                "SELECT *, {case_sql} AS \"{}\" FROM {view}",
                t.response_column
            );
            let mut result = ctx
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("enrich join: {e}")))?;

            // If onError=skip, filter out rows with failed keys
            if matches!(t.on_error, teckel_model::types::OnError::Skip) {
                let skip_keys: Vec<String> = key_values
                    .iter()
                    .filter(|k| !response_map.contains_key(*k))
                    .map(|k| format!("'{k}'"))
                    .collect();
                if !skip_keys.is_empty() {
                    let skip_view = "__teckel_enrich_result";
                    ctx.register_table(skip_view, result.into_view())
                        .map_err(|e| TeckelError::Execution(format!("enrich skip register: {e}")))?;
                    let filter = format!(
                        "SELECT * FROM {skip_view} WHERE \"{}\" NOT IN ({})",
                        t.key_column,
                        skip_keys.join(", ")
                    );
                    result = ctx
                        .sql(&filter)
                        .await
                        .map_err(|e| TeckelError::Execution(format!("enrich skip: {e}")))?;
                }
            }

            Ok(result)
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
        Source::Custom(t) => {
            let df = get(cache, &t.from)?;
            // Custom components use a SQL-based convention:
            // The component name must match a registered UDF/UDAF in the SessionContext.
            // Users register custom functions before execution via
            // DataFusionBackend::session().register_udf(...).
            let view = "__teckel_custom_src";
            ctx.register_table(view, df.into_view())
                .map_err(|e| TeckelError::Execution(format!("custom register: {e}")))?;

            // Check if a UDF with this name exists
            if ctx.udf(&t.component).is_ok() || ctx.udaf(&t.component).is_ok() {
                let opts_json = serde_json::to_string(&t.options).unwrap_or_default();
                tracing::info!(
                    component = t.component.as_str(),
                    options = opts_json.as_str(),
                    "executing custom component"
                );
                // The component is available as a UDF — the user is expected
                // to use it in a downstream select/addColumns expression.
                // For a direct custom transform, we pass through the data.
                ctx.table(view)
                    .await
                    .map_err(|e| TeckelError::Execution(format!("custom component: {e}")))
            } else {
                Err(TeckelError::spec(
                    teckel_model::TeckelErrorCode::EComp001,
                    format!(
                        "unregistered custom component \"{}\". Register UDFs via DataFusionBackend::session().register_udf() before execution.",
                        t.component
                    ),
                ))
            }
        }
        // ── v3 transformations ────────────────────────────────────
        Source::Offset(t) => {
            let df = get(cache, &t.from)?;
            df.limit(t.count as usize, None)
                .map_err(|e| TeckelError::Execution(format!("offset: {e}")))
        }
        Source::Tail(t) => {
            // Collect all rows, then take the last N.
            // DataFusion does not have a native "tail" — we use OFFSET (total - N).
            let df = get(cache, &t.from)?;
            let view = "__teckel_tail_src";
            ctx.register_table(view, df.into_view())
                .map_err(|e| TeckelError::Execution(format!("tail register: {e}")))?;
            let count_df = ctx
                .sql(&format!("SELECT COUNT(*) AS cnt FROM {view}"))
                .await
                .map_err(|e| TeckelError::Execution(format!("tail count: {e}")))?;
            let batches = count_df
                .collect()
                .await
                .map_err(|e| TeckelError::Execution(format!("tail collect: {e}")))?;
            let total: i64 = batches
                .first()
                .map(|b| {
                    arrow_array::cast::as_primitive_array::<arrow_types::Int64Type>(b.column(0))
                        .value(0)
                })
                .unwrap_or(0);
            let skip = (total - t.count as i64).max(0) as usize;
            ctx.sql(&format!("SELECT * FROM {view} OFFSET {skip}"))
                .await
                .map_err(|e| TeckelError::Execution(format!("tail: {e}")))
        }
        Source::FillNa(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_fillna_src";
            ctx.register_table(view, df.clone().into_view())
                .map_err(|e| TeckelError::Execution(format!("fillNa register: {e}")))?;

            let schema_fields: Vec<String> = df
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();

            // Determine target columns: if t.columns is set, only those; otherwise all
            let target_cols: Vec<&str> = match &t.columns {
                Some(cols) => cols.iter().map(|s| s.as_str()).collect(),
                None => schema_fields.iter().map(|s| s.as_str()).collect(),
            };

            // Build per-column fill value from `values` map, falling back to `value`
            let projections: Vec<String> = schema_fields
                .iter()
                .map(|col_name| {
                    if !target_cols.contains(&col_name.as_str()) {
                        return format!("\"{col_name}\"");
                    }
                    // Check per-column values map first
                    if let Some(ref values_map) = t.values {
                        if let Some(prim) = values_map.get(col_name.as_str()) {
                            let fill = primitive_to_sql(prim);
                            return format!("COALESCE(\"{col_name}\", {fill}) AS \"{col_name}\"");
                        }
                    }
                    // Fall back to scalar value
                    if let Some(ref prim) = t.value {
                        let fill = primitive_to_sql(prim);
                        return format!("COALESCE(\"{col_name}\", {fill}) AS \"{col_name}\"");
                    }
                    format!("\"{col_name}\"")
                })
                .collect();

            let query = format!("SELECT {} FROM {view}", projections.join(", "));
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("fillNa: {e}")))
        }
        Source::DropNa(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_dropna_src";
            ctx.register_table(view, df.clone().into_view())
                .map_err(|e| TeckelError::Execution(format!("dropNa register: {e}")))?;

            let target_cols: Vec<&str> = match &t.columns {
                Some(cols) => cols.iter().map(|s| s.as_str()).collect(),
                None => df.schema().fields().iter().map(|f| f.name().as_str()).collect(),
            };

            if let Some(thresh) = t.min_non_nulls {
                // Keep rows with at least `thresh` non-null values among target columns
                let non_null_expr: Vec<String> = target_cols
                    .iter()
                    .map(|c| format!("CASE WHEN \"{c}\" IS NOT NULL THEN 1 ELSE 0 END"))
                    .collect();
                let query = format!(
                    "SELECT * FROM {view} WHERE ({}) >= {thresh}",
                    non_null_expr.join(" + ")
                );
                ctx.sql(&query)
                    .await
                    .map_err(|e| TeckelError::Execution(format!("dropNa thresh: {e}")))
            } else {
                // "any" = drop if any null, "all" = drop if all null
                let condition = match t.how {
                    teckel_model::types::DropNaHow::Any => {
                        // Keep rows where ALL target columns are NOT NULL
                        target_cols
                            .iter()
                            .map(|c| format!("\"{c}\" IS NOT NULL"))
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    }
                    teckel_model::types::DropNaHow::All => {
                        // Keep rows where at least one target column is NOT NULL
                        target_cols
                            .iter()
                            .map(|c| format!("\"{c}\" IS NOT NULL"))
                            .collect::<Vec<_>>()
                            .join(" OR ")
                    }
                };
                let query = format!("SELECT * FROM {view} WHERE {condition}");
                ctx.sql(&query)
                    .await
                    .map_err(|e| TeckelError::Execution(format!("dropNa: {e}")))
            }
        }
        Source::Replace(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_replace_src";
            ctx.register_table(view, df.clone().into_view())
                .map_err(|e| TeckelError::Execution(format!("replace register: {e}")))?;

            let schema_fields: Vec<String> = df
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();

            let target_cols: Vec<&str> = match &t.columns {
                None => schema_fields.iter().map(|s| s.as_str()).collect(),
                Some(c) => c.iter().map(|s| s.as_str()).collect(),
            };

            let projections: Vec<String> = schema_fields
                .iter()
                .map(|col_name| {
                    if target_cols.contains(&col_name.as_str()) && !t.mappings.is_empty() {
                        // Build nested CASE for replacements
                        let mut case_sql = "CASE".to_string();
                        for replacement in &t.mappings {
                            let old_val = primitive_to_sql(&replacement.old);
                            let new_val = primitive_to_sql(&replacement.new);
                            case_sql.push_str(&format!(
                                " WHEN CAST(\"{col_name}\" AS VARCHAR) = {old_val} THEN {new_val}"
                            ));
                        }
                        case_sql.push_str(&format!(" ELSE \"{col_name}\" END AS \"{col_name}\""));
                        case_sql
                    } else {
                        format!("\"{col_name}\"")
                    }
                })
                .collect();

            let query = format!("SELECT {} FROM {view}", projections.join(", "));
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("replace: {e}")))
        }
        Source::Merge(_) => {
            Err(TeckelError::Execution(
                "MERGE transformation requires a mutable table provider. Use the Spark backend for full MERGE INTO support, or decompose into join + filter + union.".to_string()
            ))
        }
        Source::Parse(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_parse_src";
            ctx.register_table(view, df.into_view())
                .map_err(|e| TeckelError::Execution(format!("parse register: {e}")))?;

            match t.format {
                teckel_model::types::ParseFormat::Json => {
                    // Extract JSON fields using arrow_cast + json functions
                    if let Some(ref schema_cols) = t.schema {
                        let extracts: Vec<String> = schema_cols
                            .iter()
                            .map(|sc| {
                                format!(
                                    "CAST(json_extract_scalar(\"{col}\", '$.{name}') AS {dtype}) AS \"{name}\"",
                                    col = t.column,
                                    name = sc.name,
                                    dtype = sc.data_type,
                                )
                            })
                            .collect();
                        // Project all original columns plus parsed fields
                        let query = format!(
                            "SELECT *, {} FROM {view}",
                            extracts.join(", ")
                        );
                        ctx.sql(&query)
                            .await
                            .map_err(|e| TeckelError::Execution(format!("parse json: {e}")))
                    } else {
                        // No schema provided — pass through
                        ctx.table(view)
                            .await
                            .map_err(|e| TeckelError::Execution(format!("parse json passthrough: {e}")))
                    }
                }
                teckel_model::types::ParseFormat::Csv => {
                    Err(TeckelError::Execution(
                        "Parse transformation for CSV format is not yet supported in DataFusion. Use the Spark backend or decompose with SQL expressions.".to_string()
                    ))
                }
            }
        }
        Source::AsOfJoin(t) => {
            // Emulate as-of join via window-based approach: join with direction filter,
            // then pick the closest match per left row using ROW_NUMBER.
            let left_df = get(cache, &t.left)?;
            let right_df = get(cache, &t.right)?;
            let left_view = "__teckel_asof_left";
            let right_view = "__teckel_asof_right";
            ctx.register_table(left_view, left_df.into_view())
                .map_err(|e| TeckelError::Execution(format!("as-of join register left: {e}")))?;
            ctx.register_table(right_view, right_df.into_view())
                .map_err(|e| TeckelError::Execution(format!("as-of join register right: {e}")))?;

            let on_clause = if t.on.is_empty() {
                "1=1".to_string()
            } else {
                t.on.join(" AND ")
            };
            let direction_filter = match t.direction {
                teckel_model::types::AsOfDirection::Backward => {
                    format!("{right_view}.\"{}\" <= {left_view}.\"{}\"", t.right_as_of, t.left_as_of)
                }
                teckel_model::types::AsOfDirection::Forward => {
                    format!("{right_view}.\"{}\" >= {left_view}.\"{}\"", t.right_as_of, t.left_as_of)
                }
                teckel_model::types::AsOfDirection::Nearest => "1=1".to_string(),
            };
            let order_expr = match t.direction {
                teckel_model::types::AsOfDirection::Backward => {
                    format!("{right_view}.\"{}\" DESC", t.right_as_of)
                }
                teckel_model::types::AsOfDirection::Forward => {
                    format!("{right_view}.\"{}\" ASC", t.right_as_of)
                }
                teckel_model::types::AsOfDirection::Nearest => {
                    format!(
                        "ABS(EXTRACT(EPOCH FROM ({right_view}.\"{}\" - {left_view}.\"{}\"))) ASC",
                        t.right_as_of, t.left_as_of
                    )
                }
            };

            let sql = format!(
                "SELECT * FROM (\
                    SELECT {left_view}.*, {right_view}.*, \
                    ROW_NUMBER() OVER (PARTITION BY {left_view}.\"{}\" ORDER BY {}) AS __teckel_rn \
                    FROM {left_view} \
                    JOIN {right_view} ON {} AND {}\
                ) WHERE __teckel_rn = 1",
                t.left_as_of, order_expr, on_clause, direction_filter
            );
            ctx.sql(&sql)
                .await
                .map_err(|e| TeckelError::Execution(format!("as-of join: {e}")))
        }
        Source::LateralJoin(t) => {
            // Emulate lateral join as a regular join (DataFusion lacks full LATERAL support).
            // For true correlated subqueries, use the Spark backend.
            let left_df = get(cache, &t.left)?;
            let right_df = get(cache, &t.right)?;
            let left_view = "__teckel_lateral_left";
            let right_view = "__teckel_lateral_right";
            ctx.register_table(left_view, left_df.into_view())
                .map_err(|e| TeckelError::Execution(format!("lateral join register left: {e}")))?;
            ctx.register_table(right_view, right_df.into_view())
                .map_err(|e| TeckelError::Execution(format!("lateral join register right: {e}")))?;

            let join_type_sql = match t.join_type {
                teckel_model::types::JoinType::Inner => "JOIN",
                teckel_model::types::JoinType::Left => "LEFT JOIN",
                teckel_model::types::JoinType::Cross => "CROSS JOIN",
                teckel_model::types::JoinType::Right => "RIGHT JOIN",
                teckel_model::types::JoinType::Outer => "FULL OUTER JOIN",
                teckel_model::types::JoinType::LeftSemi => "LEFT SEMI JOIN",
                teckel_model::types::JoinType::LeftAnti => "LEFT ANTI JOIN",
            };
            let on_clause = if t.on.is_empty() {
                String::new()
            } else {
                format!(" ON {}", t.on.join(" AND "))
            };
            let sql = format!(
                "SELECT * FROM {left_view} {join_type_sql} {right_view}{on_clause}"
            );
            ctx.sql(&sql)
                .await
                .map_err(|e| TeckelError::Execution(format!("lateral join: {e}")))
        }
        Source::Transpose(t) => {
            // Transpose: convert columns to rows and vice versa.
            // Implemented via UNION ALL of each non-index column.
            let df = get(cache, &t.from)?;
            let view = "__teckel_transpose_src";
            ctx.register_table(view, df.clone().into_view())
                .map_err(|e| TeckelError::Execution(format!("transpose register: {e}")))?;

            let schema_fields: Vec<String> = df
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();

            let index_cols: &[String] = &t.index_columns;
            let value_cols: Vec<&String> = schema_fields
                .iter()
                .filter(|f| !index_cols.contains(f))
                .collect();

            if value_cols.is_empty() {
                return ctx
                    .table(view)
                    .await
                    .map_err(|e| TeckelError::Execution(format!("transpose empty: {e}")));
            }

            let index_select = if index_cols.is_empty() {
                String::new()
            } else {
                let cols = index_cols
                    .iter()
                    .map(|c| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{cols}, ")
            };

            let unions: Vec<String> = value_cols
                .iter()
                .map(|col_name| {
                    format!(
                        "SELECT {index_select}'{col_name}' AS \"column_name\", CAST(\"{col_name}\" AS VARCHAR) AS \"value\" FROM {view}"
                    )
                })
                .collect();

            let query = unions.join(" UNION ALL ");
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("transpose: {e}")))
        }
        Source::GroupingSets(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_gssets_src";
            ctx.register_table(view, df.into_view())
                .map_err(|e| TeckelError::Execution(format!("groupingSets register: {e}")))?;
            let sets_sql: Vec<String> = t
                .sets
                .iter()
                .map(|set| {
                    let cols = set.join(", ");
                    format!("({cols})")
                })
                .collect();
            let agg_exprs = t.agg.join(", ");
            // Collect all columns mentioned in any set for the SELECT clause
            let all_group_cols: Vec<String> = t
                .sets
                .iter()
                .flat_map(|set| set.iter().cloned())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            let select_cols = all_group_cols.join(", ");
            let query = format!(
                "SELECT {select_cols}, {agg_exprs} FROM {view} GROUP BY GROUPING SETS({})",
                sets_sql.join(", ")
            );
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("groupingSets: {e}")))
        }
        Source::Describe(t) => {
            // Produce summary statistics: count, mean, stddev, min, max for numeric columns.
            let df = get(cache, &t.from)?;
            let view = "__teckel_describe_src";
            ctx.register_table(view, df.clone().into_view())
                .map_err(|e| TeckelError::Execution(format!("describe register: {e}")))?;

            let target_cols: Vec<String> = match &t.columns {
                Some(cols) => cols.clone(),
                None => df
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect(),
            };

            let default_stats = vec!["count", "mean", "stddev", "min", "max"];
            let stats: Vec<&str> = match &t.statistics {
                Some(s) => s.iter().map(|x| x.as_str()).collect(),
                None => default_stats,
            };

            let mut stat_queries = Vec::new();
            for stat in &stats {
                let projections: Vec<String> = target_cols
                    .iter()
                    .map(|c| {
                        let expr = match *stat {
                            "count" => format!("CAST(COUNT(\"{c}\") AS VARCHAR)"),
                            "mean" | "avg" => format!("CAST(AVG(CAST(\"{c}\" AS DOUBLE)) AS VARCHAR)"),
                            "stddev" | "std" => {
                                format!("CAST(STDDEV(CAST(\"{c}\" AS DOUBLE)) AS VARCHAR)")
                            }
                            "min" => format!("CAST(MIN(\"{c}\") AS VARCHAR)"),
                            "max" => format!("CAST(MAX(\"{c}\") AS VARCHAR)"),
                            other => format!("'{other}: unsupported'"),
                        };
                        format!("{expr} AS \"{c}\"")
                    })
                    .collect();
                stat_queries.push(format!(
                    "SELECT '{stat}' AS \"statistic\", {} FROM {view}",
                    projections.join(", ")
                ));
            }

            let query = stat_queries.join(" UNION ALL ");
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("describe: {e}")))
        }
        Source::Crosstab(t) => {
            // Crosstab: frequency table of two columns.
            let df = get(cache, &t.from)?;
            let view = "__teckel_crosstab_src";
            ctx.register_table(view, df.clone().into_view())
                .map_err(|e| TeckelError::Execution(format!("crosstab register: {e}")))?;

            // Get distinct values of col2 to use as column headers
            let distinct_query = format!(
                "SELECT DISTINCT \"{}\" FROM {view} ORDER BY \"{}\"",
                t.col2, t.col2
            );
            let distinct_df = ctx
                .sql(&distinct_query)
                .await
                .map_err(|e| TeckelError::Execution(format!("crosstab distinct: {e}")))?;
            let batches = distinct_df
                .collect()
                .await
                .map_err(|e| TeckelError::Execution(format!("crosstab collect: {e}")))?;
            let mut col2_values = Vec::new();
            for batch in &batches {
                let arr = batch.column(0);
                let str_arr = arrow_array::cast::as_string_array(arr);
                for i in 0..str_arr.len() {
                    if !str_arr.is_null(i) {
                        col2_values.push(str_arr.value(i).to_string());
                    }
                }
            }

            // Build conditional aggregation: COUNT(*) FILTER (WHERE col2 = 'val') for each value
            let pivot_exprs: Vec<String> = col2_values
                .iter()
                .map(|val| {
                    format!(
                        "COUNT(*) FILTER (WHERE \"{}\" = '{val}') AS \"{val}\"",
                        t.col2
                    )
                })
                .collect();

            let query = format!(
                "SELECT \"{}\", {} FROM {view} GROUP BY \"{}\"",
                t.col1,
                pivot_exprs.join(", "),
                t.col1
            );
            ctx.sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("crosstab: {e}")))
        }
        Source::Hint(t) => {
            // Hints are optimizer directives — in DataFusion they are not directly
            // applicable. Log and pass through.
            let df = get(cache, &t.from)?;
            for hint in &t.hints {
                tracing::info!(
                    hint = hint.name.as_str(),
                    params = ?hint.parameters,
                    "ignoring optimizer hint (not supported in DataFusion backend)"
                );
            }
            Ok(df)
        }
        // I/O handled by the executor, not here
        Source::Input(_) | Source::Output(_) => Err(TeckelError::Execution(
            "Input/Output should be handled by the executor, not apply()".to_string(),
        )),
    }
}

/// Convert a Primitive value to a SQL literal string.
fn primitive_to_sql(p: &teckel_model::types::Primitive) -> String {
    match p {
        teckel_model::types::Primitive::Bool(b) => b.to_string(),
        teckel_model::types::Primitive::Int(i) => i.to_string(),
        teckel_model::types::Primitive::Float(f) => f.to_string(),
        teckel_model::types::Primitive::String(s) => {
            let escaped = s.replace('\'', "''");
            format!("'{escaped}'")
        }
    }
}

fn get(cache: &BTreeMap<String, DataFrame>, name: &str) -> Result<DataFrame, TeckelError> {
    cache.get(name).cloned().ok_or_else(|| {
        TeckelError::Execution(format!(
            "asset \"{name}\" not found (dependency not yet computed?)"
        ))
    })
}

/// Recursively flatten struct fields into SQL projections.
fn flatten_fields(
    fields: &arrow_types::Fields,
    prefix: &str,
    separator: &str,
    out: &mut Vec<String>,
) {
    for field in fields.iter() {
        let name = field.name();
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}{separator}{name}")
        };

        match field.data_type() {
            arrow_types::DataType::Struct(sub_fields) => {
                // Recurse into nested struct
                for sub_field in sub_fields.iter() {
                    let sub_name = sub_field.name();
                    let alias = format!("{path}{separator}{sub_name}");
                    match sub_field.data_type() {
                        arrow_types::DataType::Struct(deeper) => {
                            // Continue recursion
                            let sub_path = if prefix.is_empty() {
                                format!("\"{name}\".\"{sub_name}\"")
                            } else {
                                format!("{prefix}.\"{name}\".\"{sub_name}\"")
                            };
                            flatten_struct_recursive(&sub_path, &alias, separator, deeper, out);
                        }
                        _ => {
                            let access = if prefix.is_empty() {
                                format!("\"{name}\".\"{sub_name}\"")
                            } else {
                                format!("{prefix}.\"{name}\".\"{sub_name}\"")
                            };
                            out.push(format!("{access} AS \"{alias}\""));
                        }
                    }
                }
            }
            _ => {
                // Non-struct field — project directly
                let access = if prefix.is_empty() {
                    format!("\"{name}\"")
                } else {
                    format!("{prefix}.\"{name}\"")
                };
                out.push(format!("{access} AS \"{path}\""));
            }
        }
    }
}

fn flatten_struct_recursive(
    sql_path: &str,
    alias_prefix: &str,
    separator: &str,
    fields: &arrow_types::Fields,
    out: &mut Vec<String>,
) {
    for field in fields.iter() {
        let name = field.name();
        let access = format!("{sql_path}.\"{name}\"");
        let alias = format!("{alias_prefix}{separator}{name}");
        match field.data_type() {
            arrow_types::DataType::Struct(sub) => {
                flatten_struct_recursive(&access, &alias, separator, sub, out);
            }
            _ => {
                out.push(format!("{access} AS \"{alias}\""));
            }
        }
    }
}

/// HTTP call with exponential backoff retries for 5xx and timeouts.
async fn call_with_retries(
    client: &reqwest::Client,
    url: &str,
    method: &str,
    headers: &BTreeMap<String, String>,
    max_retries: u32,
) -> Result<String, String> {
    let mut attempt = 0;
    loop {
        let mut req = match method.to_uppercase().as_str() {
            "POST" => client.post(url),
            "PUT" => client.put(url),
            _ => client.get(url),
        };
        for (k, v) in headers {
            req = req.header(k.as_str(), v.as_str());
        }

        match req.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                if (200..300).contains(&status) {
                    return resp.text().await.map_err(|e| e.to_string());
                }
                if status >= 500 && attempt < max_retries {
                    attempt += 1;
                    let delay = std::time::Duration::from_millis(100 * 2u64.pow(attempt));
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(format!("HTTP {status}"));
            }
            Err(e) => {
                if attempt < max_retries {
                    attempt += 1;
                    let delay = std::time::Duration::from_millis(100 * 2u64.pow(attempt));
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(e.to_string());
            }
        }
    }
}

fn parse_expr_with_schema(
    ctx: &SessionContext,
    expr_str: &str,
    schema: &DFSchema,
) -> Result<Expr, TeckelError> {
    ctx.parse_sql_expr(expr_str, schema)
        .map_err(|e| TeckelError::Execution(format!("invalid expression \"{expr_str}\": {e}")))
}

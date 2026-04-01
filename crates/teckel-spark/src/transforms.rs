use spark_connect_rs::dataframe::DataFrame;
use spark_connect_rs::functions::{col, expr};
use spark_connect_rs::SparkSession;
use std::collections::BTreeMap;
use teckel_model::source::Source;
use teckel_model::TeckelError;

fn get(cache: &BTreeMap<String, DataFrame>, name: &str) -> Result<DataFrame, TeckelError> {
    cache
        .get(name)
        .cloned()
        .ok_or_else(|| TeckelError::Execution(format!("asset \"{name}\" not found in cache")))
}

pub async fn apply(
    session: &SparkSession,
    source: &Source,
    cache: &BTreeMap<String, DataFrame>,
) -> Result<DataFrame, TeckelError> {
    match source {
        Source::Select(t) => {
            let df = get(cache, &t.from)?;
            let cols: Vec<spark_connect_rs::column::Column> =
                t.columns.iter().map(|c| expr(c.as_str())).collect();
            Ok(df.select(cols))
        }
        Source::Where(t) => {
            let df = get(cache, &t.from)?;
            Ok(df.filter(t.filter.as_str()))
        }
        Source::GroupBy(t) => {
            let df = get(cache, &t.from)?;
            // Use SQL for group by since spark-connect-rs GroupedData returns RecordBatch
            let view = "__teckel_groupby_src";
            df.create_or_replace_temp_view(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("group by register: {e}")))?;

            let group_cols = t.by.join(", ");
            let agg_exprs = t.agg.join(", ");
            let query =
                format!("SELECT {group_cols}, {agg_exprs} FROM {view} GROUP BY {group_cols}");
            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("group by: {e}")))
        }
        Source::OrderBy(t) => {
            let df = get(cache, &t.from)?;
            let sort_cols: Vec<spark_connect_rs::column::Column> = t
                .columns
                .iter()
                .map(|sc| match sc {
                    teckel_model::types::SortColumn::Simple(name) => col(name.as_str()).asc(),
                    teckel_model::types::SortColumn::Explicit {
                        column,
                        direction,
                        nulls,
                    } => {
                        let c = col(column.as_str());
                        match (direction, nulls) {
                            (
                                teckel_model::types::SortDirection::Asc,
                                teckel_model::types::NullOrdering::First,
                            ) => c.asc_nulls_first(),
                            (
                                teckel_model::types::SortDirection::Asc,
                                teckel_model::types::NullOrdering::Last,
                            ) => c.asc_nulls_last(),
                            (
                                teckel_model::types::SortDirection::Desc,
                                teckel_model::types::NullOrdering::First,
                            ) => c.desc_nulls_first(),
                            (
                                teckel_model::types::SortDirection::Desc,
                                teckel_model::types::NullOrdering::Last,
                            ) => c.desc_nulls_last(),
                        }
                    }
                })
                .collect();
            Ok(df.sort(sort_cols))
        }
        Source::Join(t) => {
            // Use SQL for joins since spark-connect-rs join API differs from teckel's model
            let mut views_to_register = vec![(&t.left, "__teckel_join_left")];
            for (i, target) in t.right.iter().enumerate() {
                views_to_register.push((
                    &target.name,
                    Box::leak(format!("__teckel_join_right_{i}").into_boxed_str()),
                ));
            }

            for (asset_ref, view_name) in &views_to_register {
                let df = get(cache, asset_ref)?;
                df.create_or_replace_temp_view(view_name)
                    .await
                    .map_err(|e| TeckelError::Execution(format!("join register: {e}")))?;
            }

            let mut query = "SELECT * FROM __teckel_join_left".to_string();
            for (i, target) in t.right.iter().enumerate() {
                let join_type = match target.join_type {
                    teckel_model::types::JoinType::Inner => "INNER JOIN",
                    teckel_model::types::JoinType::Left => "LEFT JOIN",
                    teckel_model::types::JoinType::Right => "RIGHT JOIN",
                    teckel_model::types::JoinType::Outer => "FULL OUTER JOIN",
                    teckel_model::types::JoinType::Cross => "CROSS JOIN",
                    teckel_model::types::JoinType::LeftSemi => "LEFT SEMI JOIN",
                    teckel_model::types::JoinType::LeftAnti => "LEFT ANTI JOIN",
                };
                let condition = target.on.join(" AND ");
                if matches!(target.join_type, teckel_model::types::JoinType::Cross) {
                    query.push_str(&format!(" {join_type} __teckel_join_right_{i}"));
                } else {
                    query.push_str(&format!(
                        " {join_type} __teckel_join_right_{i} ON {condition}"
                    ));
                }
            }

            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("join: {e}")))
        }
        Source::Union(t) => {
            let mut dfs = t
                .sources
                .iter()
                .map(|s| get(cache, s))
                .collect::<Result<Vec<_>, _>>()?;
            let mut result = dfs.remove(0);
            for df in dfs {
                result = if t.all {
                    result
                        .union_all(df)
                        .map_err(|e| TeckelError::Execution(format!("union: {e}")))?
                } else {
                    result
                        .union(df)
                        .map_err(|e| TeckelError::Execution(format!("union: {e}")))?
                };
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
            left.except_all(right)
                .map_err(|e| TeckelError::Execution(format!("except: {e}")))
        }
        Source::Distinct(t) => {
            let df = get(cache, &t.from)?;
            Ok(df.distinct())
        }
        Source::Limit(t) => {
            let df = get(cache, &t.from)?;
            Ok(df.limit(t.count as i32))
        }
        Source::AddColumns(t) => {
            let df = get(cache, &t.from)?;
            let cols: Vec<(&str, spark_connect_rs::column::Column)> = t
                .columns
                .iter()
                .map(|c| (c.name.as_str(), expr(c.expression.as_str())))
                .collect();
            Ok(df.with_columns(cols))
        }
        Source::DropColumns(t) => {
            let df = get(cache, &t.from)?;
            let cols: Vec<&str> = t.columns.iter().map(|s| s.as_str()).collect();
            Ok(df.drop(cols))
        }
        Source::RenameColumns(t) => {
            let df = get(cache, &t.from)?;
            let mappings: Vec<(&str, &str)> = t
                .mappings
                .iter()
                .map(|(old, new)| (old.as_str(), new.as_str()))
                .collect();
            Ok(df.with_columns_renamed(mappings))
        }
        Source::Sql(t) => {
            for view_name in &t.views {
                let df = get(cache, view_name)?;
                df.create_or_replace_temp_view(view_name)
                    .await
                    .map_err(|e| {
                        TeckelError::Execution(format!("register view \"{view_name}\": {e}"))
                    })?;
            }
            session
                .sql(&t.query)
                .await
                .map_err(|e| TeckelError::Execution(format!("sql: {e}")))
        }
        Source::CastColumns(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_cast_src";
            df.create_or_replace_temp_view(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("cast register: {e}")))?;

            let schema = session
                .sql(&format!("SELECT * FROM {view} LIMIT 0"))
                .await
                .map_err(|e| TeckelError::Execution(format!("cast schema: {e}")))?
                .columns()
                .await
                .map_err(|e| TeckelError::Execution(format!("cast columns: {e}")))?;

            let cast_map: BTreeMap<&str, &str> = t
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.target_type.as_str()))
                .collect();

            let projections: Vec<String> = schema
                .iter()
                .map(|name| {
                    if let Some(target_type) = cast_map.get(name.as_str()) {
                        format!("CAST(`{name}` AS {target_type}) AS `{name}`")
                    } else {
                        format!("`{name}`")
                    }
                })
                .collect();

            let query = format!("SELECT {} FROM {view}", projections.join(", "));
            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("cast columns: {e}")))
        }
        Source::Window(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_window_src";
            df.create_or_replace_temp_view(view)
                .await
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
                .map(|f| format!("{} OVER ({window_spec}) AS `{}`", f.expression, f.alias))
                .collect();

            let all_cols = format!("*, {}", func_exprs.join(", "));
            let query = format!("SELECT {all_cols} FROM {view}");
            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("window: {e}")))
        }
        Source::Pivot(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_pivot_src";
            df.create_or_replace_temp_view(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("pivot register: {e}")))?;

            let group_by_cols = t.group_by.join(", ");
            let agg_exprs = t.agg.join(", ");
            let values_clause = match &t.values {
                Some(vals) => vals
                    .iter()
                    .map(|v| format!("'{v}'"))
                    .collect::<Vec<_>>()
                    .join(", "),
                None => {
                    return Err(TeckelError::Execution(
                        "pivot requires explicit values for Spark backend".to_string(),
                    ))
                }
            };
            let query = format!(
                "SELECT * FROM (SELECT {group_by_cols}, {} FROM {view}) PIVOT ({agg_exprs} FOR {} IN ({values_clause}))",
                t.pivot_column, t.pivot_column
            );
            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("pivot: {e}")))
        }
        Source::Unpivot(t) => {
            let df = get(cache, &t.from)?;
            let ids: Vec<spark_connect_rs::column::Column> =
                t.ids.iter().map(|c| col(c.as_str())).collect();
            let values: Vec<spark_connect_rs::column::Column> =
                t.values.iter().map(|c| col(c.as_str())).collect();
            Ok(df.unpivot(ids, Some(values), &t.variable_column, &t.value_column))
        }
        Source::Sample(t) => {
            let df = get(cache, &t.from)?;
            Ok(df.sample(0.0, t.fraction, Some(t.with_replacement), t.seed))
        }
        Source::Coalesce(t) => {
            let df = get(cache, &t.from)?;
            Ok(df.coalesce(t.num_partitions))
        }
        Source::Repartition(t) => {
            let df = get(cache, &t.from)?;
            Ok(df.repartition(t.num_partitions, None))
        }

        // ── v3 transformations (via Spark SQL) ────────────────────
        Source::Offset(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_offset_src";
            df.create_or_replace_temp_view(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("offset register: {e}")))?;
            let query = format!("SELECT * FROM {view} OFFSET {}", t.count);
            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("offset: {e}")))
        }
        Source::Tail(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_tail_src";
            df.create_or_replace_temp_view(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("tail register: {e}")))?;
            // Spark SQL: get total count, then OFFSET (total - N)
            let count_df = session
                .sql(&format!("SELECT COUNT(*) AS cnt FROM {view}"))
                .await
                .map_err(|e| TeckelError::Execution(format!("tail count: {e}")))?;
            let count_view = "__teckel_tail_cnt";
            count_df
                .create_or_replace_temp_view(count_view)
                .await
                .map_err(|e| TeckelError::Execution(format!("tail count register: {e}")))?;
            // Use a subquery to compute the offset dynamically
            let query = format!(
                "SELECT * FROM {view} LIMIT {} OFFSET (SELECT GREATEST(cnt - {}, 0) FROM {count_view})",
                t.count, t.count
            );
            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("tail: {e}")))
        }
        Source::FillNa(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_fillna_src";
            df.create_or_replace_temp_view(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("fillNa register: {e}")))?;

            let schema = session
                .sql(&format!("SELECT * FROM {view} LIMIT 0"))
                .await
                .map_err(|e| TeckelError::Execution(format!("fillNa schema: {e}")))?
                .columns()
                .await
                .map_err(|e| TeckelError::Execution(format!("fillNa columns: {e}")))?;

            // Determine target columns
            let target_cols: Vec<&str> = match &t.columns {
                Some(cols) => cols.iter().map(|s| s.as_str()).collect(),
                None => schema.iter().map(|s| s.as_str()).collect(),
            };

            let projections: Vec<String> = schema
                .iter()
                .map(|col_name| {
                    if !target_cols.contains(&col_name.as_str()) {
                        return format!("`{col_name}`");
                    }
                    // Check per-column values map first
                    if let Some(ref values_map) = t.values {
                        if let Some(prim) = values_map.get(col_name.as_str()) {
                            let fill = spark_primitive_to_sql(prim);
                            return format!("COALESCE(`{col_name}`, {fill}) AS `{col_name}`");
                        }
                    }
                    // Fall back to scalar value
                    if let Some(ref prim) = t.value {
                        let fill = spark_primitive_to_sql(prim);
                        return format!("COALESCE(`{col_name}`, {fill}) AS `{col_name}`");
                    }
                    format!("`{col_name}`")
                })
                .collect();

            let query = format!("SELECT {} FROM {view}", projections.join(", "));
            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("fillNa: {e}")))
        }
        Source::DropNa(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_dropna_src";
            df.create_or_replace_temp_view(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("dropNa register: {e}")))?;

            let schema = session
                .sql(&format!("SELECT * FROM {view} LIMIT 0"))
                .await
                .map_err(|e| TeckelError::Execution(format!("dropNa schema: {e}")))?
                .columns()
                .await
                .map_err(|e| TeckelError::Execution(format!("dropNa columns: {e}")))?;

            let target_cols: Vec<String> = match &t.columns {
                Some(cols) => cols.clone(),
                None => schema,
            };

            if let Some(thresh) = t.min_non_nulls {
                let non_null_expr: Vec<String> = target_cols
                    .iter()
                    .map(|c| format!("CASE WHEN `{c}` IS NOT NULL THEN 1 ELSE 0 END"))
                    .collect();
                let query = format!(
                    "SELECT * FROM {view} WHERE ({}) >= {thresh}",
                    non_null_expr.join(" + ")
                );
                session
                    .sql(&query)
                    .await
                    .map_err(|e| TeckelError::Execution(format!("dropNa thresh: {e}")))
            } else {
                let condition = match t.how {
                    teckel_model::types::DropNaHow::Any => target_cols
                        .iter()
                        .map(|c| format!("`{c}` IS NOT NULL"))
                        .collect::<Vec<_>>()
                        .join(" AND "),
                    teckel_model::types::DropNaHow::All => target_cols
                        .iter()
                        .map(|c| format!("`{c}` IS NOT NULL"))
                        .collect::<Vec<_>>()
                        .join(" OR "),
                };
                let query = format!("SELECT * FROM {view} WHERE {condition}");
                session
                    .sql(&query)
                    .await
                    .map_err(|e| TeckelError::Execution(format!("dropNa: {e}")))
            }
        }
        Source::Replace(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_replace_src";
            df.create_or_replace_temp_view(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("replace register: {e}")))?;

            let schema = session
                .sql(&format!("SELECT * FROM {view} LIMIT 0"))
                .await
                .map_err(|e| TeckelError::Execution(format!("replace schema: {e}")))?
                .columns()
                .await
                .map_err(|e| TeckelError::Execution(format!("replace columns: {e}")))?;

            let target_cols: Vec<String> = match &t.columns {
                Some(cols) => cols.clone(),
                None => schema.clone(),
            };

            let projections: Vec<String> = schema
                .iter()
                .map(|col_name| {
                    if target_cols.iter().any(|c| c == col_name) && !t.mappings.is_empty() {
                        let mut case_sql = String::from("CASE");
                        for replacement in &t.mappings {
                            let old_val = spark_primitive_to_sql(&replacement.old);
                            let new_val = spark_primitive_to_sql(&replacement.new);
                            case_sql.push_str(&format!(
                                " WHEN CAST(`{col_name}` AS STRING) = {old_val} THEN {new_val}"
                            ));
                        }
                        case_sql.push_str(&format!(" ELSE `{col_name}` END AS `{col_name}`"));
                        case_sql
                    } else {
                        format!("`{col_name}`")
                    }
                })
                .collect();

            let query = format!("SELECT {} FROM {view}", projections.join(", "));
            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("replace: {e}")))
        }
        Source::Merge(t) => {
            // Spark SQL supports MERGE INTO natively for Delta tables
            let target_df = get(cache, &t.target)?;
            let source_df = get(cache, &t.source)?;
            target_df
                .create_or_replace_temp_view("__teckel_merge_target")
                .await
                .map_err(|e| TeckelError::Execution(format!("merge target register: {e}")))?;
            source_df
                .create_or_replace_temp_view("__teckel_merge_source")
                .await
                .map_err(|e| TeckelError::Execution(format!("merge source register: {e}")))?;

            let on_clause = t.on.join(" AND ");
            let mut merge_sql = format!(
                "MERGE INTO __teckel_merge_target t USING __teckel_merge_source s ON {on_clause}"
            );

            for action in &t.when_matched {
                let cond = action
                    .condition
                    .as_ref()
                    .map(|c| format!(" AND {c}"))
                    .unwrap_or_default();
                match action.action {
                    teckel_model::types::MergeActionType::Update => {
                        if action.star {
                            merge_sql.push_str(&format!(" WHEN MATCHED{cond} THEN UPDATE SET *"));
                        } else if let Some(ref set) = action.set {
                            let assignments: Vec<String> =
                                set.iter().map(|(k, v)| format!("t.`{k}` = {v}")).collect();
                            merge_sql.push_str(&format!(
                                " WHEN MATCHED{cond} THEN UPDATE SET {}",
                                assignments.join(", ")
                            ));
                        }
                    }
                    teckel_model::types::MergeActionType::Delete => {
                        merge_sql.push_str(&format!(" WHEN MATCHED{cond} THEN DELETE"));
                    }
                    teckel_model::types::MergeActionType::Insert => {}
                }
            }

            for action in &t.when_not_matched {
                let cond = action
                    .condition
                    .as_ref()
                    .map(|c| format!(" AND {c}"))
                    .unwrap_or_default();
                if matches!(action.action, teckel_model::types::MergeActionType::Insert) {
                    if action.star {
                        merge_sql.push_str(&format!(" WHEN NOT MATCHED{cond} THEN INSERT *"));
                    } else if let Some(ref set) = action.set {
                        let cols: Vec<&str> = set.keys().map(|k| k.as_str()).collect();
                        let vals: Vec<&str> = set.values().map(|v| v.as_str()).collect();
                        merge_sql.push_str(&format!(
                            " WHEN NOT MATCHED{cond} THEN INSERT ({}) VALUES ({})",
                            cols.join(", "),
                            vals.join(", ")
                        ));
                    }
                }
            }

            session
                .sql(&merge_sql)
                .await
                .map_err(|e| TeckelError::Execution(format!("merge: {e}")))
        }
        Source::Parse(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_parse_src";
            df.create_or_replace_temp_view(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("parse register: {e}")))?;

            let func_name = match t.format {
                teckel_model::types::ParseFormat::Json => "from_json",
                teckel_model::types::ParseFormat::Csv => "from_csv",
            };

            if let Some(ref schema_cols) = t.schema {
                let schema_str: Vec<String> = schema_cols
                    .iter()
                    .map(|sc| format!("{} {}", sc.name, sc.data_type))
                    .collect();
                let schema_ddl = schema_str.join(", ");
                let query = format!(
                    "SELECT *, {func_name}(`{}`, '{schema_ddl}') AS __parsed FROM {view}",
                    t.column
                );
                session
                    .sql(&query)
                    .await
                    .map_err(|e| TeckelError::Execution(format!("parse: {e}")))
            } else {
                // No schema — pass through
                session
                    .sql(&format!("SELECT * FROM {view}"))
                    .await
                    .map_err(|e| TeckelError::Execution(format!("parse passthrough: {e}")))
            }
        }
        Source::AsOfJoin(t) => {
            // Spark does not have native AS OF JOIN in SQL;
            // emulate via window function approach.
            let left_df = get(cache, &t.left)?;
            let right_df = get(cache, &t.right)?;
            left_df
                .create_or_replace_temp_view("__teckel_asof_left")
                .await
                .map_err(|e| TeckelError::Execution(format!("asof left register: {e}")))?;
            right_df
                .create_or_replace_temp_view("__teckel_asof_right")
                .await
                .map_err(|e| TeckelError::Execution(format!("asof right register: {e}")))?;

            let on_clause = if t.on.is_empty() {
                String::from("1=1")
            } else {
                t.on.join(" AND ")
            };

            let direction_filter = match t.direction {
                teckel_model::types::AsOfDirection::Backward => {
                    format!("r.`{}` <= l.`{}`", t.right_as_of, t.left_as_of)
                }
                teckel_model::types::AsOfDirection::Forward => {
                    format!("r.`{}` >= l.`{}`", t.right_as_of, t.left_as_of)
                }
                teckel_model::types::AsOfDirection::Nearest => {
                    String::from("1=1") // Handled by ordering below
                }
            };

            let order_expr = match t.direction {
                teckel_model::types::AsOfDirection::Backward => {
                    format!("r.`{}` DESC", t.right_as_of)
                }
                teckel_model::types::AsOfDirection::Forward => {
                    format!("r.`{}`", t.right_as_of)
                }
                teckel_model::types::AsOfDirection::Nearest => {
                    format!(
                        "ABS(CAST(r.`{}` AS LONG) - CAST(l.`{}` AS LONG))",
                        t.right_as_of, t.left_as_of
                    )
                }
            };

            let query = format!(
                "SELECT * FROM ( \
                    SELECT l.*, r.*, ROW_NUMBER() OVER (PARTITION BY l.`{}` ORDER BY {order_expr}) AS __rn \
                    FROM __teckel_asof_left l \
                    JOIN __teckel_asof_right r ON {on_clause} AND {direction_filter} \
                ) WHERE __rn = 1",
                t.left_as_of
            );

            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("asOfJoin: {e}")))
        }
        Source::LateralJoin(t) => {
            let left_df = get(cache, &t.left)?;
            let right_df = get(cache, &t.right)?;
            left_df
                .create_or_replace_temp_view("__teckel_lateral_left")
                .await
                .map_err(|e| TeckelError::Execution(format!("lateral left register: {e}")))?;
            right_df
                .create_or_replace_temp_view("__teckel_lateral_right")
                .await
                .map_err(|e| TeckelError::Execution(format!("lateral right register: {e}")))?;

            let join_type = match t.join_type {
                teckel_model::types::JoinType::Inner => "JOIN",
                teckel_model::types::JoinType::Left => "LEFT JOIN",
                teckel_model::types::JoinType::Cross => "CROSS JOIN",
                _ => "JOIN",
            };
            let on_clause = if t.on.is_empty() {
                String::new()
            } else {
                format!(" ON {}", t.on.join(" AND "))
            };
            // The right dataset is registered as a view; use it in a lateral subquery
            let query = format!(
                "SELECT * FROM __teckel_lateral_left {join_type} LATERAL (SELECT * FROM __teckel_lateral_right){on_clause}"
            );
            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("lateralJoin: {e}")))
        }
        Source::Transpose(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_transpose_src";
            df.create_or_replace_temp_view(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("transpose register: {e}")))?;

            let schema = session
                .sql(&format!("SELECT * FROM {view} LIMIT 0"))
                .await
                .map_err(|e| TeckelError::Execution(format!("transpose schema: {e}")))?
                .columns()
                .await
                .map_err(|e| TeckelError::Execution(format!("transpose columns: {e}")))?;

            let index_cols = &t.index_columns;
            let value_cols: Vec<&String> =
                schema.iter().filter(|c| !index_cols.contains(c)).collect();

            if value_cols.is_empty() {
                return session
                    .sql(&format!("SELECT * FROM {view}"))
                    .await
                    .map_err(|e| TeckelError::Execution(format!("transpose empty: {e}")));
            }

            let index_select = if index_cols.is_empty() {
                String::new()
            } else {
                let idx = index_cols
                    .iter()
                    .map(|c| format!("`{c}`"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{idx}, ")
            };

            let unions: Vec<String> = value_cols
                .iter()
                .map(|col_name| {
                    format!(
                        "SELECT {index_select}'{col_name}' AS `column_name`, CAST(`{col_name}` AS STRING) AS `value` FROM {view}"
                    )
                })
                .collect();
            let query = unions.join(" UNION ALL ");
            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("transpose: {e}")))
        }
        Source::GroupingSets(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_gssets_src";
            df.create_or_replace_temp_view(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("groupingSets register: {e}")))?;
            let sets_sql: Vec<String> = t
                .sets
                .iter()
                .map(|set| format!("({})", set.join(", ")))
                .collect();
            let agg_exprs = t.agg.join(", ");
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
            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("groupingSets: {e}")))
        }
        Source::Describe(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_describe_src";
            df.create_or_replace_temp_view(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("describe register: {e}")))?;
            // Spark has a built-in DESCRIBE TABLE, but for statistics we use SUMMARY
            let query = format!("SELECT * FROM (DESCRIBE {view})");
            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("describe: {e}")))
        }
        Source::Crosstab(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_crosstab_src";
            df.create_or_replace_temp_view(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("crosstab register: {e}")))?;
            // Use Spark's crosstab function via SQL
            let query = format!(
                "SELECT * FROM (SELECT `{}`, `{}` FROM {view}) PIVOT (COUNT(*) FOR `{}` IN (SELECT DISTINCT `{}` FROM {view}))",
                t.col1, t.col2, t.col2, t.col2
            );
            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("crosstab: {e}")))
        }
        Source::Hint(t) => {
            let df = get(cache, &t.from)?;
            let view = "__teckel_hint_src";
            df.create_or_replace_temp_view(view)
                .await
                .map_err(|e| TeckelError::Execution(format!("hint register: {e}")))?;
            // Spark SQL supports /*+ HINT */ syntax
            let hints_sql: Vec<String> = t
                .hints
                .iter()
                .map(|h| {
                    if h.parameters.is_empty() {
                        h.name.clone()
                    } else {
                        format!("{}({})", h.name, h.parameters.join(", "))
                    }
                })
                .collect();
            let query = format!("SELECT /*+ {} */ * FROM {view}", hints_sql.join(", "));
            session
                .sql(&query)
                .await
                .map_err(|e| TeckelError::Execution(format!("hint: {e}")))
        }

        // Fallback for transforms not yet mapped
        source => Err(TeckelError::Execution(format!(
            "transform {:?} is not yet supported by the Spark Connect backend",
            std::mem::discriminant(source)
        ))),
    }
}

fn spark_primitive_to_sql(p: &teckel_model::types::Primitive) -> String {
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

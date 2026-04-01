use polars::prelude::*;
use std::collections::BTreeMap;
use teckel_model::source::Source;
use teckel_model::TeckelError;

pub fn apply(
    source: &Source,
    cache: &BTreeMap<String, DataFrame>,
) -> Result<DataFrame, TeckelError> {
    match source {
        Source::Select(t) => {
            let df = get(cache, &t.from)?;
            let exprs: Vec<Expr> = t.columns.iter().map(|c| parse_polars_expr(c)).collect();
            df.lazy()
                .select(exprs)
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars select: {e}")))
        }
        Source::Where(t) => {
            let df = get(cache, &t.from)?;
            let expr = parse_polars_expr(&t.filter);
            df.lazy()
                .filter(expr)
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars where: {e}")))
        }
        Source::GroupBy(t) => {
            let df = get(cache, &t.from)?;
            let group_exprs: Vec<Expr> = t.by.iter().map(|c| col(c.as_str())).collect();
            let agg_exprs: Vec<Expr> = t.agg.iter().map(|a| parse_polars_expr(a)).collect();
            df.lazy()
                .group_by(group_exprs)
                .agg(agg_exprs)
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars group_by: {e}")))
        }
        Source::OrderBy(t) => {
            let df = get(cache, &t.from)?;
            let mut sort_cols = Vec::new();
            let mut descending = Vec::new();
            let mut nulls_last = Vec::new();

            for sc in &t.columns {
                match sc {
                    teckel_model::types::SortColumn::Simple(name) => {
                        sort_cols.push(col(name.as_str()));
                        descending.push(false);
                        nulls_last.push(true);
                    }
                    teckel_model::types::SortColumn::Explicit {
                        column,
                        direction,
                        nulls,
                    } => {
                        sort_cols.push(col(column.as_str()));
                        descending.push(matches!(
                            direction,
                            teckel_model::types::SortDirection::Desc
                        ));
                        nulls_last.push(matches!(
                            nulls,
                            teckel_model::types::NullOrdering::Last
                        ));
                    }
                }
            }
            let sort_opts = SortMultipleOptions::default()
                .with_order_descending_multi(descending)
                .with_nulls_last_multi(nulls_last);

            df.lazy()
                .sort_by_exprs(sort_cols, sort_opts)
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars order_by: {e}")))
        }
        Source::Join(t) => {
            let mut result = get(cache, &t.left)?.lazy();
            for target in &t.right {
                let right = get(cache, &target.name)?.lazy();
                let _join_type = match target.join_type {
                    teckel_model::types::JoinType::Inner => JoinType::Inner,
                    teckel_model::types::JoinType::Left => JoinType::Left,
                    teckel_model::types::JoinType::Right => JoinType::Right,
                    teckel_model::types::JoinType::Outer => JoinType::Full,
                    teckel_model::types::JoinType::Cross => JoinType::Cross,
                    teckel_model::types::JoinType::LeftSemi => JoinType::Left,
                    teckel_model::types::JoinType::LeftAnti => JoinType::Left,
                };

                if matches!(target.join_type, teckel_model::types::JoinType::Cross) {
                    result = result.cross_join(right, None);
                } else {
                    // Use SQL for complex join conditions
                    let result_df = result.collect()
                        .map_err(|e| TeckelError::Execution(format!("join collect left: {e}")))?;
                    let right_df = right.collect()
                        .map_err(|e| TeckelError::Execution(format!("join collect right: {e}")))?;
                    let mut ctx = polars::sql::SQLContext::new();
                    ctx.register("__left", result_df.lazy());
                    ctx.register("__right", right_df.lazy());
                    let on_clause = target.on.join(" AND ");
                    let jt = match target.join_type {
                        teckel_model::types::JoinType::Inner => "INNER JOIN",
                        teckel_model::types::JoinType::Left => "LEFT JOIN",
                        teckel_model::types::JoinType::Right => "RIGHT JOIN",
                        teckel_model::types::JoinType::Outer => "FULL OUTER JOIN",
                        teckel_model::types::JoinType::LeftSemi => "LEFT SEMI JOIN",
                        teckel_model::types::JoinType::LeftAnti => "LEFT ANTI JOIN",
                        _ => "INNER JOIN",
                    };
                    let query = format!("SELECT * FROM __left {jt} __right ON {on_clause}");
                    result = ctx.execute(&query)
                        .map_err(|e| TeckelError::Execution(format!("polars join sql: {e}")))?;
                }
            }
            result
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars join: {e}")))
        }
        Source::Union(t) => {
            let dfs: Vec<LazyFrame> = t
                .sources
                .iter()
                .map(|s| get(cache, s).map(|df| df.lazy()))
                .collect::<Result<Vec<_>, _>>()?;
            let mut result = concat(dfs, UnionArgs::default())
                .map_err(|e| TeckelError::Execution(format!("polars union: {e}")))?;
            if !t.all {
                result = result.unique(None, UniqueKeepStrategy::First);
            }
            result
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars union collect: {e}")))
        }
        Source::Intersect(t) => {
            // Use SQL for intersect
            let dfs: Vec<DataFrame> = t.sources.iter().map(|s| get(cache, s)).collect::<Result<Vec<_>, _>>()?;
            let mut ctx = polars::sql::SQLContext::new();
            for (i, df) in dfs.iter().enumerate() {
                ctx.register(&format!("__t{i}"), df.clone().lazy());
            }
            let tables: Vec<String> = (0..dfs.len()).map(|i| format!("SELECT * FROM __t{i}")).collect();
            let query = tables.join(" INTERSECT ");
            ctx.execute(&query)
                .map_err(|e| TeckelError::Execution(format!("polars intersect: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars intersect collect: {e}")))
        }
        Source::Except(t) => {
            let left = get(cache, &t.left)?;
            let right = get(cache, &t.right)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__left", left.lazy());
            ctx.register("__right", right.lazy());
            ctx.execute("SELECT * FROM __left EXCEPT SELECT * FROM __right")
                .map_err(|e| TeckelError::Execution(format!("polars except: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars except collect: {e}")))
        }
        Source::Distinct(t) => {
            let df = get(cache, &t.from)?;
            df.unique_stable(None::<Vec<String>>.as_deref(), UniqueKeepStrategy::First, None)
                .map_err(|e| TeckelError::Execution(format!("polars distinct: {e}")))
        }
        Source::Limit(t) => {
            let df = get(cache, &t.from)?;
            Ok(df.head(Some(t.count as usize)))
        }
        Source::AddColumns(t) => {
            let df = get(cache, &t.from)?;
            let mut lf = df.lazy();
            for col_def in &t.columns {
                let expr = parse_polars_expr(&col_def.expression).alias(&col_def.name);
                lf = lf.with_column(expr);
            }
            lf.collect()
                .map_err(|e| TeckelError::Execution(format!("polars add_columns: {e}")))
        }
        Source::DropColumns(t) => {
            let df = get(cache, &t.from)?;
            let cols: Vec<String> = t.columns.clone();
            Ok(df.drop_many(&cols))
        }
        Source::RenameColumns(t) => {
            let mut df = get(cache, &t.from)?;
            for (old, new) in &t.mappings {
                df.rename(old.as_str(), PlSmallStr::from(new.as_str()))
                    .map_err(|e| TeckelError::Execution(format!("polars rename: {e}")))?;
            }
            Ok(df)
        }
        Source::CastColumns(t) => {
            let df = get(cache, &t.from)?;
            let mut lf = df.lazy();
            for cast in &t.columns {
                let dtype = parse_polars_dtype(&cast.target_type);
                lf = lf.with_column(col(cast.name.as_str()).cast(dtype));
            }
            lf.collect()
                .map_err(|e| TeckelError::Execution(format!("polars cast: {e}")))
        }
        Source::Sample(t) => {
            let df = get(cache, &t.from)?;
            let n = (df.height() as f64 * t.fraction) as usize;
            df.sample_n_literal(n, t.with_replacement, false, t.seed.map(|s| s as u64))
                .map_err(|e| TeckelError::Execution(format!("polars sample: {e}")))
        }
        Source::Conditional(t) => {
            // Use SQL CASE WHEN for conditional logic — avoids Polars type system issues
            let df = get(cache, &t.from)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__src", df.lazy());
            let mut case_sql = String::from("CASE");
            for branch in &t.branches {
                case_sql.push_str(&format!(" WHEN {} THEN {}", branch.condition, branch.value));
            }
            if let Some(otherwise) = &t.otherwise {
                case_sql.push_str(&format!(" ELSE {otherwise}"));
            }
            case_sql.push_str(" END");
            let query = format!("SELECT *, {case_sql} AS \"{}\" FROM __src", t.output_column);
            ctx.execute(&query)
                .map_err(|e| TeckelError::Execution(format!("polars conditional: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars conditional collect: {e}")))
        }
        Source::Repartition(t) => get(cache, &t.from),
        Source::Coalesce(t) => get(cache, &t.from),
        Source::Sql(t) => {
            let mut ctx = polars::sql::SQLContext::new();
            for view_name in &t.views {
                let df = get(cache, view_name)?;
                ctx.register(view_name, df.lazy());
            }
            ctx.execute(&t.query)
                .map_err(|e| TeckelError::Execution(format!("polars sql: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars sql collect: {e}")))
        }
        Source::Window(t) => {
            let df = get(cache, &t.from)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__src", df.lazy());
            let partition = t.partition_by.join(", ");
            let order = if t.order_by.is_empty() {
                String::new()
            } else {
                let parts: Vec<String> = t.order_by.iter().map(sort_col_to_sql).collect();
                format!("ORDER BY {}", parts.join(", "))
            };
            let frame = format!(
                "{} BETWEEN {} AND {}",
                match t.frame.frame_type {
                    teckel_model::types::FrameType::Rows => "ROWS",
                    teckel_model::types::FrameType::Range => "RANGE",
                },
                t.frame.start,
                t.frame.end
            );
            let window_spec = format!("PARTITION BY {partition} {order} {frame}");
            let funcs: Vec<String> = t
                .functions
                .iter()
                .map(|f| format!("{} OVER ({window_spec}) AS \"{}\"", f.expression, f.alias))
                .collect();
            let query = format!("SELECT *, {} FROM __src", funcs.join(", "));
            ctx.execute(&query)
                .map_err(|e| TeckelError::Execution(format!("polars window: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars window collect: {e}")))
        }
        Source::Rollup(t) => sql_agg(cache, &t.from, &t.by, &t.agg, "ROLLUP"),
        Source::Cube(t) => sql_agg(cache, &t.from, &t.by, &t.agg, "CUBE"),
        Source::Pivot(t) => {
            let df = get(cache, &t.from)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__src", df.lazy());
            let group_cols = t.group_by.join(", ");
            let values: Vec<String> = match &t.values {
                Some(vals) => vals.clone(),
                None => {
                    let distinct_q = format!("SELECT DISTINCT \"{}\" FROM __src", t.pivot_column);
                    let res = ctx.execute(&distinct_q)
                        .map_err(|e| TeckelError::Execution(format!("pivot distinct: {e}")))?
                        .collect()
                        .map_err(|e| TeckelError::Execution(format!("pivot distinct collect: {e}")))?;
                    res.column(&t.pivot_column)
                        .map_err(|e| TeckelError::Execution(format!("pivot col: {e}")))?
                        .str()
                        .map_err(|e| TeckelError::Execution(format!("pivot str: {e}")))?
                        .into_no_null_iter()
                        .map(|s| s.to_string())
                        .collect()
                }
            };
            let mut pivot_exprs = Vec::new();
            for agg in &t.agg {
                for val in &values {
                    pivot_exprs.push(format!(
                        "{agg} FILTER (WHERE \"{}\" = '{val}') AS \"{val}\"",
                        t.pivot_column
                    ));
                }
            }
            let query = format!(
                "SELECT {group_cols}, {} FROM __src GROUP BY {group_cols}",
                pivot_exprs.join(", ")
            );
            ctx.execute(&query)
                .map_err(|e| TeckelError::Execution(format!("polars pivot: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars pivot collect: {e}")))
        }
        Source::Unpivot(t) => {
            // Use SQL UNION ALL approach for unpivot
            let df = get(cache, &t.from)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__src", df.lazy());
            let id_cols = t.ids.join(", ");
            let unions: Vec<String> = t.values.iter().map(|col_name| {
                format!(
                    "SELECT {id_cols}, '{col_name}' AS \"{}\", \"{col_name}\" AS \"{}\" FROM __src",
                    t.variable_column, t.value_column
                )
            }).collect();
            let query = unions.join(" UNION ALL ");
            ctx.execute(&query)
                .map_err(|e| TeckelError::Execution(format!("polars unpivot: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars unpivot collect: {e}")))
        }
        Source::Assertion(t) => {
            let df = get(cache, &t.from)?;
            for check in &t.checks {
                if check.rule == "not_null" {
                    if let Some(col_name) = &check.column {
                        let null_count = df
                            .column(col_name)
                            .map_err(|e| TeckelError::Execution(format!("assertion col: {e}")))?
                            .null_count();
                        if null_count > 0 {
                            let msg = format!("column \"{col_name}\" has {null_count} null values");
                            match t.on_failure {
                                teckel_model::types::OnFailure::Fail => {
                                    return Err(TeckelError::spec(
                                        teckel_model::TeckelErrorCode::EQuality001,
                                        msg,
                                    ));
                                }
                                teckel_model::types::OnFailure::Warn => tracing::warn!("{msg}"),
                                teckel_model::types::OnFailure::Drop => {
                                    return df
                                        .lazy()
                                        .filter(col(col_name).is_not_null())
                                        .collect()
                                        .map_err(|e| TeckelError::Execution(format!("drop nulls: {e}")));
                                }
                            }
                        }
                    }
                }
            }
            Ok(df)
        }
        Source::SchemaEnforce(t) => {
            let df = get(cache, &t.from)?;
            let select_exprs: Vec<Expr> = t
                .columns
                .iter()
                .map(|c| col(c.name.as_str()).cast(parse_polars_dtype(&c.data_type)))
                .collect();
            df.lazy()
                .select(select_exprs)
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars schema enforce: {e}")))
        }
        Source::Scd2(t) => {
            let current = get(cache, &t.current)?;
            let incoming = get(cache, &t.incoming)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__current", current.lazy());
            ctx.register("__incoming", incoming.lazy());
            let key_join = t.key_columns.iter()
                .map(|k| format!("c.\"{k}\" = i.\"{k}\""))
                .collect::<Vec<_>>().join(" AND ");
            let track_changed = t.track_columns.iter()
                .map(|tc| format!("c.\"{tc}\" IS DISTINCT FROM i.\"{tc}\""))
                .collect::<Vec<_>>().join(" OR ");
            let query = format!(
                "SELECT c.* FROM __current c LEFT JOIN __incoming i ON {key_join} \
                 WHERE i.\"{}\" IS NULL OR NOT ({track_changed}) \
                 UNION ALL \
                 SELECT i.* FROM __incoming i INNER JOIN __current c ON {key_join} \
                 WHERE {track_changed} \
                 UNION ALL \
                 SELECT i.* FROM __incoming i LEFT JOIN __current c ON {key_join} \
                 WHERE c.\"{}\" IS NULL",
                t.key_columns[0], t.key_columns[0]
            );
            ctx.execute(&query)
                .map_err(|e| TeckelError::Execution(format!("polars scd2: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars scd2 collect: {e}")))
        }
        Source::Flatten(_) => Err(TeckelError::Execution(
            "flatten not yet implemented for polars backend".to_string(),
        )),
        Source::Enrich(_) => Err(TeckelError::Execution(
            "enrich not yet implemented for polars backend (use datafusion)".to_string(),
        )),
        Source::Custom(_) => Err(TeckelError::spec(
            teckel_model::TeckelErrorCode::EComp001,
            "custom components not supported in polars backend",
        )),
        Source::Split(_) => Err(TeckelError::Execution(
            "split should have been expanded by parser".to_string(),
        )),
        // ── v3 transformations ────────────────────────────────────
        Source::Offset(t) => {
            let df = get(cache, &t.from)?;
            Ok(df.slice(t.count as i64, df.height()))
        }
        Source::Tail(t) => {
            let df = get(cache, &t.from)?;
            Ok(df.tail(Some(t.count as usize)))
        }
        Source::FillNa(t) => {
            let df = get(cache, &t.from)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__src", df.clone().lazy());
            let all_cols: Vec<String> = df.get_column_names_str().iter().map(|s| s.to_string()).collect();

            // Determine target columns: if t.columns is set, only those; otherwise all
            let target_cols: Vec<&str> = match &t.columns {
                Some(cols) => cols.iter().map(|s| s.as_str()).collect(),
                None => all_cols.iter().map(|s| s.as_str()).collect(),
            };

            // Build per-column fill value from `values` map, falling back to `value`
            let projections: Vec<String> = all_cols
                .iter()
                .map(|col_name| {
                    if !target_cols.contains(&col_name.as_str()) {
                        return format!("\"{col_name}\"");
                    }
                    // Check per-column values map first
                    if let Some(ref values_map) = t.values {
                        if let Some(prim) = values_map.get(col_name.as_str()) {
                            let fill = polars_primitive_to_sql(prim);
                            return format!("COALESCE(\"{col_name}\", {fill}) AS \"{col_name}\"");
                        }
                    }
                    // Fall back to scalar value
                    if let Some(ref prim) = t.value {
                        let fill = polars_primitive_to_sql(prim);
                        return format!("COALESCE(\"{col_name}\", {fill}) AS \"{col_name}\"");
                    }
                    format!("\"{col_name}\"")
                })
                .collect();

            let query = format!("SELECT {} FROM __src", projections.join(", "));
            ctx.execute(&query)
                .map_err(|e| TeckelError::Execution(format!("polars fillNa: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars fillNa collect: {e}")))
        }
        Source::DropNa(t) => {
            let df = get(cache, &t.from)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__src", df.clone().lazy());
            let target_cols: Vec<&str> = match &t.columns {
                Some(cols) => cols.iter().map(|s| s.as_str()).collect(),
                None => df.get_column_names_str().to_vec(),
            };
            if let Some(thresh) = t.min_non_nulls {
                let non_null_expr: Vec<String> = target_cols
                    .iter()
                    .map(|c| format!("CASE WHEN \"{c}\" IS NOT NULL THEN 1 ELSE 0 END"))
                    .collect();
                let query = format!(
                    "SELECT * FROM __src WHERE ({}) >= {thresh}",
                    non_null_expr.join(" + ")
                );
                ctx.execute(&query)
                    .map_err(|e| TeckelError::Execution(format!("polars dropNa thresh: {e}")))?
                    .collect()
                    .map_err(|e| TeckelError::Execution(format!("polars dropNa thresh collect: {e}")))
            } else {
                let condition = match t.how {
                    teckel_model::types::DropNaHow::Any => {
                        target_cols
                            .iter()
                            .map(|c| format!("\"{c}\" IS NOT NULL"))
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    }
                    teckel_model::types::DropNaHow::All => {
                        target_cols
                            .iter()
                            .map(|c| format!("\"{c}\" IS NOT NULL"))
                            .collect::<Vec<_>>()
                            .join(" OR ")
                    }
                };
                let query = format!("SELECT * FROM __src WHERE {condition}");
                ctx.execute(&query)
                    .map_err(|e| TeckelError::Execution(format!("polars dropNa: {e}")))?
                    .collect()
                    .map_err(|e| TeckelError::Execution(format!("polars dropNa collect: {e}")))
            }
        }
        Source::Replace(t) => {
            let df = get(cache, &t.from)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__src", df.clone().lazy());
            let cols: Vec<String> = df.get_column_names_str().iter().map(|s| s.to_string()).collect();
            let target_cols: Vec<&str> = match &t.columns {
                None => cols.iter().map(|s| s.as_str()).collect(),
                Some(c) => c.iter().map(|s| s.as_str()).collect(),
            };
            let projections: Vec<String> = cols
                .iter()
                .map(|col_name| {
                    if target_cols.contains(&col_name.as_str()) && !t.mappings.is_empty() {
                        let mut case_sql = String::from("CASE");
                        for replacement in &t.mappings {
                            let old_val = polars_primitive_to_sql(&replacement.old);
                            let new_val = polars_primitive_to_sql(&replacement.new);
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
            let query = format!("SELECT {} FROM __src", projections.join(", "));
            ctx.execute(&query)
                .map_err(|e| TeckelError::Execution(format!("polars replace: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars replace collect: {e}")))
        }
        Source::Merge(_) => Err(TeckelError::Execution(
            "MERGE transformation requires a mutable table provider. Use the Spark backend for full MERGE INTO support, or decompose into join + filter + union.".to_string(),
        )),
        Source::Parse(_) => Err(TeckelError::Execution(
            "Parse transformation is not yet supported in the Polars backend. Use the Spark backend or decompose with SQL expressions.".to_string(),
        )),
        Source::AsOfJoin(t) => {
            // Emulate as-of join via SQL window approach (ROW_NUMBER + join + direction filter).
            // Native Polars as-of join requires the `asof_join` feature.
            let left_df = get(cache, &t.left)?;
            let right_df = get(cache, &t.right)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__left", left_df.lazy());
            ctx.register("__right", right_df.lazy());

            let on_clause = if t.on.is_empty() {
                "1=1".to_string()
            } else {
                t.on.join(" AND ")
            };
            let direction_filter = match t.direction {
                teckel_model::types::AsOfDirection::Backward => {
                    format!("__right.\"{}\" <= __left.\"{}\"", t.right_as_of, t.left_as_of)
                }
                teckel_model::types::AsOfDirection::Forward => {
                    format!("__right.\"{}\" >= __left.\"{}\"", t.right_as_of, t.left_as_of)
                }
                teckel_model::types::AsOfDirection::Nearest => "1=1".to_string(),
            };
            let order_expr = match t.direction {
                teckel_model::types::AsOfDirection::Backward => {
                    format!("__right.\"{}\" DESC", t.right_as_of)
                }
                teckel_model::types::AsOfDirection::Forward => {
                    format!("__right.\"{}\" ASC", t.right_as_of)
                }
                teckel_model::types::AsOfDirection::Nearest => {
                    format!(
                        "ABS(__right.\"{}\" - __left.\"{}\") ASC",
                        t.right_as_of, t.left_as_of
                    )
                }
            };

            let sql = format!(
                "SELECT * FROM (\
                    SELECT __left.*, __right.*, \
                    ROW_NUMBER() OVER (PARTITION BY __left.\"{}\" ORDER BY {}) AS __teckel_rn \
                    FROM __left \
                    JOIN __right ON {} AND {}\
                ) WHERE __teckel_rn = 1",
                t.left_as_of, order_expr, on_clause, direction_filter
            );
            ctx.execute(&sql)
                .map_err(|e| TeckelError::Execution(format!("polars as-of join: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars as-of join collect: {e}")))
        }
        Source::LateralJoin(t) => {
            // Emulate lateral join as a regular join via Polars SQL context.
            // For true correlated subqueries, use the Spark backend.
            let left_df = get(cache, &t.left)?;
            let right_df = get(cache, &t.right)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__left", left_df.lazy());
            ctx.register("__right", right_df.lazy());
            let join_type_sql = match t.join_type {
                teckel_model::types::JoinType::Inner => "INNER JOIN",
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
            let query = format!("SELECT * FROM __left {join_type_sql} __right{on_clause}");
            ctx.execute(&query)
                .map_err(|e| TeckelError::Execution(format!("polars lateral join: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars lateral join collect: {e}")))
        }
        Source::Transpose(t) => {
            let df = get(cache, &t.from)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__src", df.clone().lazy());
            let cols: Vec<String> = df.get_column_names_str().iter().map(|s| s.to_string()).collect();
            let index_cols = &t.index_columns;
            let value_cols: Vec<&String> = cols.iter().filter(|c| !index_cols.contains(c)).collect();
            if value_cols.is_empty() {
                return Ok(df);
            }
            let index_select = if index_cols.is_empty() {
                String::new()
            } else {
                let idx = index_cols.iter().map(|c| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
                format!("{idx}, ")
            };
            let unions: Vec<String> = value_cols
                .iter()
                .map(|col_name| {
                    format!(
                        "SELECT {index_select}'{col_name}' AS \"column_name\", CAST(\"{col_name}\" AS VARCHAR) AS \"value\" FROM __src"
                    )
                })
                .collect();
            let query = unions.join(" UNION ALL ");
            ctx.execute(&query)
                .map_err(|e| TeckelError::Execution(format!("polars transpose: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars transpose collect: {e}")))
        }
        Source::GroupingSets(t) => {
            let df = get(cache, &t.from)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__src", df.lazy());
            let sets_sql: Vec<String> = t
                .sets
                .iter()
                .map(|set| {
                    let cols = set.join(", ");
                    format!("({cols})")
                })
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
                "SELECT {select_cols}, {agg_exprs} FROM __src GROUP BY GROUPING SETS({})",
                sets_sql.join(", ")
            );
            ctx.execute(&query)
                .map_err(|e| TeckelError::Execution(format!("polars groupingSets: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars groupingSets collect: {e}")))
        }
        Source::Describe(t) => {
            let df = get(cache, &t.from)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__src", df.clone().lazy());
            let target_cols: Vec<String> = match &t.columns {
                Some(cols) => cols.clone(),
                None => df.get_column_names_str().iter().map(|s| s.to_string()).collect(),
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
                            "stddev" | "std" => format!("CAST(STDDEV(CAST(\"{c}\" AS DOUBLE)) AS VARCHAR)"),
                            "min" => format!("CAST(MIN(\"{c}\") AS VARCHAR)"),
                            "max" => format!("CAST(MAX(\"{c}\") AS VARCHAR)"),
                            other => format!("'{other}: unsupported'"),
                        };
                        format!("{expr} AS \"{c}\"")
                    })
                    .collect();
                stat_queries.push(format!(
                    "SELECT '{stat}' AS \"statistic\", {} FROM __src",
                    projections.join(", ")
                ));
            }
            let query = stat_queries.join(" UNION ALL ");
            ctx.execute(&query)
                .map_err(|e| TeckelError::Execution(format!("polars describe: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars describe collect: {e}")))
        }
        Source::Crosstab(t) => {
            let df = get(cache, &t.from)?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("__src", df.clone().lazy());
            // Get distinct values of col2
            let distinct_q = format!("SELECT DISTINCT \"{}\" FROM __src", t.col2);
            let distinct_df = ctx.execute(&distinct_q)
                .map_err(|e| TeckelError::Execution(format!("polars crosstab distinct: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars crosstab distinct collect: {e}")))?;
            let col2_values: Vec<String> = distinct_df
                .column(&t.col2)
                .map_err(|e| TeckelError::Execution(format!("polars crosstab col: {e}")))?
                .str()
                .map_err(|e| TeckelError::Execution(format!("polars crosstab str: {e}")))?
                .into_no_null_iter()
                .map(|s| s.to_string())
                .collect();
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
                "SELECT \"{}\", {} FROM __src GROUP BY \"{}\"",
                t.col1,
                pivot_exprs.join(", "),
                t.col1
            );
            // Re-register since ctx may have been consumed
            ctx.register("__src", get(cache, &t.from)?.lazy());
            ctx.execute(&query)
                .map_err(|e| TeckelError::Execution(format!("polars crosstab: {e}")))?
                .collect()
                .map_err(|e| TeckelError::Execution(format!("polars crosstab collect: {e}")))
        }
        Source::Hint(t) => {
            // Hints are optimizer directives — not applicable in Polars. Pass through.
            let df = get(cache, &t.from)?;
            for hint in &t.hints {
                tracing::info!(
                    hint = hint.name.as_str(),
                    "ignoring optimizer hint (not supported in polars backend)"
                );
            }
            Ok(df)
        }
        Source::Input(_) | Source::Output(_) => Err(TeckelError::Execution(
            "Input/Output handled by executor".to_string(),
        )),
    }
}

fn polars_primitive_to_sql(p: &teckel_model::types::Primitive) -> String {
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

fn sql_agg(
    cache: &BTreeMap<String, DataFrame>,
    from: &str,
    by: &[String],
    agg: &[String],
    group_type: &str,
) -> Result<DataFrame, TeckelError> {
    let df = get(cache, from)?;
    let mut ctx = polars::sql::SQLContext::new();
    ctx.register("__src", df.lazy());
    let by_cols = by.join(", ");
    let agg_exprs = agg.join(", ");
    let query =
        format!("SELECT {by_cols}, {agg_exprs} FROM __src GROUP BY {group_type}({by_cols})");
    ctx.execute(&query)
        .map_err(|e| TeckelError::Execution(format!("polars {group_type}: {e}")))?
        .collect()
        .map_err(|e| TeckelError::Execution(format!("polars {group_type} collect: {e}")))
}

fn get(cache: &BTreeMap<String, DataFrame>, name: &str) -> Result<DataFrame, TeckelError> {
    cache
        .get(name)
        .cloned()
        .ok_or_else(|| TeckelError::Execution(format!("asset \"{name}\" not found in cache")))
}

fn parse_polars_expr(expr_str: &str) -> Expr {
    let trimmed = expr_str.trim();
    if trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return col(trimmed);
    }
    if let Some((_, column)) = trimmed.split_once('.') {
        if column
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return col(column);
        }
    }
    col(trimmed)
}

fn parse_polars_dtype(type_str: &str) -> DataType {
    match type_str.to_lowercase().as_str() {
        "string" | "varchar" | "text" => DataType::String,
        "integer" | "int" | "int32" => DataType::Int32,
        "long" | "bigint" | "int64" => DataType::Int64,
        "float" | "float32" => DataType::Float32,
        "double" | "float64" => DataType::Float64,
        "boolean" | "bool" => DataType::Boolean,
        "date" => DataType::Date,
        "timestamp" => DataType::Datetime(TimeUnit::Microseconds, None),
        "binary" => DataType::Binary,
        _ => DataType::String,
    }
}

fn sort_col_to_sql(sc: &teckel_model::types::SortColumn) -> String {
    match sc {
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
    }
}

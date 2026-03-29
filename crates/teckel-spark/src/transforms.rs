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
            let query = format!("SELECT {group_cols}, {agg_exprs} FROM {view} GROUP BY {group_cols}");
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
                            (teckel_model::types::SortDirection::Asc, teckel_model::types::NullOrdering::First) => c.asc_nulls_first(),
                            (teckel_model::types::SortDirection::Asc, teckel_model::types::NullOrdering::Last) => c.asc_nulls_last(),
                            (teckel_model::types::SortDirection::Desc, teckel_model::types::NullOrdering::First) => c.desc_nulls_first(),
                            (teckel_model::types::SortDirection::Desc, teckel_model::types::NullOrdering::Last) => c.desc_nulls_last(),
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
                views_to_register.push((&target.name, Box::leak(format!("__teckel_join_right_{i}").into_boxed_str())));
            }

            for (asset_ref, view_name) in &views_to_register {
                let df = get(cache, asset_ref)?;
                df.create_or_replace_temp_view(view_name)
                    .await
                    .map_err(|e| TeckelError::Execution(format!("join register: {e}")))?;
            }

            let mut query = format!("SELECT * FROM __teckel_join_left");
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
                    query.push_str(&format!(" {join_type} __teckel_join_right_{i} ON {condition}"));
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
                    result.union_all(df).map_err(|e| TeckelError::Execution(format!("union: {e}")))?
                } else {
                    result.union(df).map_err(|e| TeckelError::Execution(format!("union: {e}")))?
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
                result = result.intersect(df).map_err(|e| TeckelError::Execution(format!("intersect: {e}")))?;
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
                    .map_err(|e| TeckelError::Execution(format!("register view \"{view_name}\": {e}")))?;
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
                let parts: Vec<String> = t.order_by.iter().map(|sc| match sc {
                    teckel_model::types::SortColumn::Simple(name) => name.clone(),
                    teckel_model::types::SortColumn::Explicit { column, direction, nulls } => {
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
                }).collect();
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

            let window_spec = format!("PARTITION BY {partition_clause} {order_clause} {frame_clause}");
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
                Some(vals) => vals.iter().map(|v| format!("'{v}'")).collect::<Vec<_>>().join(", "),
                None => return Err(TeckelError::Execution("pivot requires explicit values for Spark backend".to_string())),
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

        // Fallback for transforms not yet mapped
        source => Err(TeckelError::Execution(format!(
            "transform {:?} is not yet supported by the Spark Connect backend",
            std::mem::discriminant(source)
        ))),
    }
}

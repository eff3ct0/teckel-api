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
        _ => Err(TeckelError::Execution(format!(
            "transformation not yet implemented in DataFusion backend: {:?}",
            std::mem::discriminant(source)
        ))),
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

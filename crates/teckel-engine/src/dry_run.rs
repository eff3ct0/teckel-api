use crate::dag::PipelineDag;
use teckel_model::asset::Context;
use teckel_model::source::Source;
use teckel_model::TeckelError;

/// Generate a human-readable execution plan without running anything.
pub fn explain(context: &Context) -> Result<String, TeckelError> {
    let dag = PipelineDag::from_context(context)?;
    let order = dag.topological_order()?;
    let waves = dag.parallel_schedule()?;

    let mut lines = Vec::new();
    lines.push("=== Pipeline Execution Plan ===\n".to_string());

    // Group by type
    let mut inputs = Vec::new();
    let mut transforms = Vec::new();
    let mut outputs = Vec::new();

    for name in &order {
        let asset = &context[name];
        match &asset.source {
            Source::Input(i) => {
                inputs.push(format!("  [{}] format={}, path={}", name, i.format, i.path));
            }
            Source::Output(o) => {
                outputs.push(format!(
                    "  [{}] from={}, format={}, mode={:?}, path={}",
                    name, o.asset_ref, o.format, o.mode, o.path
                ));
            }
            source => {
                let deps = source.dependencies();
                let dep_str = deps
                    .iter()
                    .map(|d| d.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let op_name = source_op_name(source);
                transforms.push(format!("  [{}] {} (from: {})", name, op_name, dep_str));
            }
        }
    }

    lines.push("--- Inputs ---".to_string());
    lines.extend(inputs.iter().cloned());
    lines.push(String::new());

    if !transforms.is_empty() {
        lines.push("--- Transformations ---".to_string());
        lines.extend(transforms.iter().cloned());
        lines.push(String::new());
    }

    lines.push("--- Outputs ---".to_string());
    lines.extend(outputs.iter().cloned());
    lines.push(String::new());

    lines.push(format!(
        "Total: {} inputs, {} transformations, {} outputs",
        inputs.len(),
        transforms.len(),
        outputs.len()
    ));

    lines.push(format!("Execution waves: {}", waves.len()));
    for (i, wave) in waves.iter().enumerate() {
        lines.push(format!("  Wave {}: {}", i, wave.join(", ")));
    }

    Ok(lines.join("\n"))
}

fn source_op_name(source: &Source) -> &'static str {
    match source {
        Source::Input(_) => "INPUT",
        Source::Output(_) => "OUTPUT",
        Source::Select(_) => "SELECT",
        Source::Where(_) => "WHERE",
        Source::GroupBy(_) => "GROUP BY",
        Source::OrderBy(_) => "ORDER BY",
        Source::Join(_) => "JOIN",
        Source::Union(_) => "UNION",
        Source::Intersect(_) => "INTERSECT",
        Source::Except(_) => "EXCEPT",
        Source::Distinct(_) => "DISTINCT",
        Source::Limit(_) => "LIMIT",
        Source::AddColumns(_) => "ADD COLUMNS",
        Source::DropColumns(_) => "DROP COLUMNS",
        Source::RenameColumns(_) => "RENAME COLUMNS",
        Source::CastColumns(_) => "CAST COLUMNS",
        Source::Window(_) => "WINDOW",
        Source::Pivot(_) => "PIVOT",
        Source::Unpivot(_) => "UNPIVOT",
        Source::Flatten(_) => "FLATTEN",
        Source::Sample(_) => "SAMPLE",
        Source::Conditional(_) => "CONDITIONAL",
        Source::Split(_) => "SPLIT",
        Source::Sql(_) => "SQL",
        Source::Rollup(_) => "ROLLUP",
        Source::Cube(_) => "CUBE",
        Source::Scd2(_) => "SCD2",
        Source::Enrich(_) => "ENRICH",
        Source::SchemaEnforce(_) => "SCHEMA ENFORCE",
        Source::Assertion(_) => "ASSERTION",
        Source::Repartition(_) => "REPARTITION",
        Source::Coalesce(_) => "COALESCE",
        Source::Custom(_) => "CUSTOM",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use teckel_model::asset::Asset;
    use teckel_model::source::*;

    #[test]
    fn explain_simple_pipeline() {
        let context: Context = [
            (
                "src".to_string(),
                Asset {
                    asset_ref: "src".to_string(),
                    source: Source::Input(InputSource {
                        format: "csv".to_string(),
                        path: "data.csv".to_string(),
                        options: Default::default(),
                    }),
                },
            ),
            (
                "filtered".to_string(),
                Asset {
                    asset_ref: "filtered".to_string(),
                    source: Source::Where(WhereTransform {
                        from: "src".to_string(),
                        filter: "x > 0".to_string(),
                    }),
                },
            ),
        ]
        .into_iter()
        .collect();

        let plan = explain(&context).unwrap();
        assert!(plan.contains("[src]"));
        assert!(plan.contains("WHERE"));
        assert!(plan.contains("1 inputs"));
        assert!(plan.contains("1 transformations"));
    }
}

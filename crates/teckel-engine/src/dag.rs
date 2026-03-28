use petgraph::algo::toposort;
use petgraph::graph::{DiGraph, NodeIndex};
use std::collections::BTreeMap;
use teckel_model::asset::Context;
use teckel_model::{TeckelError, TeckelErrorCode};

/// Pipeline DAG built from the asset context.
pub struct PipelineDag {
    graph: DiGraph<String, ()>,
    node_indices: BTreeMap<String, NodeIndex>,
}

impl PipelineDag {
    /// Build the DAG from a validated asset context.
    pub fn from_context(context: &Context) -> Result<Self, TeckelError> {
        let mut graph = DiGraph::new();
        let mut node_indices = BTreeMap::new();

        for name in context.keys() {
            let idx = graph.add_node(name.clone());
            node_indices.insert(name.clone(), idx);
        }

        for (name, asset) in context {
            let to_idx = node_indices[name];
            for dep in asset.source.dependencies() {
                if let Some(&from_idx) = node_indices.get(dep.as_str()) {
                    graph.add_edge(from_idx, to_idx, ());
                }
            }
        }

        Ok(Self {
            graph,
            node_indices,
        })
    }

    /// Return assets in topological order (dependencies first).
    pub fn topological_order(&self) -> Result<Vec<String>, TeckelError> {
        let sorted = toposort(&self.graph, None).map_err(|cycle| {
            let node_name = &self.graph[cycle.node_id()];
            TeckelError::spec(
                TeckelErrorCode::ECycle001,
                format!("circular dependency detected at asset \"{node_name}\""),
            )
        })?;

        Ok(sorted
            .into_iter()
            .map(|idx| self.graph[idx].clone())
            .collect())
    }

    /// Return groups of assets that can execute in parallel (wave scheduling).
    ///
    /// Each wave contains assets whose dependencies are all in previous waves.
    /// Phase 1 executes waves sequentially; Phase 2 will use `tokio::spawn` per wave.
    pub fn parallel_schedule(&self) -> Result<Vec<Vec<String>>, TeckelError> {
        let order = self.topological_order()?;
        let mut waves: Vec<Vec<String>> = Vec::new();
        let mut asset_wave: BTreeMap<String, usize> = BTreeMap::new();

        for name in &order {
            let asset_idx = self.node_indices[name];
            let max_dep_wave = self
                .graph
                .neighbors_directed(asset_idx, petgraph::Direction::Incoming)
                .filter_map(|dep_idx| {
                    let dep_name = &self.graph[dep_idx];
                    asset_wave.get(dep_name.as_str()).copied()
                })
                .max();

            let wave_idx = match max_dep_wave {
                Some(w) => w + 1,
                None => 0,
            };

            asset_wave.insert(name.clone(), wave_idx);
            while waves.len() <= wave_idx {
                waves.push(Vec::new());
            }
            waves[wave_idx].push(name.clone());
        }

        Ok(waves)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use teckel_model::asset::{Asset, AssetMetadata};
    use teckel_model::source::*;

    fn input_asset(name: &str) -> (String, Asset) {
        (
            name.to_string(),
            Asset {
                asset_ref: name.to_string(),
                source: Source::Input(InputSource {
                    format: "csv".to_string(),
                    path: format!("{name}.csv"),
                    options: Default::default(),
                }),
                metadata: AssetMetadata::default(),
            },
        )
    }

    fn filter_asset(name: &str, from: &str) -> (String, Asset) {
        (
            name.to_string(),
            Asset {
                asset_ref: name.to_string(),
                source: Source::Where(WhereTransform {
                    from: from.to_string(),
                    filter: "x > 0".to_string(),
                }),
                metadata: AssetMetadata::default(),
            },
        )
    }

    #[test]
    fn topological_order_respects_deps() {
        let context: Context = [input_asset("a"), filter_asset("b", "a")]
            .into_iter()
            .collect();

        let dag = PipelineDag::from_context(&context).unwrap();
        let order = dag.topological_order().unwrap();

        assert!(order.iter().position(|n| n == "a") < order.iter().position(|n| n == "b"));
    }

    #[test]
    fn parallel_schedule_groups_independent() {
        let context: Context = [input_asset("a"), input_asset("b"), filter_asset("c", "a")]
            .into_iter()
            .collect();

        let dag = PipelineDag::from_context(&context).unwrap();
        let waves = dag.parallel_schedule().unwrap();

        // Wave 0: a, b (independent inputs)
        // Wave 1: c (depends on a)
        assert_eq!(waves.len(), 2);
        assert!(waves[0].contains(&"a".to_string()));
        assert!(waves[0].contains(&"b".to_string()));
        assert_eq!(waves[1], vec!["c".to_string()]);
    }

    #[test]
    fn diamond_dag_schedule() {
        // a → b, a → c, b+c → d
        let context: Context = [
            input_asset("a"),
            filter_asset("b", "a"),
            filter_asset("c", "a"),
            (
                "d".to_string(),
                Asset {
                    asset_ref: "d".to_string(),
                    source: Source::Union(UnionTransform {
                        sources: vec!["b".to_string(), "c".to_string()],
                        all: true,
                    }),
                    metadata: AssetMetadata::default(),
                },
            ),
        ]
        .into_iter()
        .collect();

        let dag = PipelineDag::from_context(&context).unwrap();
        let waves = dag.parallel_schedule().unwrap();

        assert_eq!(waves.len(), 3);
        assert_eq!(waves[0], vec!["a".to_string()]);
        assert_eq!(waves[1].len(), 2); // b and c in parallel
        assert_eq!(waves[2], vec!["d".to_string()]);
    }
}

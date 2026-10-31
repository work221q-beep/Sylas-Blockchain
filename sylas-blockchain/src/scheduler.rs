//! Dependency graph scheduler.
//!
//! Write/write conflicts are represented as graph edges. A greedy coloring pass
//! converts the graph into deterministic execution stages. Transactions in the
//! same stage have disjoint write sets and can safely execute in parallel
//! against a stage-level snapshot. Stages themselves are committed in order.

use std::collections::BTreeSet;

use crate::types::{Pubkey, Transaction};

#[derive(Clone, Debug)]
pub struct ExecutionPlan {
    pub stages: Vec<Vec<usize>>,
    pub conflict_edges: usize,
}

pub struct Scheduler;

impl Scheduler {
    pub fn plan(transactions: &[Transaction]) -> ExecutionPlan {
        let graph = build_conflict_graph(transactions);
        let conflict_edges = graph.iter().map(BTreeSet::len).sum::<usize>() / 2;
        let colors = greedy_color(&graph);
        let color_count = colors.iter().copied().max().map_or(0, |color| color + 1);
        let mut stages = vec![Vec::new(); color_count];
        for (index, color) in colors.into_iter().enumerate() {
            stages[color].push(index);
        }
        ExecutionPlan {
            stages,
            conflict_edges,
        }
    }
}

fn build_conflict_graph(transactions: &[Transaction]) -> Vec<BTreeSet<usize>> {
    let mut graph = vec![BTreeSet::new(); transactions.len()];
    for left in 0..transactions.len() {
        for right in left + 1..transactions.len() {
            if write_conflicts(&transactions[left].write_set, &transactions[right].write_set) {
                graph[left].insert(right);
                graph[right].insert(left);
            }
        }
    }
    graph
}

fn greedy_color(graph: &[BTreeSet<usize>]) -> Vec<usize> {
    let mut colors = vec![usize::MAX; graph.len()];
    for node in 0..graph.len() {
        let mut unavailable = BTreeSet::new();
        for neighbor in &graph[node] {
            let color = colors[*neighbor];
            if color != usize::MAX {
                unavailable.insert(color);
            }
        }
        let mut color = 0;
        while unavailable.contains(&color) {
            color += 1;
        }
        colors[node] = color;
    }
    colors
}

fn write_conflicts(left: &[Pubkey], right: &[Pubkey]) -> bool {
    if left.len() <= right.len() {
        left.iter().any(|key| right.contains(key))
    } else {
        right.iter().any(|key| left.contains(key))
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{Transaction, TransferOp};

    use super::Scheduler;

    fn key(value: u8) -> [u8; 32] {
        [value; 32]
    }

    fn tx(write_set: Vec<[u8; 32]>) -> Transaction {
        let signer = write_set[0];
        Transaction::unsigned(
            signer,
            1,
            0,
            [0u8; 32],
            vec![],
            write_set.clone(),
            vec![TransferOp {
                from: signer,
                to: write_set[1],
                amount: 1,
            }],
        )
        .expect("test transaction")
    }

    #[test]
    fn disjoint_writes_share_a_stage() {
        let plan = Scheduler::plan(&[tx(vec![key(1), key(2)]), tx(vec![key(3), key(4)])]);
        assert_eq!(plan.stages.len(), 1);
        assert_eq!(plan.stages[0], vec![0, 1]);
    }

    #[test]
    fn conflicting_writes_split_stages() {
        let plan = Scheduler::plan(&[tx(vec![key(1), key(2)]), tx(vec![key(1), key(3)])]);
        assert_eq!(plan.stages.len(), 2);
        assert_eq!(plan.conflict_edges, 1);
    }
}

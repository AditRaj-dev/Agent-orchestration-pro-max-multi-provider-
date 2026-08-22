//! Workflow-spec validation (PRD §9 OR-03) — everything checkable BEFORE
//! execution is checked here, so the engine never mutates state for a spec
//! that could not run to completion.
//!
//! Five rules:
//!
//! 1. at least one node;
//! 2. node ids are unique;
//! 3. every `dependsOn` refers to a declared node;
//! 4. the dependency graph is acyclic (Kahn topological sort; on failure a
//!    concrete cycle path is reported);
//! 5. loops are bounded (`max_iterations >= 1`).
//!
//! All rejections are the machine-readable [`ValidationError`].

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use crate::error::{ValidationError, WorkflowError};
use crate::spec::{NodeType, WorkflowSpec};

/// Validate a workflow spec. Returns `Ok(())` or the first rejection in the
/// deterministic rule order listed in the [module docs](self).
pub fn validate(spec: &WorkflowSpec) -> Result<(), WorkflowError> {
    topological_order(spec).map(|_| ())
}

/// Compute a deterministic topological order of the spec's node ids, or
/// reject the spec with the first applicable [`ValidationError`].
///
/// Ordering: within each "wave" of Kahn's algorithm, nodes are emitted in
/// spec-declaration order, so the result is a stable function of the spec
/// (the same determinism the engine's scheduler relies on).
pub fn topological_order(spec: &WorkflowSpec) -> Result<Vec<String>, WorkflowError> {
    // Rule 1: non-empty.
    if spec.nodes.is_empty() {
        return Err(ValidationError::EmptyWorkflow.into());
    }

    // Rule 2: unique ids.
    let mut seen = HashSet::new();
    let mut duplicates = BTreeSet::new();
    for node in &spec.nodes {
        if !seen.insert(node.id.as_str()) {
            duplicates.insert(node.id.clone());
        }
    }
    if !duplicates.is_empty() {
        return Err(ValidationError::DuplicateNodeIds {
            duplicates: duplicates.into_iter().collect(),
        }
        .into());
    }

    // Rule 3: dependencies resolve.
    for node in &spec.nodes {
        for dep in &node.depends_on {
            if !seen.contains(dep.as_str()) {
                return Err(ValidationError::UnknownDependency {
                    node: node.id.clone(),
                    dependency: dep.clone(),
                }
                .into());
            }
        }
    }

    // Rule 5: loops bounded.
    for node in &spec.nodes {
        if let NodeType::Loop { max_iterations } = node.node_type {
            if max_iterations == 0 {
                return Err(ValidationError::UnboundedLoop {
                    node: node.id.clone(),
                    max_iterations,
                }
                .into());
            }
        }
    }

    // Rule 4: acyclic, via Kahn's algorithm.
    let order = kahn_order(spec);
    if order.len() != spec.nodes.len() {
        let cycle = find_cycle(spec);
        return Err(ValidationError::CycleDetected { cycle }.into());
    }
    Ok(order)
}

/// Kahn's algorithm over the spec graph. Edges run dependency -> dependent.
/// Emits each ready wave in spec-declaration order; a graph with a cycle
/// yields a shorter-than-node-count order.
fn kahn_order(spec: &WorkflowSpec) -> Vec<String> {
    let position: HashMap<&str, usize> = spec
        .nodes
        .iter()
        .enumerate()
        .map(|(idx, node)| (node.id.as_str(), idx))
        .collect();

    // indegree[node] = number of unsatisfied dependencies.
    let mut indegree: Vec<usize> = spec.nodes.iter().map(|n| n.depends_on.len()).collect();
    // dependents[dep idx] = nodes depending on it.
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); spec.nodes.len()];
    for (idx, node) in spec.nodes.iter().enumerate() {
        for dep in &node.depends_on {
            dependents[position[dep.as_str()]].push(idx);
        }
    }

    let mut queue: VecDeque<usize> = spec
        .nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| node.depends_on.is_empty())
        .map(|(idx, _)| idx)
        .collect();
    let mut order = Vec::with_capacity(spec.nodes.len());
    while let Some(idx) = queue.pop_front() {
        order.push(spec.nodes[idx].id.clone());
        for &dependent in &dependents[idx] {
            indegree[dependent] -= 1;
            if indegree[dependent] == 0 {
                queue.push_back(dependent);
            }
        }
    }
    order
}

/// Extract one concrete cycle for error reporting. Deterministic: DFS in
/// spec-declaration order, returning the first grey-node hit as a path that
/// starts and ends on the same id (e.g. `["a", "b", "a"]`; a self-dependency
/// yields `["a", "a"]`).
fn find_cycle(spec: &WorkflowSpec) -> Vec<String> {
    #[derive(Clone, Copy, PartialEq)]
    enum Color {
        White,
        Grey,
        Black,
    }

    let mut colors = vec![Color::White; spec.nodes.len()];
    let position: HashMap<&str, usize> = spec
        .nodes
        .iter()
        .enumerate()
        .map(|(idx, node)| (node.id.as_str(), idx))
        .collect();

    fn dfs(
        spec: &WorkflowSpec,
        position: &HashMap<&str, usize>,
        colors: &mut [Color],
        idx: usize,
        stack: &mut Vec<usize>,
    ) -> Option<Vec<String>> {
        colors[idx] = Color::Grey;
        stack.push(idx);
        for dep in &spec.nodes[idx].depends_on {
            let dep_idx = position[dep.as_str()];
            match colors[dep_idx] {
                Color::Grey => {
                    // Found it: slice the stack from the first occurrence of
                    // dep_idx and close the loop by repeating that node.
                    let start = stack
                        .iter()
                        .position(|&i| i == dep_idx)
                        .expect("grey node is on the stack");
                    let mut cycle: Vec<String> = stack[start..]
                        .iter()
                        .map(|&i| spec.nodes[i].id.clone())
                        .collect();
                    cycle.push(spec.nodes[dep_idx].id.clone());
                    return Some(cycle);
                }
                Color::White => {
                    if let Some(cycle) = dfs(spec, position, colors, dep_idx, stack) {
                        return Some(cycle);
                    }
                }
                Color::Black => {}
            }
        }
        stack.pop();
        colors[idx] = Color::Black;
        None
    }

    for idx in 0..spec.nodes.len() {
        if colors[idx] == Color::White {
            let mut stack = Vec::new();
            if let Some(cycle) = dfs(spec, &position, &mut colors, idx, &mut stack) {
                return cycle;
            }
        }
    }
    // Unreachable when called after kahn_order found a deficit, but keep the
    // function total: report no cycle rather than panicking.
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{Budgets, NodeSpec, RetryPolicy};

    fn node(id: &str, node_type: NodeType, depends_on: &[&str]) -> NodeSpec {
        NodeSpec {
            id: id.to_owned(),
            node_type,
            depends_on: depends_on.iter().map(|d| d.to_string()).collect(),
            agent_role: None,
            budgets: Budgets::default(),
            retry: RetryPolicy::default(),
        }
    }

    fn spec(nodes: Vec<NodeSpec>) -> WorkflowSpec {
        WorkflowSpec {
            id: "test-workflow".to_owned(),
            version: 1,
            nodes,
        }
    }

    #[test]
    fn topological_order_is_deterministic_and_wave_stable() {
        let workflow = spec(vec![
            node("a", NodeType::Run, &[]),
            node("b", NodeType::Run, &[]),
            node("c", NodeType::Review, &["a", "b"]),
            node("d", NodeType::GitGate, &["c"]),
        ]);
        assert_eq!(
            topological_order(&workflow).unwrap(),
            vec!["a", "b", "c", "d"]
        );
        // Same spec, same order — a pure function of declaration order.
        assert_eq!(
            topological_order(&workflow).unwrap(),
            topological_order(&workflow).unwrap()
        );
    }

    #[test]
    fn self_dependency_is_reported_as_a_cycle() {
        let workflow = spec(vec![node("a", NodeType::Run, &["a"])]);
        let err = validate(&workflow).unwrap_err();
        match err {
            WorkflowError::Validation(ValidationError::CycleDetected { cycle }) => {
                assert_eq!(cycle, vec!["a".to_owned(), "a".to_owned()]);
            }
            other => panic!("expected CycleDetected, got {other:?}"),
        }
    }
}

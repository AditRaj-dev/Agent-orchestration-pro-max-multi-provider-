//! F-06 integration tests — spec validation rejections (PRD §9 OR-03:
//! everything checkable before execution is checked, and the rejection is
//! machine-readable).

use agentos_workflow::{
    Budgets, NodeSpec, NodeType, RetryPolicy, TaskContract, ValidationError, WorkflowError,
    WorkflowSpec,
};

fn node(id: &str, node_type: NodeType, depends_on: &[&str]) -> NodeSpec {
    NodeSpec {
        id: id.to_owned(),
        node_type,
        depends_on: depends_on.iter().map(|d| (*d).to_owned()).collect(),
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
fn prd_example_workflow_is_valid() {
    let workflow = spec(vec![
        node("spec", NodeType::Run, &[]),
        node("build", NodeType::Parallel, &["spec"]),
        node("review", NodeType::Review, &["build"]),
        node("commit", NodeType::GitGate, &["review"]),
    ]);
    agentos_workflow::validate(&workflow).expect("PRD example must validate");
}

#[test]
fn cycle_is_rejected_with_the_cycle_path() {
    let workflow = spec(vec![
        node("a", NodeType::Run, &["c"]),
        node("b", NodeType::Run, &["a"]),
        node("c", NodeType::Run, &["b"]),
        node("d", NodeType::Run, &[]), // innocent bystander
    ]);
    let err = agentos_workflow::validate(&workflow).unwrap_err();
    match err {
        WorkflowError::Validation(ValidationError::CycleDetected { cycle }) => {
            // A concrete path, closed: first == last, every hop an edge.
            assert_eq!(cycle.first(), cycle.last());
            assert!(
                cycle.len() >= 2,
                "cycle must have at least one edge: {cycle:?}"
            );
            assert!(
                cycle
                    .iter()
                    .all(|id| ["a", "b", "c"].contains(&id.as_str())),
                "cycle must only involve cycle members: {cycle:?}"
            );
        }
        other => panic!("expected CycleDetected, got {other:?}"),
    }
}

#[test]
fn dangling_dependency_is_rejected() {
    let workflow = spec(vec![
        node("a", NodeType::Run, &[]),
        node("b", NodeType::Run, &["ghost"]),
    ]);
    let err = agentos_workflow::validate(&workflow).unwrap_err();
    assert_eq!(
        err,
        WorkflowError::Validation(ValidationError::UnknownDependency {
            node: "b".to_owned(),
            dependency: "ghost".to_owned(),
        })
    );
}

#[test]
fn duplicate_node_id_is_rejected() {
    let workflow = spec(vec![
        node("n1", NodeType::Run, &[]),
        node("n2", NodeType::Run, &[]),
        node("n1", NodeType::Review, &["n2"]),
    ]);
    let err = agentos_workflow::validate(&workflow).unwrap_err();
    assert_eq!(
        err,
        WorkflowError::Validation(ValidationError::DuplicateNodeIds {
            duplicates: vec!["n1".to_owned()],
        })
    );
}

#[test]
fn unbounded_loop_is_rejected() {
    let workflow = spec(vec![node(
        "spin",
        NodeType::Loop { max_iterations: 0 },
        &[],
    )]);
    let err = agentos_workflow::validate(&workflow).unwrap_err();
    assert_eq!(
        err,
        WorkflowError::Validation(ValidationError::UnboundedLoop {
            node: "spin".to_owned(),
            max_iterations: 0,
        })
    );
    // ...while a bounded loop passes.
    let bounded = spec(vec![node(
        "spin",
        NodeType::Loop { max_iterations: 3 },
        &[],
    )]);
    agentos_workflow::validate(&bounded).expect("bounded loop is valid");
}

#[test]
fn empty_workflow_is_rejected() {
    let err = agentos_workflow::validate(&spec(Vec::new())).unwrap_err();
    assert_eq!(
        err,
        WorkflowError::Validation(ValidationError::EmptyWorkflow)
    );
}

/// OR-04: the task contract subset round-trips with its full field set.
#[test]
fn task_contract_round_trips_fully_populated() {
    let contract = TaskContract {
        objective: "Implement the scheduler".to_owned(),
        allowed_paths: vec!["crates/agentos-workflow/src".to_owned()],
        forbidden_paths: vec!["crates/agentos-core".to_owned()],
        acceptance_criteria: vec!["all tests green".to_owned()],
        required_checks: vec!["cargo clippy -D warnings".to_owned()],
    };
    let wire = serde_json::to_value(&contract).unwrap();
    assert_eq!(
        wire["allowedPaths"],
        serde_json::json!(["crates/agentos-workflow/src"])
    );
    let parsed: TaskContract = serde_json::from_value(wire).unwrap();
    assert_eq!(parsed, contract);
}

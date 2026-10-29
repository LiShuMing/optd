//! Public rule-registry and executable-plan verification contracts.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::logical_expr::LogicalPlan;
use optd_incremental_sql::planner::{
    DeltaExpr, IncrementalOptimizer, IncrementalRule, Maintenance, RuleContext, RuleRewrite,
    MAX_EXPR_DEPTH, MAX_EXPR_NODES,
};
use optd_incremental_sql::{DurableSqlView, Error, Result, SqlView};

const SQL: &str = "SELECT v AS value FROM t WHERE k > 0";
const PRIORITY: i32 = i32::MAX - 10;

fn tables() -> BTreeMap<String, RecordBatch> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    BTreeMap::from([("t".into(), batch)])
}

#[derive(Clone, Copy)]
enum Target {
    Projection,
    Ordered,
    IneligibleAggregate,
}

#[derive(Clone, Copy)]
enum Replacement {
    Recompute,
    InvalidPort,
    InvalidArity,
    WrongSchema,
    SourceAtProjection,
    JoinAtProjection,
    Groups,
    Constant,
    EmptySum,
    Negates(usize),
    Terms(usize),
    MatchError,
    RewriteError,
}

struct TestRule {
    name: &'static str,
    priority: i32,
    target: Target,
    replacement: Replacement,
    rewrites: Arc<AtomicUsize>,
}

impl TestRule {
    fn projection(name: &'static str, replacement: Replacement) -> Self {
        Self {
            name,
            priority: PRIORITY,
            target: Target::Projection,
            replacement,
            rewrites: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl IncrementalRule for TestRule {
    fn name(&self) -> &str {
        self.name
    }

    fn version(&self) -> u32 {
        7
    }

    fn priority(&self) -> i32 {
        self.priority
    }

    fn matches(&self, context: &RuleContext<'_>) -> Result<bool> {
        let matched = match self.target {
            Target::Projection => matches!(context.logical_plan(), LogicalPlan::Projection(_)),
            Target::Ordered => context.ordered(),
            Target::IneligibleAggregate => {
                matches!(context.logical_plan(), LogicalPlan::Aggregate(_))
                    && !context.group_eligible()
            }
        };
        if matched && matches!(self.replacement, Replacement::MatchError) {
            return Err(Error::Invalid("intentional matches failure".into()));
        }
        Ok(matched)
    }

    fn rewrite(&self, _context: &RuleContext<'_>) -> Result<RuleRewrite> {
        self.rewrites.fetch_add(1, Ordering::SeqCst);
        let maintenance = match self.replacement {
            Replacement::Recompute => Maintenance::Recompute,
            Replacement::InvalidPort => Maintenance::Delta(DeltaExpr::InputDelta(usize::MAX)),
            Replacement::InvalidArity => Maintenance::Delta(DeltaExpr::Linear(vec![])),
            Replacement::WrongSchema => Maintenance::Delta(DeltaExpr::InputDelta(0)),
            Replacement::SourceAtProjection => Maintenance::Delta(DeltaExpr::SourceDelta),
            Replacement::JoinAtProjection => Maintenance::Delta(DeltaExpr::Join {
                left: Box::new(DeltaExpr::Empty),
                right: Box::new(DeltaExpr::Empty),
            }),
            Replacement::Groups => Maintenance::ChangedGroups,
            Replacement::Constant => Maintenance::Constant,
            Replacement::EmptySum => Maintenance::Delta(DeltaExpr::Sum(vec![])),
            Replacement::Negates(count) => {
                let mut expression = DeltaExpr::Empty;
                for _ in 0..count {
                    expression = DeltaExpr::Negate(Box::new(expression));
                }
                Maintenance::Delta(expression)
            }
            Replacement::Terms(count) => {
                Maintenance::Delta(DeltaExpr::Sum(vec![DeltaExpr::Empty; count]))
            }
            Replacement::MatchError | Replacement::RewriteError => {
                return Err(Error::Invalid("intentional rewrite failure".into()));
            }
        };
        Ok(RuleRewrite {
            maintenance,
            reason: "public integration-test replacement".into(),
        })
    }
}

fn optimizer(rule: TestRule) -> IncrementalOptimizer {
    IncrementalOptimizer::default()
        .with_rule(Arc::new(rule))
        .unwrap()
}

async fn rejected(sql: &str, optimizer: IncrementalOptimizer, name: &str) -> Error {
    let error = SqlView::new_with_optimizer(sql, tables(), optimizer)
        .await
        .err()
        .expect("invalid replacement must fail before bootstrap");
    assert!(error.to_string().contains(name), "{error}");
    error
}

#[tokio::test]
async fn priority_then_stable_name_selects_the_actual_replacement_deterministically() {
    let a = || TestRule::projection("test-a", Replacement::Recompute);
    let z = || TestRule::projection("test-z", Replacement::Recompute);
    let first = IncrementalOptimizer::default()
        .with_rule(Arc::new(z()))
        .unwrap()
        .with_rule(Arc::new(a()))
        .unwrap();
    let second = IncrementalOptimizer::default()
        .with_rule(Arc::new(a()))
        .unwrap()
        .with_rule(Arc::new(z()))
        .unwrap();
    let first = SqlView::new_with_optimizer(SQL, tables(), first)
        .await
        .unwrap();
    let second = SqlView::new_with_optimizer(SQL, tables(), second)
        .await
        .unwrap();
    assert_eq!(
        first.incremental_plan().explain(),
        second.incremental_plan().explain()
    );
    let selected = first
        .incremental_plan()
        .nodes()
        .iter()
        .filter(|node| node.rule.name == "test-a")
        .collect::<Vec<_>>();
    assert!(!selected.is_empty());
    for node in selected {
        assert_eq!(node.rule.version, 7);
        assert_eq!(node.rule.priority, PRIORITY);
        assert_eq!(node.maintenance, Maintenance::Recompute);
        assert_eq!(node.reason, "public integration-test replacement");
    }
    let mut higher = z();
    higher.priority += 1;
    let higher = IncrementalOptimizer::default()
        .with_rule(Arc::new(a()))
        .unwrap()
        .with_rule(Arc::new(higher))
        .unwrap();
    let higher = SqlView::new_with_optimizer(SQL, tables(), higher)
        .await
        .unwrap();
    assert!(higher
        .incremental_plan()
        .nodes()
        .iter()
        .any(|node| node.rule.name == "test-z"));
    assert!(!higher
        .incremental_plan()
        .nodes()
        .iter()
        .any(|node| node.rule.name == "test-a"));
}

#[tokio::test]
async fn registration_removal_and_no_match_are_explicit() {
    let make = || TestRule::projection("test-remove", Replacement::Recompute);
    let registry = optimizer(make());
    assert!(registry.clone().with_rule(Arc::new(make())).is_err());
    assert!(IncrementalOptimizer::empty()
        .with_rule(Arc::new(TestRule::projection(" ", Replacement::Recompute)))
        .is_err());
    assert!(registry.clone().without_rule("misspelled-rule").is_err());
    let removed = registry.without_rule("test-remove").unwrap();
    let view = SqlView::new_with_optimizer(SQL, tables(), removed)
        .await
        .unwrap();
    assert!(view
        .incremental_plan()
        .nodes()
        .iter()
        .all(|node| node.rule.name != "test-remove"));
    assert!(
        SqlView::new_with_optimizer(SQL, tables(), IncrementalOptimizer::empty())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn removing_a_required_builtin_does_not_enable_a_universal_fallback() {
    let default = SqlView::new(SQL, tables()).await.unwrap();
    let source = default
        .incremental_plan()
        .nodes()
        .iter()
        .find(|node| {
            node.maintenance == Maintenance::Delta(DeltaExpr::Linear(vec![DeltaExpr::SourceDelta]))
        })
        .expect("this query requires a native source rule");
    let optimizer = IncrementalOptimizer::default()
        .without_rule(&source.rule.name)
        .unwrap();
    let error = SqlView::new_with_optimizer(SQL, tables(), optimizer)
        .await
        .err()
        .expect("remaining builtins must not hide the missing source implementation");
    let Error::Rewrite {
        node,
        phase,
        source: error,
        ..
    } = error
    else {
        panic!("missing implementation requires an identified planning error");
    };
    assert_eq!(node, source.id);
    assert_eq!(phase, "match");
    assert!(error.to_string().contains("no incremental rule matched"));
}

#[tokio::test]
async fn match_and_rewrite_errors_cannot_be_hidden_by_a_later_valid_rule() {
    for replacement in [Replacement::MatchError, Replacement::RewriteError] {
        let failure = TestRule::projection("test-fatal", replacement);
        let mut later = TestRule::projection("test-later-valid", Replacement::Recompute);
        later.priority -= 1;
        let later_rewrites = later.rewrites.clone();
        let registry = IncrementalOptimizer::default()
            .with_rule(Arc::new(later))
            .unwrap()
            .with_rule(Arc::new(failure))
            .unwrap();
        let error = rejected(SQL, registry, "test-fatal").await;
        assert!(error.to_string().contains("intentional"), "{error}");
        assert_eq!(later_rewrites.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn malformed_ports_arity_schemas_and_primitive_capabilities_fail_closed() {
    for replacement in [
        Replacement::InvalidPort,
        Replacement::InvalidArity,
        Replacement::WrongSchema,
        Replacement::SourceAtProjection,
        Replacement::JoinAtProjection,
        Replacement::Groups,
        Replacement::Constant,
        Replacement::EmptySum,
    ] {
        rejected(
            SQL,
            optimizer(TestRule::projection("test-invalid", replacement)),
            "test-invalid",
        )
        .await;
    }
}

#[tokio::test]
async fn ordered_nodes_and_unqualified_groups_cannot_adopt_native_replacements() {
    let mut ordered = TestRule::projection("test-ordered", Replacement::Negates(0));
    ordered.target = Target::Ordered;
    rejected(
        "SELECT k AS renamed FROM t ORDER BY v DESC, k ASC LIMIT 1",
        optimizer(ordered),
        "test-ordered",
    )
    .await;
    let mut global = TestRule::projection("test-unqualified-group", Replacement::Groups);
    global.target = Target::IneligibleAggregate;
    rejected(
        "SELECT SUM(v) AS total FROM t",
        optimizer(global),
        "test-unqualified-group",
    )
    .await;
}

#[tokio::test]
async fn expression_depth_and_size_limits_are_inclusive_and_bounded() {
    assert_eq!(MAX_EXPR_DEPTH, 64);
    assert_eq!(MAX_EXPR_NODES, 1024);
    for replacement in [
        Replacement::Negates(MAX_EXPR_DEPTH - 1),
        Replacement::Terms(MAX_EXPR_NODES - 1),
    ] {
        SqlView::new_with_optimizer(
            SQL,
            tables(),
            optimizer(TestRule::projection("test-boundary", replacement)),
        )
        .await
        .expect("the documented expression boundary must be accepted");
    }
    for replacement in [
        Replacement::Negates(MAX_EXPR_DEPTH),
        Replacement::Terms(MAX_EXPR_NODES),
    ] {
        rejected(
            SQL,
            optimizer(TestRule::projection("test-over-budget", replacement)),
            "test-over-budget",
        )
        .await;
    }
}

#[tokio::test]
async fn invalid_registry_cannot_publish_bootstrap_or_change_an_existing_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    assert!(DurableSqlView::open_with_optimizer(
        &path,
        SQL,
        tables(),
        IncrementalOptimizer::empty(),
    )
    .await
    .is_err());
    assert!(!path.join("CURRENT").exists());
    let view = DurableSqlView::open(&path, SQL, tables()).await.unwrap();
    drop(view);
    let current = std::fs::read(path.join("CURRENT")).unwrap();
    assert!(DurableSqlView::open_with_optimizer(
        &path,
        SQL,
        BTreeMap::new(),
        optimizer(TestRule::projection(
            "test-bad-reopen",
            Replacement::InvalidPort
        )),
    )
    .await
    .is_err());
    assert_eq!(std::fs::read(path.join("CURRENT")).unwrap(), current);
    let view = DurableSqlView::open(&path, SQL, BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(view.view().epoch(), 0);
}

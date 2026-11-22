//! Readable text tree and lowered physical DAG renderers for incremental plans.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;

use super::plan::{
    CompiledProgram, IndexAlgorithm, Operation, PhysicalStep, SnapshotDependency, StateSpec,
};
use super::{IncrementalPlan, Strategy};
use crate::planning::{PlanBlocker, PlanPreflight};

/// Output verbosity for text-based incremental plan explanations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExplainStyle {
    /// Visual Unicode DAG tree with strategies, rules, states, snapshots and blockers.
    #[default]
    Tree,
    /// `Tree` followed by each node's deduplicated lowered physical step DAG.
    Detailed,
}

/// Format a compiled incremental plan as a readable text DAG tree.
pub(crate) fn format_plan(plan: &IncrementalPlan, style: ExplainStyle) -> String {
    let nodes = plan.nodes();
    let programs = plan.programs();
    let mut strategy_counts = BTreeMap::<&'static str, usize>::new();
    let mut state_count = 0usize;
    let mut snapshot_count = 0usize;
    let mut blocker_count = 0usize;
    for node in nodes {
        let strategy = programs
            .get(node.id)
            .map_or_else(|| node.maintenance.strategy(), CompiledProgram::strategy);
        *strategy_counts.entry(strategy_label(strategy)).or_default() += 1;
        if strategy == Strategy::OperatorRecompute {
            blocker_count += 1;
        }
        if let Some(program) = programs.get(node.id) {
            if program.state.is_some() {
                state_count += 1;
            }
            snapshot_count += program.snapshot_dependencies().len();
        }
    }
    let strict_eligible = blocker_count == 0;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Incremental Plan (strict_eligible={strict_eligible}, nodes={}, states={state_count}, snapshots={snapshot_count}, blockers={blocker_count})",
        nodes.len()
    );
    write_strategy_summary(&mut out, &strategy_counts);
    let _ = writeln!(out, "Logical DAG:");

    let mut visited = BTreeSet::new();
    let root = plan.root();
    render_plan_node(
        plan,
        root,
        "",
        true,
        true,
        &mut visited,
        &BTreeMap::new(),
        &mut out,
    );

    if style == ExplainStyle::Detailed && !programs.is_empty() {
        write_physical_programs(&mut out, plan);
    }
    out
}

/// Format a schema-only preflight report (including rejected/blocked queries) as a text DAG tree.
pub(crate) fn format_preflight(preflight: &PlanPreflight) -> String {
    format_query_explain(preflight, None, ExplainStyle::Tree)
}

/// Format a combined preflight report and optional compiled plan as a readable text DAG tree.
pub(crate) fn format_query_explain(
    preflight: &PlanPreflight,
    plan: Option<&IncrementalPlan>,
    style: ExplainStyle,
) -> String {
    let blockers_by_node: BTreeMap<usize, Vec<&PlanBlocker>> = {
        let mut map: BTreeMap<usize, Vec<&PlanBlocker>> = BTreeMap::new();
        for blocker in &preflight.blockers {
            map.entry(blocker.id).or_default().push(blocker);
        }
        map
    };

    let mut strategy_counts = BTreeMap::<&'static str, usize>::new();
    let mut state_count = 0usize;
    let mut snapshot_count = 0usize;
    for node in &preflight.planned_nodes {
        let label = node.strategy.map_or("CompileFailed", strategy_label);
        *strategy_counts.entry(label).or_default() += 1;
        snapshot_count += node.snapshot_dependencies.len();
        if let Some(compiled) = plan.and_then(|p| p.programs().get(node.id)) {
            if compiled.state.is_some() {
                state_count += 1;
            }
        }
    }

    let mut out = String::new();
    let _ = writeln!(
        out,
        "Incremental Plan (strict_eligible={}, nodes={}, states={state_count}, snapshots={snapshot_count}, blockers={})",
        preflight.strict_plan_eligible,
        preflight.planned_nodes.len(),
        preflight.blockers.len()
    );
    write_strategy_summary(&mut out, &strategy_counts);
    if !preflight.numerical_columns.is_empty() {
        let _ = writeln!(out, "Numerical Columns: {:?}", preflight.numerical_columns);
    }
    let _ = writeln!(out, "Logical DAG:");

    let mut visited = BTreeSet::new();
    if let Some(plan) = plan {
        render_plan_node(
            plan,
            plan.root(),
            "",
            true,
            true,
            &mut visited,
            &blockers_by_node,
            &mut out,
        );
    } else if !preflight.planned_nodes.is_empty() {
        let root = preflight.planned_nodes.len() - 1;
        render_preflight_node(
            preflight,
            root,
            "",
            true,
            true,
            &mut visited,
            &blockers_by_node,
            &mut out,
        );
    }

    if !preflight.blockers.is_empty() {
        let _ = writeln!(out, "Blockers ({}):", preflight.blockers.len());
        for blocker in &preflight.blockers {
            let _ = writeln!(
                out,
                "  - #{} {} [{} / {}]: {}",
                blocker.id,
                short_operator_title(&blocker.operator),
                blocker.phase,
                blocker.rule,
                clean_inline(&blocker.reason)
            );
            for detail in &blocker.details {
                let _ = writeln!(out, "      detail: {}", clean_inline(detail));
            }
        }
    }

    if style == ExplainStyle::Detailed {
        if let Some(plan) = plan {
            if !plan.programs().is_empty() {
                write_physical_programs(&mut out, plan);
            }
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn render_plan_node(
    plan: &IncrementalPlan,
    id: usize,
    prefix: &str,
    is_last: bool,
    is_root: bool,
    visited: &mut BTreeSet<usize>,
    blockers_by_node: &BTreeMap<usize, Vec<&PlanBlocker>>,
    out: &mut String,
) {
    let branch = if is_root {
        ""
    } else if is_last {
        "└─ "
    } else {
        "├─ "
    };
    let node = &plan.nodes()[id];
    if !visited.insert(id) {
        let _ = writeln!(
            out,
            "{prefix}{branch}#{id} {} (shared, see above)",
            short_operator_title(&node.operator)
        );
        return;
    }

    let program = plan.programs().get(id);
    let strategy = program.map_or_else(|| node.maintenance.strategy(), CompiledProgram::strategy);
    let blocked = strategy == Strategy::OperatorRecompute || blockers_by_node.contains_key(&id);
    let strategy_tag = if blocked {
        format!("BLOCKED: {}", strategy_label(strategy))
    } else {
        strategy_label(strategy).to_owned()
    };

    let mut header = format!(
        "{prefix}{branch}#{id} {} [{strategy_tag}] rule={}@{}",
        short_operator_title(&node.operator),
        node.rule.name,
        node.rule.version
    );
    if node.ordered {
        header.push_str(" ordered=true");
    }
    if let Some(state) = program.and_then(|p| p.state.as_ref()) {
        let _ = write!(header, " state={}", format_state_spec(state));
    }
    let _ = writeln!(out, "{header}");

    let child_prefix = if is_root {
        String::new()
    } else if is_last {
        format!("{prefix}   ")
    } else {
        format!("{prefix}│  ")
    };
    let detail_bar = if node.inputs.is_empty() {
        "   "
    } else {
        "│  "
    };

    let op_inline = clean_inline(&node.operator);
    if op_inline != short_operator_title(&node.operator) {
        let _ = writeln!(out, "{child_prefix}{detail_bar}op: {op_inline}");
    }

    if let Some(deps) = program.map(CompiledProgram::snapshot_dependencies) {
        if !deps.is_empty() {
            let _ = writeln!(
                out,
                "{child_prefix}{detail_bar}snapshot: {}",
                format_snapshots(&deps)
            );
        }
    }

    if let Some(blockers) = blockers_by_node.get(&id) {
        for blocker in blockers {
            let _ = writeln!(
                out,
                "{child_prefix}{detail_bar}blocker: [{} / {}] {}",
                blocker.phase,
                blocker.rule,
                clean_inline(&blocker.reason)
            );
        }
    } else if strategy == Strategy::OperatorRecompute {
        let _ = writeln!(
            out,
            "{child_prefix}{detail_bar}blocker: [policy / {}] {}",
            node.rule.name,
            clean_inline(&node.reason)
        );
    }

    let last_idx = node.inputs.len().saturating_sub(1);
    for (idx, &child_id) in node.inputs.iter().enumerate() {
        render_plan_node(
            plan,
            child_id,
            &child_prefix,
            idx == last_idx,
            false,
            visited,
            blockers_by_node,
            out,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn render_preflight_node(
    preflight: &PlanPreflight,
    id: usize,
    prefix: &str,
    is_last: bool,
    is_root: bool,
    visited: &mut BTreeSet<usize>,
    blockers_by_node: &BTreeMap<usize, Vec<&PlanBlocker>>,
    out: &mut String,
) {
    let branch = if is_root {
        ""
    } else if is_last {
        "└─ "
    } else {
        "├─ "
    };
    let node = &preflight.planned_nodes[id];
    if !visited.insert(id) {
        let _ = writeln!(
            out,
            "{prefix}{branch}#{id} {} (shared, see above)",
            short_operator_title(&node.operator)
        );
        return;
    }

    let blocked = node.strategy.is_none()
        || node.strategy == Some(Strategy::OperatorRecompute)
        || blockers_by_node.contains_key(&id);
    let strategy_tag = match node.strategy {
        Some(strategy) if blocked => format!("BLOCKED: {}", strategy_label(strategy)),
        Some(strategy) => strategy_label(strategy).to_owned(),
        None => "BLOCKED: CompileFailed".to_owned(),
    };
    let rule_part = node.rule.as_ref().map_or_else(
        || "rule=<none>".to_owned(),
        |r| format!("rule={}@{}", r.name, r.version),
    );

    let _ = writeln!(
        out,
        "{prefix}{branch}#{id} {} [{strategy_tag}] {rule_part}",
        short_operator_title(&node.operator)
    );

    let child_prefix = if is_root {
        String::new()
    } else if is_last {
        format!("{prefix}   ")
    } else {
        format!("{prefix}│  ")
    };
    let detail_bar = if node.inputs.is_empty() {
        "   "
    } else {
        "│  "
    };

    let op_inline = clean_inline(&node.operator);
    if op_inline != short_operator_title(&node.operator) {
        let _ = writeln!(out, "{child_prefix}{detail_bar}op: {op_inline}");
    }
    if !node.snapshot_dependencies.is_empty() {
        let _ = writeln!(
            out,
            "{child_prefix}{detail_bar}snapshot: {}",
            format_snapshots(&node.snapshot_dependencies)
        );
    }
    if let Some(blockers) = blockers_by_node.get(&id) {
        for blocker in blockers {
            let _ = writeln!(
                out,
                "{child_prefix}{detail_bar}blocker: [{} / {}] {}",
                blocker.phase,
                blocker.rule,
                clean_inline(&blocker.reason)
            );
        }
    }

    let last_idx = node.inputs.len().saturating_sub(1);
    for (idx, &child_id) in node.inputs.iter().enumerate() {
        render_preflight_node(
            preflight,
            child_id,
            &child_prefix,
            idx == last_idx,
            false,
            visited,
            blockers_by_node,
            out,
        );
    }
}

fn write_strategy_summary(out: &mut String, counts: &BTreeMap<&'static str, usize>) {
    if counts.is_empty() {
        return;
    }
    let parts: Vec<String> = counts.iter().map(|(k, v)| format!("{k}={v}")).collect();
    let _ = writeln!(out, "Strategies: {}", parts.join(", "));
}

fn write_physical_programs(out: &mut String, plan: &IncrementalPlan) {
    let _ = writeln!(out, "Physical Programs (DAG steps):");
    for (id, program) in plan.programs().iter().enumerate() {
        let node = &plan.nodes()[id];
        let _ = writeln!(
            out,
            "  #{id} {} [{}] ({} steps):",
            short_operator_title(&node.operator),
            strategy_label(program.strategy()),
            program.steps.len()
        );
        for (step_id, step) in program.steps.iter().enumerate() {
            let _ = writeln!(out, "    %{step_id} = {}", format_step(step));
        }
    }
}

fn format_step(step: &PhysicalStep) -> String {
    let inputs = if step.inputs.is_empty() {
        String::new()
    } else {
        let refs: Vec<String> = step.inputs.iter().map(|i| format!("%{i}")).collect();
        format!("({})", refs.join(", "))
    };
    match &step.operation {
        Operation::SourceRead { name, delta, .. } => {
            format!(
                "SourceRead({name}, {})",
                if *delta { "delta" } else { "snapshot" }
            )
        }
        Operation::DeltaRead { input, .. } => format!("DeltaRead(#{input})"),
        Operation::SnapshotRead { input, version, .. } => {
            format!("SnapshotRead(#{input}, {version:?})")
        }
        Operation::SnapshotReadWhenChanged {
            input,
            version,
            trigger,
            ..
        } => format!("SnapshotReadWhenChanged(#{input}, {version:?}, trigger=#{trigger}){inputs}"),
        Operation::Empty(_) => "Empty".to_owned(),
        Operation::Sum => format!("Sum{inputs}"),
        Operation::Negate => format!("Negate{inputs}"),
        Operation::Replace => format!("Replace{inputs}"),
        Operation::Apply { operator, mode } => {
            format!(
                "Apply[{mode:?}]{inputs} {}",
                clean_inline(&operator.template.display().to_string())
            )
        }
        Operation::ProjectKeys(operator) => {
            format!(
                "ProjectKeys{inputs} {}",
                clean_inline(&operator.template.display().to_string())
            )
        }
        Operation::PrepareIndex(spec) => {
            format!(
                "PrepareIndex(state=#{}, scope={:?}){inputs}",
                spec.id, spec.scope
            )
        }
        Operation::CompleteIndex(spec) => {
            format!(
                "CompleteIndex(state=#{}, scope={:?}){inputs}",
                spec.id, spec.scope
            )
        }
        Operation::MaintainMatchJoin { state, operator } => {
            format!(
                "MaintainMatchJoin(state=#{state}){inputs} {}",
                clean_inline(&operator.template.display().to_string())
            )
        }
        Operation::IndexLookup {
            state,
            port,
            version,
            ..
        } => format!("IndexLookup(state=#{state}, port={port}, {version:?}){inputs}"),
        Operation::CachedOutput { state, .. } => format!("CachedOutput(state=#{state}){inputs}"),
        Operation::EvaluateSnapshot {
            state,
            version,
            operator,
        } => format!(
            "EvaluateSnapshot(state=#{state}, {version:?}){inputs} {}",
            clean_inline(&operator.template.display().to_string())
        ),
        Operation::PrepareGroups { state } => format!("PrepareGroups(state=#{state}){inputs}"),
        Operation::GroupLookup { state, .. } => format!("GroupLookup(state=#{state}){inputs}"),
        Operation::CompleteGroups { state } => format!("CompleteGroups(state=#{state}){inputs}"),
        Operation::UpdateAccumulator { state, .. } => {
            format!("UpdateAccumulator(state=#{state}){inputs}")
        }
        Operation::UpdateRank { state, .. } => format!("UpdateRank(state=#{state}){inputs}"),
    }
}

fn format_state_spec(state: &StateSpec) -> String {
    match state {
        StateSpec::Accumulator { id, .. } => format!("Accumulator(#{id})"),
        StateSpec::Rank { id, .. } => format!("Rank(#{id})"),
        StateSpec::Group { id, .. } => format!("Group(#{id})"),
        StateSpec::Keyed(spec) => {
            let algo = match spec.algorithm {
                IndexAlgorithm::Scoped => "Scoped",
                IndexAlgorithm::MatchCounts(_) => "MatchCounts",
            };
            format!(
                "Keyed(#{}, {algo}, scope={:?}, before={}, after={})",
                spec.id, spec.scope, spec.before, spec.after
            )
        }
    }
}

fn format_snapshots(deps: &[SnapshotDependency]) -> String {
    deps.iter()
        .map(|dep| match dep.trigger {
            Some(trigger) => format!("#{}({:?}, trigger=#{trigger})", dep.input, dep.version),
            None => format!("#{}({:?}, unconditional)", dep.input, dep.version),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn strategy_label(strategy: Strategy) -> &'static str {
    match strategy {
        Strategy::NativeDelta => "NativeDelta",
        Strategy::IndexedDelta => "IndexedDelta",
        Strategy::MatchCountDelta => "MatchCountDelta",
        Strategy::KeyedRecompute => "KeyedRecompute",
        Strategy::AccumulatorDelta => "AccumulatorDelta",
        Strategy::RankDelta => "RankDelta",
        Strategy::ChangedGroupRecompute => "ChangedGroupRecompute",
        Strategy::OperatorRecompute => "OperatorRecompute",
    }
}

fn short_operator_title(operator: &str) -> String {
    let inline = clean_inline(operator);
    if let Some(rest) = inline.strip_prefix("TableScan: ") {
        let table = rest.split_whitespace().next().unwrap_or(rest);
        return format!("TableScan: {table}");
    }
    if let Some((head, _)) = inline.split_once(':') {
        return head.trim().to_owned();
    }
    inline
}

fn clean_inline(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

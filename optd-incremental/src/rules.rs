// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! The delta rules: pushing the change marker `Δ` from the view root down to the
//! scans.
//!
//! Each rule is the operator's delta form from `docs/src/incremental/`. Two
//! properties the implementation enforces deliberately:
//!
//! * **Totality or failure.** If an operator has no delta rule, the rewrite
//!   returns [`RewriteError::Unresolved`]. A partially rewritten plan would still
//!   *execute* and would silently produce the changes-only result of the part that
//!   was rewritten — wrong data, not an error (`overview.md` §4.2). Callers must
//!   treat this as "cannot maintain incrementally", not as "assume nothing".
//! * **The join rule's snapshot side is built, not read.**
//!   `Σ(r, before) = r ⊖ Δr` is written as `Union(r, Negate(Δr))`, so no versioned
//!   storage is required (`change_capture.md` §5).

use crate::plan::{
    agg, children_of, delta, empty, filter, join, merge_agg, negate, project, rebuild, stored_view,
    union, IncNodeType, IncPlanNode,
};

/// Failure to derive a complete executable delta expression from a concrete plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RewriteError {
    /// An operator that has no delta rule in this milestone. The caller must fall
    /// back to a full rebuild.
    Unresolved(String),
    /// The plan is not shaped the way the rules expect (a marker where one cannot
    /// appear). This is a bug in the plan construction, not a missing feature.
    Malformed(String),
}

impl std::fmt::Display for RewriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RewriteError::Unresolved(op) => {
                write!(f, "no delta rule for `{}`: rebuild required", op)
            }
            RewriteError::Malformed(m) => write!(f, "malformed plan: {}", m),
        }
    }
}

/// Rewrite a plan so that every `Δ` has been pushed to a scan.
///
/// Idempotent on an already-rewritten plan: `Delta(Scan)` remains as the resolved
/// change-feed read. The view driver rewrites once at load and reuses that plan.
pub fn rewrite(plan: IncPlanNode) -> Result<IncPlanNode, RewriteError> {
    if plan.typ == IncNodeType::Delta {
        return rewrite_delta(&plan);
    }
    let children = children_of(&plan)
        .into_iter()
        .map(|c| rewrite(c.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rebuild(plan.typ.clone(), children, plan.predicates.clone()))
}

/// Resolve one `Δ( operator(…))` node.
fn rewrite_delta(marked: &IncPlanNode) -> Result<IncPlanNode, RewriteError> {
    if marked.children.len() != 1 || children_of(marked).len() != 1 {
        return Err(RewriteError::Malformed(
            "Delta must have one concrete child".into(),
        ));
    }
    let child = match children_of(marked).first() {
        Some(c) => (*c).clone(),
        None => return Err(RewriteError::Malformed("Delta with no child".into())),
    };
    let grand_children = children_of(&child);
    let arity = match &child.typ {
        IncNodeType::Scan(_) | IncNodeType::EmptyRelation | IncNodeType::StoredView => 0,
        IncNodeType::Join(_) | IncNodeType::Union | IncNodeType::MergeAgg { .. } => 2,
        _ => 1,
    };
    if grand_children.len() != arity || grand_children.len() != child.children.len() {
        return Err(RewriteError::Malformed(format!(
            "invalid concrete arity for {}",
            child.typ
        )));
    }

    match child.typ.clone() {
        // --- leaves ---------------------------------------------------------
        // A scan under `Δ` is where the change feed is read; it is resolved by
        // the interpreter, not rewritten further.
        IncNodeType::Scan(_) => Ok(marked.clone()),
        // The empty relation never changes, so its delta is itself (empty).
        IncNodeType::EmptyRelation => Ok(empty()),

        // --- transposition rules --------------------------------------------
        // Δ(π(X)) = π(ΔX)
        IncNodeType::Projection(cols) => {
            let inner = rewrite_delta(&delta(grand_children[0].clone()))?;
            Ok(project(inner, cols))
        }
        // Δ(σ_p(X)) = σ_p(ΔX)  — same predicate object, so NULL semantics cannot
        // drift between the delta path and a rebuild.
        IncNodeType::Filter => {
            let pred = child
                .predicates
                .first()
                .map(|p| p.typ.clone())
                .ok_or_else(|| RewriteError::Malformed("Filter without predicate".into()))?;
            let inner = rewrite_delta(&delta(grand_children[0].clone()))?;
            Ok(filter(inner, pred))
        }
        // Negation commutes with the marker: Δ(⊖X) = ⊖(ΔX).
        IncNodeType::Negate => {
            let inner = rewrite_delta(&delta(grand_children[0].clone()))?;
            Ok(negate(inner))
        }
        // Δ(X ∪ Y) = ΔX ∪ ΔY — union is the only connective delta algebra needs.
        IncNodeType::Union => {
            let l = rewrite_delta(&delta(grand_children[0].clone()))?;
            let r = rewrite_delta(&delta(grand_children[1].clone()))?;
            Ok(union(l, r))
        }

        // Δ(A ⋈ B) = ΔA ⋈ Σ(B, before)  ∪  Σ(A, after) ⋈ ΔB
        //   Σ(B, before) = B ⊖ ΔB, built here as Union(B, Negate(ΔB))
        //   Σ(A, after)  = A as it now stands
        IncNodeType::Join(keys) => {
            let left = grand_children[0].clone();
            let right = grand_children[1].clone();
            let d_left = rewrite_delta(&delta(left.clone()))?;
            let d_right = rewrite_delta(&delta(right.clone()))?;
            let right_before = union(right, negate(d_right.clone()));
            Ok(union(
                join(d_left, right_before, keys.clone()),
                join(left, d_right, keys),
            ))
        }

        // --- terminal rule ---------------------------------------------------
        // Δ(γ(X)) is *not* γ(ΔX): a group also contains the unchanged rows still
        // contributing to it. Compute the partial aggregate of the delta, then
        // merge it into the stored state (`agg.md` §1).
        IncNodeType::Agg { groups, aggs } => {
            let inner = rewrite_delta(&delta(grand_children[0].clone()))?;
            let partial = agg(inner, groups.clone(), aggs.clone());
            // MergeAgg produces the *whole new state*, not a change to it. The
            // driver unions whatever this rule returns into the view's contents,
            // so we must subtract the old state — otherwise every group untouched
            // by this batch is counted twice. `new ⊖ old` is the real delta and
            // keeps the uniform "every rule returns a delta" contract.
            Ok(union(
                merge_agg(partial, stored_view(), groups.clone(), aggs.clone()),
                negate(stored_view()),
            ))
        }

        // --- no rule ---------------------------------------------------------
        IncNodeType::Delta => Err(RewriteError::Malformed("nested Delta".into())),
        IncNodeType::MergeAgg { .. } => Err(RewriteError::Unresolved("MergeAgg".into())),
        IncNodeType::StoredView => Err(RewriteError::Malformed(
            "the view cannot be defined in terms of itself".into(),
        )),
    }
}

/// True when the plan contains a change marker that the rules could not resolve.
/// The driver uses this as the guard against silently emitting a delta-only plan.
pub fn has_unresolved_delta(plan: &IncPlanNode) -> bool {
    if plan.typ == IncNodeType::Delta {
        // A marker whose child is a scan is resolved: it reads the change feed.
        return match children_of(plan).first() {
            Some(c) => !matches!(c.typ, IncNodeType::Scan(_)),
            None => true,
        };
    }
    children_of(plan).into_iter().any(has_unresolved_delta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{scan, AggKind, Expr};

    #[test]
    fn projection_and_filter_transpose() {
        let plan = project(
            filter(
                scan(0),
                Expr::Gt(
                    Box::new(Expr::Col(0)),
                    Box::new(Expr::Lit(crate::change::Value::Int(0))),
                ),
            ),
            vec![0],
        );
        let d = rewrite(delta(plan)).unwrap();
        // The marker must have moved below both operators, onto the scan.
        assert!(matches!(d.typ, IncNodeType::Projection(_)));
        let inner = children_of(&d)[0];
        assert_eq!(inner.typ, IncNodeType::Filter);
        let leaf = children_of(inner)[0];
        assert_eq!(leaf.typ, IncNodeType::Delta);
        assert!(matches!(children_of(leaf)[0].typ, IncNodeType::Scan(0)));
    }

    #[test]
    fn join_delta_has_two_branches_and_builds_the_before_snapshot() {
        let plan = join(scan(0), scan(1), vec![(0, 0)]);
        let d = rewrite(delta(plan)).unwrap();
        assert_eq!(d.typ, IncNodeType::Union, "Δ(A⋈B) must be a union");
        let branches = children_of(&d);
        assert_eq!(branches.len(), 2);
        // Branch 2 is Σ(A,after) ⋈ ΔB: its right child is a marker on a scan.
        let right_marker = children_of(branches[1])[1];
        assert_eq!(right_marker.typ, IncNodeType::Delta);
        // Branch 1's right child is the *before* snapshot: Union(B, Negate(ΔB)).
        let before = children_of(branches[0])[1];
        assert_eq!(before.typ, IncNodeType::Union);
        assert_eq!(children_of(before)[1].typ, IncNodeType::Negate);
    }

    #[test]
    fn union_delta_is_a_union_of_deltas() {
        let d = rewrite(delta(union(scan(0), scan(1)))).unwrap();
        assert_eq!(d.typ, IncNodeType::Union);
    }

    #[test]
    fn aggregate_is_terminal_and_merges_with_stored_state() {
        let plan = agg(scan(0), vec![0], vec![AggKind::Count]);
        let d = rewrite(delta(plan)).unwrap();
        // The rule must produce a *delta*, i.e. new-state minus old-state, or the
        // driver would double-count every group the batch did not touch.
        assert_eq!(d.typ, IncNodeType::Union);
        let kids = children_of(&d);
        assert!(matches!(kids[0].typ, IncNodeType::MergeAgg { .. }));
        assert_eq!(
            kids[1].typ,
            IncNodeType::Negate,
            "old state must be subtracted"
        );
        let merge_kids = children_of(kids[0]);
        assert_eq!(merge_kids[1].typ, IncNodeType::StoredView);
        assert!(matches!(merge_kids[0].typ, IncNodeType::Agg { .. }));
    }

    #[test]
    fn an_operator_without_a_rule_fails_loudly() {
        // StoredView cannot appear under a delta; the rewrite must refuse rather
        // than emit a partial plan.
        let err = rewrite(delta(stored_view())).unwrap_err();
        assert!(matches!(err, RewriteError::Malformed(_)));
    }
}

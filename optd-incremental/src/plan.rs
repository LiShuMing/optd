// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! The incremental plan representation.
//!
//! Nodes are `optd-core`'s [`PlanNode`], parameterised by [`IncNodeType`]. We
//! define our own operator set rather than reusing another crate's, for one
//! concrete reason: **delta algebra needs a multiset `Union`**
//! (`docs/src/incremental/overview.md` §7 item 1 — `Δ(A⋈B)` *is* a union of
//! branches). Defining the set here means `Union` exists from the first commit
//! and no existing crate has to change.

use std::fmt;
use std::sync::Arc;

use optd_core::nodes::{ArcPlanNode, ArcPredNode, PlanNode, PlanNodeOrGroup, PredNode};

use crate::change::{TableId, Value};

/// A column reference within the input row of an operator.
pub type ColRef = usize;

/// Scalar expressions, used as the predicate type of a node.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Expr {
    Col(ColRef),
    Lit(Value),
    /// `a > b` — enough to exercise filtering; the framework does not need a full
    /// expression language to demonstrate the algebra.
    Gt(Box<Expr>, Box<Expr>),
    /// `a = b`
    Eq(Box<Expr>, Box<Expr>),
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Col(i) => write!(f, "c{}", i),
            Expr::Lit(v) => write!(f, "{}", v),
            Expr::Gt(a, b) => write!(f, "{} > {}", a, b),
            Expr::Eq(a, b) => write!(f, "{} = {}", a, b),
        }
    }
}

/// The aggregate functions the framework supports in this milestone.
///
/// `Sum`/`Count` are the invertible pair; non-invertible functions (`Min`/`Max`)
/// and `DISTINCT` aggregates are deliberately out of scope for milestone 1 — see
/// `docs/src/incremental/agg.md` §1 for why they need extra state.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum AggKind {
    Count,
    Sum(ColRef),
}

/// The operator set.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum IncNodeType {
    /// Read a base table. Under a `Delta` marker this becomes "read the changes
    /// to the table in the current refresh range".
    Scan(TableId),
    /// The empty relation. Present so the rewrite pass is *total*.
    EmptyRelation,
    /// Keep the listed columns, in order.
    Projection(Vec<ColRef>),
    /// Predicate lives in `predicates[0]`.
    Filter,
    /// Inner equi-join on `(left_col, right_col)` pairs.
    Join(Vec<(ColRef, ColRef)>),
    /// Multiset union of both children.
    Union,
    /// Group by `groups`; aggregate `aggs`.
    ///
    /// The output is the group keys followed by the **accumulator columns** — the
    /// aggregate's intermediate state, not a finalised value. Storing state rather
    /// than the value is what lets a later batch merge into it
    /// (`docs/src/incremental/agg.md` §1).
    Agg { groups: Vec<ColRef>, aggs: Vec<AggKind> },
    /// Flip every weight of the input. This is what makes
    /// `Σ(before) = Σ(after) ⊖ Δ` expressible as a plan, so no versioned storage
    /// is needed for the join rule's snapshot side.
    Negate,
    /// Merge a partial aggregate of a delta into the stored aggregate state:
    /// children are `[partial, stored]`. Groups whose count reaches zero are
    /// dropped (a group that sums to zero is *not* empty — see `agg.md` §1).
    MergeAgg { groups: Vec<ColRef>, aggs: Vec<AggKind> },
    /// The **change marker**: `Δ(child)` is the change to `child`, not its value.
    Delta,
    /// The view's own current contents, readable as an input relation.
    ///
    /// A terminal rule (the aggregate merge) has to read the state it is
    /// updating; this is that state made addressable, rather than a special case
    /// hidden in the driver.
    StoredView,
}

impl fmt::Display for IncNodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IncNodeType::Scan(t) => write!(f, "Scan({})", t),
            IncNodeType::EmptyRelation => write!(f, "Empty"),
            IncNodeType::Projection(cols) => write!(f, "Proj{:?}", cols),
            IncNodeType::Filter => write!(f, "Filter"),
            IncNodeType::Join(keys) => write!(f, "Join{:?}", keys),
            IncNodeType::Union => write!(f, "Union"),
            IncNodeType::Agg { groups, aggs } => write!(f, "Agg{:?}{:?}", groups, aggs),
            IncNodeType::Delta => write!(f, "Delta"),
            IncNodeType::Negate => write!(f, "Negate"),
            IncNodeType::MergeAgg { .. } => write!(f, "MergeAgg"),
            IncNodeType::StoredView => write!(f, "StoredView"),
        }
    }
}

impl optd_core::nodes::NodeType for IncNodeType {
    type PredType = Expr;

    fn is_logical(&self) -> bool {
        // `Delta` is a logical marker for a rewrite, not a physical operator.
        !matches!(self, IncNodeType::Delta)
    }
}

pub type IncPlanNode = ArcPlanNode<IncNodeType>;

fn node(typ: IncNodeType, children: Vec<IncPlanNode>) -> IncPlanNode {
    Arc::new(PlanNode {
        typ,
        children: children
            .into_iter()
            .map(|c| PlanNodeOrGroup::PlanNode(c))
            .collect(),
        predicates: vec![],
    })
}

fn node_with_pred(typ: IncNodeType, children: Vec<IncPlanNode>, pred: Expr) -> IncPlanNode {
    let predicate: ArcPredNode<IncNodeType> = Arc::new(PredNode {
        typ: pred,
        children: vec![],
        data: None,
    });
    Arc::new(PlanNode {
        typ,
        children: children
            .into_iter()
            .map(|c| PlanNodeOrGroup::PlanNode(c))
            .collect(),
        predicates: vec![predicate],
    })
}

// --- constructors -----------------------------------------------------------

pub fn scan(table: TableId) -> IncPlanNode {
    node(IncNodeType::Scan(table), vec![])
}

pub fn empty() -> IncPlanNode {
    node(IncNodeType::EmptyRelation, vec![])
}

pub fn project(child: IncPlanNode, cols: Vec<ColRef>) -> IncPlanNode {
    node(IncNodeType::Projection(cols), vec![child])
}

pub fn filter(child: IncPlanNode, pred: Expr) -> IncPlanNode {
    node_with_pred(IncNodeType::Filter, vec![child], pred)
}

pub fn join(left: IncPlanNode, right: IncPlanNode, keys: Vec<(ColRef, ColRef)>) -> IncPlanNode {
    node(IncNodeType::Join(keys), vec![left, right])
}

pub fn union(left: IncPlanNode, right: IncPlanNode) -> IncPlanNode {
    node(IncNodeType::Union, vec![left, right])
}

pub fn agg(child: IncPlanNode, groups: Vec<ColRef>, aggs: Vec<AggKind>) -> IncPlanNode {
    node(IncNodeType::Agg { groups, aggs }, vec![child])
}

pub fn delta(child: IncPlanNode) -> IncPlanNode {
    node(IncNodeType::Delta, vec![child])
}

pub fn negate(child: IncPlanNode) -> IncPlanNode {
    node(IncNodeType::Negate, vec![child])
}

pub fn stored_view() -> IncPlanNode {
    node(IncNodeType::StoredView, vec![])
}

/// Merge a partial aggregate with the stored state: children are `[partial, stored]`.
pub fn merge_agg(
    partial: IncPlanNode,
    stored: IncPlanNode,
    groups: Vec<ColRef>,
    aggs: Vec<AggKind>,
) -> IncPlanNode {
    node(IncNodeType::MergeAgg { groups, aggs }, vec![partial, stored])
}


/// Rebuild a node with new children, preserving its type and predicates.
pub fn rebuild(typ: IncNodeType, children: Vec<IncPlanNode>, predicates: Vec<ArcPredNode<IncNodeType>>) -> IncPlanNode {
    Arc::new(PlanNode {
        typ,
        children: children
            .into_iter()
            .map(PlanNodeOrGroup::PlanNode)
            .collect(),
        predicates,
    })
}

// --- accessors --------------------------------------------------------------

impl IncNodeType {
    /// The predicate expression of a `Filter`, if any.
    pub fn predicate(&self, node: &PlanNode<IncNodeType>) -> Option<Expr> {
        node.predicates.first().map(|p| p.typ.clone())
    }
}

pub fn children_of(node: &PlanNode<IncNodeType>) -> Vec<&IncPlanNode> {
    node.children
        .iter()
        .filter_map(|c| match c {
            PlanNodeOrGroup::PlanNode(p) => Some(p),
            PlanNodeOrGroup::Group(_) => None,
        })
        .collect()
}
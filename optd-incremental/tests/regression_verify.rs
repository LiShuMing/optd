// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Regression tests from the independent verification of 2026-10-27.
//!
//! These encode the **required** behaviour, not the current behaviour: against
//! the implementation as verified they are expected to be RED. They turn green
//! only when the corresponding fix lands. Each test names the finding it pins
//! and is written so that more than one legitimate fix satisfies it — e.g. the
//! aggregate test accepts either "reject the plan at LOAD" or "maintain it
//! correctly", because both remove the silent-wrongness.
//!
//! Findings (full write-up: `optd-incremental-verify-report.md`):
//!
//! * A — the aggregate rule reads `StoredView` (the whole view's contents) as
//!   the aggregate state, so an aggregate that is not the plan root is merged
//!   against rows that are not accumulator rows. Not rejected anywhere.
//! * B — `View::refresh` commits `contents` before the negative-count guard, so
//!   an `Err` leaves the view mutated with a stale watermark; replaying the
//!   range then wedges the view permanently.
//! * C — `View::load` takes the base-table list from the caller and never
//!   checks it against the plan; an omitted table is silently never refreshed.
//!
//! The tests build views through the public API only; no `src/` file is
//! touched. Change batches use rows that are currently present, so the feed
//! itself stays a valid change log once the fix is in.

use optd_incremental::change::{Capabilities, Multiset, Row, Value};
use optd_incremental::engine::View;
use optd_incremental::plan::{agg, filter, join, project, scan, AggKind, Expr, IncPlanNode};
use optd_incremental::source::InMemorySource;

const T_A: u32 = 0;
const T_B: u32 = 1;

fn row(vals: &[i64]) -> Row {
    vals.iter().map(|v| Value::Int(*v)).collect()
}

fn source_with(seed: &[(u32, Row)]) -> InMemorySource {
    let mut src = InMemorySource::new(Capabilities::change_log());
    for (t, r) in seed {
        src.insert(*t, r.clone());
    }
    src
}

/// Finding A. An aggregate below another operator must either be refused at
/// LOAD or maintained correctly; what it must not do is load and then diverge
/// from a full recompute.
fn aggregate_plan_must_be_refused_or_correct(
    name: &str,
    plan: IncPlanNode,
    seed: &[(u32, Row)],
    extra: (u32, Row),
) {
    let mut src = source_with(seed);

    let mut view = match View::load(plan, &src, vec![T_A]) {
        // Rejecting a plan this milestone cannot maintain soundly is a valid fix.
        Err(_) => return,
        Ok(view) => view,
    };

    assert_eq!(
        view.contents(),
        &view.recompute(&src).unwrap(),
        "{name}: LOAD accepted the plan but disagrees with a recompute"
    );

    src.insert(extra.0, extra.1);
    view.refresh(&src)
        .unwrap_or_else(|e| panic!("{name}: refresh failed: {e}"));

    assert_eq!(
        view.contents(),
        &view.recompute(&src).unwrap(),
        "{name}: a non-root aggregate diverged from a full recompute"
    );
}

#[test]
fn a1_aggregate_under_projection_must_be_refused_or_correct() {
    // SELECT key FROM (SELECT key, count(*) FROM t GROUP BY key)
    let plan = project(
        agg(scan(T_A), vec![0], vec![AggKind::Count]),
        vec![0], // drops the accumulator column the merge rule needs
    );
    aggregate_plan_must_be_refused_or_correct(
        "agg under projection",
        plan,
        &[
            (T_A, row(&[0, 10])),
            (T_A, row(&[0, 20])),
            (T_A, row(&[1, 30])),
        ],
        (T_A, row(&[1, 40])),
    );
}

#[test]
fn a2_having_must_be_refused_or_correct() {
    // HAVING count > 1 == filter over the aggregate
    let plan = filter(
        agg(scan(T_A), vec![0], vec![AggKind::Count]),
        Expr::Gt(Box::new(Expr::Col(1)), Box::new(Expr::Lit(Value::Int(1)))),
    );
    aggregate_plan_must_be_refused_or_correct(
        "having",
        plan,
        &[
            (T_A, row(&[0, 10])),
            (T_A, row(&[0, 20])),
            (T_A, row(&[1, 30])),
        ],
        // group 1 reaches count 2, so HAVING must let it appear
        (T_A, row(&[1, 40])),
    );
}

/// Finding B. A failed refresh must leave the view exactly as it was. Today the
/// negative-count guard fires *after* `contents` has been mutated and *before*
/// the watermark advances, so the range is replayed onto corrupt state and the
/// view never recovers.
#[test]
fn a_failed_refresh_must_not_mutate_the_view() {
    let mut src = source_with(&[(T_A, row(&[0, 1]))]);
    let plan = project(scan(T_A), vec![0, 1]);
    let mut view = View::load(plan, &src, vec![T_A]).expect("LOAD");

    let before: Multiset = view.contents().clone();
    let watermark_before = view.watermarks().get(&T_A).copied();

    // Retract a row that is not there: the maintained view would go negative.
    src.apply_batch(vec![(T_A, row(&[9, 9]), -1)]);
    view.refresh(&src)
        .expect_err("the negative-count guard must fire");

    assert_eq!(
        view.contents(),
        &before,
        "a failed refresh must not mutate contents (finding B)"
    );
    assert_eq!(
        view.watermarks().get(&T_A).copied(),
        watermark_before,
        "a failed refresh must not advance the watermark (finding B)"
    );

    // Repair the feed: the view must recover, not stay wedged.
    src.apply_batch(vec![(T_A, row(&[9, 9]), 1)]);
    view.refresh(&src)
        .expect("after the feed is repaired the view must refresh again");
    assert_eq!(view.contents(), &view.recompute(&src).unwrap());
}

/// Finding C. If `View::load` succeeds, the view must track every table its plan
/// reads. Passing an incomplete table list is a caller error, but it must be
/// rejected or compensated for — never silently ignored.
#[test]
fn load_must_not_ignore_a_table_referenced_by_the_plan() {
    let mut src = source_with(&[(T_A, row(&[0, 1])), (T_B, row(&[1]))]);
    let plan = join(scan(T_A), scan(T_B), vec![(1, 0)]);

    // The plan reads T_B; the caller lists only T_A.
    let mut view = match View::load(plan, &src, vec![T_A]) {
        // Rejecting a list that does not match the plan is a valid fix.
        Err(_) => return,
        Ok(view) => view,
    };

    src.insert(T_B, row(&[1])); // the join result gains a row
    view.refresh(&src).expect("refresh");
    assert_eq!(
        view.contents(),
        &view.recompute(&src).unwrap(),
        "a table omitted from the caller's table list was silently not refreshed (finding C)"
    );
}
/// D — `SUM` over a non-integer column must not silently accumulate zero.
///
/// The Iceberg feed maps decimal, float and binary columns to `Value::Str`.
/// Before this fix `interp::aggregate` matched only `Value::Int` and dropped
/// everything else, so `SUM(decimal_col)` returned 0 while `count(*)` stayed
/// correct — a wrong answer that looks like a real one. The interpreter now
/// rejects it; widening the accumulator would be the other legitimate fix and
/// would make this test green by returning the right sum instead of an error.
#[test]
fn d_sum_over_a_non_integer_column_is_refused() {
    use optd_incremental::interp::aggregate;
    use optd_incremental::plan::AggKind;

    let mut input = Multiset::new();
    input.add_row(vec![Value::Int(1), Value::Str("12.5".into())], 1);
    let err = aggregate(&input, &[0], &[AggKind::Sum(1)]).unwrap_err();
    assert!(err.contains("SUM"), "{err}");
}

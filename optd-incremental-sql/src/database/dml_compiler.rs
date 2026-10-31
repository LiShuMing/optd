//! Compile one DML statement into old/new typed row images from one snapshot.

use super::dml_work::{DmlExecutionWork, SelectorWork};
use super::response::DmlWork;
use super::store::DatabaseSnapshot;
use super::table_rows::TableRows;
use super::{selector_work, sql};
use crate::bag::Bag;
use crate::error::invalid;
use crate::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, FromTable, Statement, TableFactor, TableObject, TableWithJoins,
};
use std::collections::BTreeMap;

/// Explicit transactions cannot read materialized views that reflect committed state.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum TransactionMode {
    Autocommit,
    Explicit,
}

struct RowImages {
    table: String,
    old: RecordBatch,
    new: RecordBatch,
    source_plan: LogicalPlan,
    source_work: SelectorWork,
}

fn target(table: &TableWithJoins) -> Result<String> {
    if !table.joins.is_empty() {
        return Err(invalid("joined DML target is not supported"));
    }
    let TableFactor::Table {
        name: target,
        alias: None,
        args: None,
        ..
    } = &table.relation
    else {
        return Err(invalid("expected a plain DML table target"));
    };
    let n = sql::name(target)?;
    if table.to_string() != target.to_string() {
        return Err(invalid("DML target modifiers are not supported"));
    }
    Ok(n)
}

pub(super) struct DmlResult {
    pub table: String,
    pub rows: TableRows,
    pub delta: Bag,
    pub work: DmlWork,
}

pub(super) struct DmlCompiler<'a> {
    snapshot: &'a DatabaseSnapshot,
    selector: selector_work::MeasuredSelector,
    mode: TransactionMode,
}

impl<'a> DmlCompiler<'a> {
    pub(super) fn new(snapshot: &'a DatabaseSnapshot, mode: TransactionMode) -> Result<Self> {
        Ok(Self {
            snapshot,
            selector: selector_work::MeasuredSelector::new(snapshot)?,
            mode,
        })
    }

    pub(super) async fn compile(self, statement: &Statement) -> Result<DmlResult> {
        let RowImages {
            table,
            old,
            new,
            source_plan: plan,
            source_work,
        } = match statement {
            Statement::Insert(_) => self.compile_insert(statement).await?,
            Statement::Delete(_) => self.compile_delete(statement).await?,
            Statement::Update(_) => self.compile_update(statement).await?,
            _ => return Err(invalid("expected INSERT, DELETE or UPDATE")),
        };
        let before = &self
            .snapshot
            .tables
            .get(&table)
            .ok_or_else(|| invalid("unbound DML target"))?
            .rows;
        let delta = Bag::from_parts(&new, &old)?;
        let update = before.apply(&delta)?;
        let execution = DmlExecutionWork {
            selector: source_work,
            old_image_rows: old.num_rows(),
            new_image_rows: new.num_rows(),
            target: update.work.into(),
        };
        let mut input_rows = 0usize;
        for name in sql::reads(&plan)? {
            let rows = self
                .snapshot
                .tables
                .get(&name)
                .map(|t| t.rows.num_rows())
                .or_else(|| self.snapshot.views.get(&name).map(|v| v.output.num_rows()))
                .unwrap_or(0);
            input_rows = input_rows.checked_add(rows).ok_or(crate::Error::Overflow)?;
        }
        let affected_rows = if matches!(statement, Statement::Insert(_)) {
            new.num_rows()
        } else {
            old.num_rows()
        };
        Ok(DmlResult {
            table,
            rows: update.rows,
            work: DmlWork {
                affected_rows,
                source_relation_rows: input_rows,
                net_changed_rows: delta.len(),
                execution: Some(execution),
            },
            delta,
        })
    }

    async fn compile_insert(&self, statement: &Statement) -> Result<RowImages> {
        let Statement::Insert(insert) = statement else {
            return Err(invalid("expected INSERT statement"));
        };
        let snapshot = self.snapshot;
        let ctx = &self.selector.context;
        let mode = self.mode;
        let TableObject::TableName(target) = &insert.table else {
            return Err(invalid("INSERT requires a named base table"));
        };
        let table = sql::name(target)?;
        let source = insert
            .source
            .as_ref()
            .ok_or_else(|| invalid("INSERT requires VALUES or SELECT"))?;
        let columns = if insert.columns.is_empty() {
            String::new()
        } else {
            format!(
                " ({})",
                insert
                    .columns
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let simple = format!(
            "INSERT {}{}{} {}",
            if insert.into { "INTO " } else { "" },
            target,
            columns,
            source
        );
        if statement.to_string() != simple {
            return Err(invalid("INSERT modifiers are not supported"));
        }
        let original = snapshot
            .tables
            .get(&table)
            .ok_or_else(|| invalid("INSERT target must be a base table"))?;
        let plan = ctx
            .state()
            .create_logical_plan(&statement.to_string())
            .await?;
        let LogicalPlan::Dml(dml) = plan else {
            return Err(invalid("expected a typed INSERT plan"));
        };
        let input = sql::prepare_query(dml.input.as_ref().clone())?;
        sql::reject_mv_reads(snapshot, &input, mode)?;
        let selected = self.selector.collect(input.clone()).await?;
        let new = sql::typed(&selected.batch, original.rows.schema())?;
        Ok(RowImages {
            table,
            old: RecordBatch::new_empty(original.rows.schema()),
            new,
            source_plan: input,
            source_work: selected.work,
        })
    }
    async fn compile_delete(&self, statement: &Statement) -> Result<RowImages> {
        let Statement::Delete(delete) = statement else {
            return Err(invalid("expected DELETE statement"));
        };
        let snapshot = self.snapshot;
        let ctx = &self.selector.context;
        let mode = self.mode;
        let from = match &delete.from {
            FromTable::WithFromKeyword(f) | FromTable::WithoutKeyword(f) => f,
        };
        if from.len() != 1 {
            return Err(invalid("DELETE requires exactly one target"));
        }
        let table = target(&from[0])?;
        let filter = delete
            .selection
            .as_ref()
            .map(|e| format!(" WHERE {e}"))
            .unwrap_or_default();
        if statement.to_string() != format!("DELETE FROM {}{filter}", from[0]) {
            return Err(invalid("DELETE modifiers are not supported"));
        }
        let original = snapshot
            .tables
            .get(&table)
            .ok_or_else(|| invalid("DELETE target must be a base table"))?;
        let plan = sql::plan_query(
            ctx,
            &format!("SELECT * FROM {}{filter}", sql::quote(&table)),
        )
        .await?;
        sql::reject_mv_reads(snapshot, &plan, mode)?;
        let selected = self.selector.collect(plan.clone()).await?;
        let old = sql::typed(&selected.batch, original.rows.schema())?;
        let new = RecordBatch::new_empty(original.rows.schema());
        Ok(RowImages {
            table,
            old,
            new,
            source_plan: plan,
            source_work: selected.work,
        })
    }
    async fn compile_update(&self, statement: &Statement) -> Result<RowImages> {
        let Statement::Update(update) = statement else {
            return Err(invalid("expected UPDATE statement"));
        };
        let snapshot = self.snapshot;
        let ctx = &self.selector.context;
        let mode = self.mode;
        let table = target(&update.table)?;
        let original = snapshot
            .tables
            .get(&table)
            .ok_or_else(|| invalid("UPDATE target must be a base table"))?;
        let filter = update
            .selection
            .as_ref()
            .map(|e| format!(" WHERE {e}"))
            .unwrap_or_default();
        let simple = format!(
            "UPDATE {} SET {}{filter}",
            update.table,
            update
                .assignments
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
        if statement.to_string() != simple {
            return Err(invalid("UPDATE modifiers are not supported"));
        }
        let mut assignments = BTreeMap::new();
        for assignment in &update.assignments {
            let AssignmentTarget::ColumnName(column) = &assignment.target else {
                return Err(invalid("tuple assignment is not supported"));
            };
            let column = sql::name(column)?;
            original.rows.schema().index_of(&column)?;
            if assignments
                .insert(column, assignment.value.to_string())
                .is_some()
            {
                return Err(invalid("duplicate UPDATE assignment"));
            }
        }
        if assignments.is_empty() {
            return Err(invalid("UPDATE has no assignments"));
        }
        let width = original.rows.num_columns();
        let mut expressions = Vec::new();
        for (i, f) in original.rows.schema().fields().iter().enumerate() {
            expressions.push(format!("{} AS __optd_old_{i}", sql::quote(f.name())));
        }
        for (i, f) in original.rows.schema().fields().iter().enumerate() {
            let e = assignments
                .get(f.name())
                .cloned()
                .unwrap_or_else(|| sql::quote(f.name()));
            expressions.push(format!("({e}) AS __optd_new_{i}"));
        }
        let plan = sql::plan_query(
            ctx,
            &format!(
                "SELECT {} FROM {}{filter}",
                expressions.join(", "),
                sql::quote(&table)
            ),
        )
        .await?;
        sql::reject_mv_reads(snapshot, &plan, mode)?;
        let selected = self.selector.collect(plan.clone()).await?;
        let images = selected.batch;
        let old = sql::typed(
            &images.project(&(0..width).collect::<Vec<_>>())?,
            original.rows.schema(),
        )?;
        let new = sql::typed(
            &images.project(&(width..2 * width).collect::<Vec<_>>())?,
            original.rows.schema(),
        )?;
        Ok(RowImages {
            table,
            old,
            new,
            source_plan: plan,
            source_work: selected.work,
        })
    }
}

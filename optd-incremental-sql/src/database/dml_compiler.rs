//! Compile one DML statement into old/new typed row images from one snapshot.

use super::response::DmlWork;
use super::sql::{self, RelationScope};
use super::store::DatabaseSnapshot;
use crate::bag::Bag;
use crate::error::invalid;
use crate::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
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
    pub batch: RecordBatch,
    pub delta: Bag,
    pub work: DmlWork,
}

pub(super) struct DmlCompiler<'a> {
    snapshot: &'a DatabaseSnapshot,
    context: SessionContext,
    mode: TransactionMode,
}

impl<'a> DmlCompiler<'a> {
    pub(super) fn new(snapshot: &'a DatabaseSnapshot, mode: TransactionMode) -> Result<Self> {
        Ok(Self {
            snapshot,
            context: sql::context(snapshot, RelationScope::AllRelations)?,
            mode,
        })
    }

    pub(super) async fn compile(&self, statement: &Statement) -> Result<DmlResult> {
        let RowImages {
            table,
            old,
            new,
            source_plan: plan,
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
            .batch;
        let delta = Bag::from_parts(&new, &old)?;
        let next = Bag::from_batch(before)?.add(&delta)?.to_batch()?;
        let mut input_rows = 0usize;
        for name in sql::reads(&plan)? {
            let rows = self
                .snapshot
                .tables
                .get(&name)
                .map(|t| t.batch.num_rows())
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
            batch: next,
            work: DmlWork {
                affected_rows,
                source_relation_rows: input_rows,
                net_changed_rows: delta.len(),
            },
            delta,
        })
    }

    async fn compile_insert(&self, statement: &Statement) -> Result<RowImages> {
        let Statement::Insert(insert) = statement else {
            return Err(invalid("expected INSERT statement"));
        };
        let snapshot = self.snapshot;
        let ctx = &self.context;
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
        let input = dml.input.as_ref().clone();
        sql::immutable(&input)?;
        sql::reject_mv_reads(snapshot, &input, mode)?;
        let new = sql::typed(
            &sql::collect(ctx, input.clone()).await?,
            original.batch.schema(),
        )?;
        Ok(RowImages {
            table,
            old: RecordBatch::new_empty(original.batch.schema()),
            new,
            source_plan: input,
        })
    }
    async fn compile_delete(&self, statement: &Statement) -> Result<RowImages> {
        let Statement::Delete(delete) = statement else {
            return Err(invalid("expected DELETE statement"));
        };
        let snapshot = self.snapshot;
        let ctx = &self.context;
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
        let old = sql::typed(
            &sql::collect(ctx, plan.clone()).await?,
            original.batch.schema(),
        )?;
        let new = RecordBatch::new_empty(original.batch.schema());
        Ok(RowImages {
            table,
            old,
            new,
            source_plan: plan,
        })
    }
    async fn compile_update(&self, statement: &Statement) -> Result<RowImages> {
        let Statement::Update(update) = statement else {
            return Err(invalid("expected UPDATE statement"));
        };
        let snapshot = self.snapshot;
        let ctx = &self.context;
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
            original.batch.schema().index_of(&column)?;
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
        let width = original.batch.num_columns();
        let mut expressions = Vec::new();
        for (i, f) in original.batch.schema().fields().iter().enumerate() {
            expressions.push(format!("{} AS __optd_old_{i}", sql::quote(f.name())));
        }
        for (i, f) in original.batch.schema().fields().iter().enumerate() {
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
        let images = sql::collect(ctx, plan.clone()).await?;
        let old = sql::typed(
            &images.project(&(0..width).collect::<Vec<_>>())?,
            original.batch.schema(),
        )?;
        let new = sql::typed(
            &images.project(&(width..2 * width).collect::<Vec<_>>())?,
            original.batch.schema(),
        )?;
        Ok(RowImages {
            table,
            old,
            new,
            source_plan: plan,
        })
    }
}

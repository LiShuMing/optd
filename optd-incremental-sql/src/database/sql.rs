//! SQL binding and row-image extraction; no incremental maintenance algorithms.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::{cast, concat_batches};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::TableReference;
use datafusion::logical_expr::{DdlStatement, Expr, LogicalPlan, Volatility};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, FromTable, ObjectName, Statement, TableFactor, TableObject, TableWithJoins,
};

use super::store::DatabaseSnapshot;
use super::{invalid, DmlWork};
use crate::bag::Bag;
use crate::{Result, TableChange};

pub(super) fn statements(sql: &str) -> Result<Vec<Statement>> {
    if sql.len() > 16 * 1024 * 1024 {
        return Err(invalid("SQL exceeds 16MiB budget"));
    }
    let parsed = DFParser::parse_sql(sql).map_err(|e| invalid(e.to_string()))?;
    if parsed.is_empty() || parsed.len() > 4096 {
        return Err(invalid("expected 1..4096 SQL statements"));
    }
    parsed
        .into_iter()
        .map(|s| match s {
            DFStatement::Statement(s) => Ok(*s),
            _ => Err(invalid("unsupported SQL extension")),
        })
        .collect()
}

pub(super) fn validate_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        || name.as_bytes()[0].is_ascii_digit()
    {
        return Err(invalid(
            "relation names must be unqualified lowercase ASCII identifiers",
        ));
    }
    Ok(())
}

pub(super) fn name(name: &ObjectName) -> Result<String> {
    if name.0.len() != 1 {
        return Err(invalid(
            "qualified relation/assignment names are not supported",
        ));
    }
    let ident = name.0[0]
        .as_ident()
        .ok_or_else(|| invalid("expected identifier"))?;
    let value = if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    };
    validate_name(&value)?;
    Ok(value)
}

pub(super) fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(super) fn context(snapshot: &DatabaseSnapshot, include_views: bool) -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for (name, t) in &snapshot.tables {
        ctx.register_batch(TableReference::bare(name.as_str()), t.batch.clone())?;
    }
    if include_views {
        for (name, v) in &snapshot.views {
            ctx.register_batch(TableReference::bare(name.as_str()), v.output.clone())?;
        }
    }
    Ok(ctx)
}

pub(super) fn reads(plan: &LogicalPlan) -> Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    plan.apply_with_subqueries(|p| {
        if let LogicalPlan::TableScan(scan) = p {
            names.insert(scan.table_name.table().to_owned());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(names)
}

fn immutable(plan: &LogicalPlan) -> Result<()> {
    plan.apply_with_subqueries(|p| {
        for expr in p.expressions() {
            expr.apply(|e| {
                if matches!(e, Expr::ScalarFunction(f) if f.func.signature().volatility != Volatility::Immutable) {
                    return Err(datafusion::common::DataFusionError::Plan("SQL database expressions must be immutable".into()));
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

pub(super) async fn plan_query(ctx: &SessionContext, query: &str) -> Result<LogicalPlan> {
    let parsed = statements(query)?;
    if parsed.len() != 1 || !matches!(parsed[0], Statement::Query(_)) {
        return Err(invalid("expected a read-only query"));
    }
    let plan = ctx.state().create_logical_plan(query).await?;
    immutable(&plan)?;
    Ok(plan)
}

pub(super) async fn collect(ctx: &SessionContext, plan: LogicalPlan) -> Result<RecordBatch> {
    let frame = ctx.execute_logical_plan(plan).await?;
    let schema = Arc::new(frame.schema().as_arrow().clone());
    let result = concat_batches(&schema, &frame.collect().await?)?;
    Bag::from_batch(&result)?;
    Ok(result)
}

pub(super) fn typed(batch: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    if batch.num_columns() != schema.fields().len() {
        return Err(invalid("DML column count differs from target table"));
    }
    let arrays = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(a, f)| {
            let a = cast(a, f.data_type())?;
            if !f.is_nullable() && a.null_count() != 0 {
                return Err(invalid("NULL violates NOT NULL column"));
            }
            Ok(a)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, arrays)?)
}

pub(super) async fn create_table(
    ctx: &SessionContext,
    statement: &Statement,
) -> Result<(String, RecordBatch)> {
    let Statement::CreateTable(table) = statement else {
        return Err(invalid("expected CREATE TABLE"));
    };
    let table_name = name(&table.name)?;
    let simple = format!(
        "CREATE TABLE {} ({})",
        table.name,
        table
            .columns
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );
    if statement.to_string() != simple || table.columns.is_empty() {
        return Err(invalid("CREATE TABLE supports explicit columns only; options, constraints and CTAS are not admitted"));
    }
    for column in &table.columns {
        for option in &column.options {
            use datafusion::sql::sqlparser::ast::ColumnOption;
            if option.name.is_some()
                || !matches!(option.option, ColumnOption::Null | ColumnOption::NotNull)
            {
                return Err(invalid("only NULL/NOT NULL column options are supported"));
            }
        }
    }
    let plan = ctx
        .state()
        .create_logical_plan(&statement.to_string())
        .await?;
    let LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(table)) = plan else {
        return Err(invalid("expected typed memory table definition"));
    };
    Ok((
        table_name,
        RecordBatch::new_empty(Arc::new(table.input.schema().as_arrow().clone())),
    ))
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
    let n = name(target)?;
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

pub(super) async fn dml(
    snapshot: &DatabaseSnapshot,
    statement: &Statement,
    explicit: bool,
) -> Result<DmlResult> {
    let ctx = context(snapshot, true)?;
    let (table, old, new, plan) = match statement {
        Statement::Insert(insert) => {
            let TableObject::TableName(target) = &insert.table else {
                return Err(invalid("INSERT requires a named base table"));
            };
            let table = name(target)?;
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
            immutable(&input)?;
            reject_mv_reads(snapshot, &input, explicit)?;
            let new = typed(
                &collect(&ctx, input.clone()).await?,
                original.batch.schema(),
            )?;
            (
                table,
                RecordBatch::new_empty(original.batch.schema()),
                new,
                input,
            )
        }
        Statement::Delete(delete) => {
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
            let plan =
                plan_query(&ctx, &format!("SELECT * FROM {}{filter}", quote(&table))).await?;
            reject_mv_reads(snapshot, &plan, explicit)?;
            let old = typed(&collect(&ctx, plan.clone()).await?, original.batch.schema())?;
            let new = RecordBatch::new_empty(original.batch.schema());
            (table, old, new, plan)
        }
        Statement::Update(update) => {
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
                let column = name(column)?;
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
                expressions.push(format!("{} AS __optd_old_{i}", quote(f.name())));
            }
            for (i, f) in original.batch.schema().fields().iter().enumerate() {
                let e = assignments
                    .get(f.name())
                    .cloned()
                    .unwrap_or_else(|| quote(f.name()));
                expressions.push(format!("({e}) AS __optd_new_{i}"));
            }
            let plan = plan_query(
                &ctx,
                &format!(
                    "SELECT {} FROM {}{filter}",
                    expressions.join(", "),
                    quote(&table)
                ),
            )
            .await?;
            reject_mv_reads(snapshot, &plan, explicit)?;
            let images = collect(&ctx, plan.clone()).await?;
            let old = typed(
                &images.project(&(0..width).collect::<Vec<_>>())?,
                original.batch.schema(),
            )?;
            let new = typed(
                &images.project(&(width..2 * width).collect::<Vec<_>>())?,
                original.batch.schema(),
            )?;
            (table, old, new, plan)
        }
        _ => return Err(invalid("expected INSERT, DELETE or UPDATE")),
    };
    let before = &snapshot
        .tables
        .get(&table)
        .ok_or_else(|| invalid("unbound DML target"))?
        .batch;
    let delta = Bag::from_parts(&new, &old)?;
    let next = Bag::from_batch(before)?.add(&delta)?.to_batch()?;
    let mut input_rows = 0usize;
    for name in reads(&plan)? {
        let rows = snapshot
            .tables
            .get(&name)
            .map(|t| t.batch.num_rows())
            .or_else(|| snapshot.views.get(&name).map(|v| v.output.num_rows()))
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

pub(super) fn reject_mv_reads(
    snapshot: &DatabaseSnapshot,
    plan: &LogicalPlan,
    explicit: bool,
) -> Result<()> {
    if explicit && reads(plan)?.iter().any(|n| snapshot.views.contains_key(n)) {
        return Err(invalid(
            "materialized-view reads inside explicit transactions are not yet supported",
        ));
    }
    Ok(())
}

pub(super) fn changes(name: &str, delta: &Bag) -> Result<Vec<TableChange>> {
    let (positive, negative) = delta.signed_parts()?;
    Ok(vec![
        TableChange {
            table: name.into(),
            weights: vec![1; positive.num_rows()],
            rows: positive,
        },
        TableChange {
            table: name.into(),
            weights: vec![-1; negative.num_rows()],
            rows: negative,
        },
    ])
}

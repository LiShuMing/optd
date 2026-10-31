//! SQL binding and row-image extraction; no incremental maintenance algorithms.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::{cast_with_options, concat_batches, CastOptions};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::TableReference;
use datafusion::logical_expr::{DdlStatement, Expr, LogicalPlan, Volatility};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{ObjectName, Statement};

use super::dml_compiler::TransactionMode;
use super::store::DatabaseSnapshot;
use super::table_provider::{RowTable, TableReadCounter};
use crate::bag::Bag;
use crate::error::invalid;
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

/// Relations visible to a SQL planning context, independent of transaction state.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RelationScope {
    BaseTables,
    AllRelations,
}

pub(super) fn context(snapshot: &DatabaseSnapshot, scope: RelationScope) -> Result<SessionContext> {
    Ok(measured_context(snapshot, scope)?.context)
}

/// Metadata-bound relations and a counter for only their actually demanded table chunks.
pub(super) struct TableContext {
    pub(super) context: SessionContext,
    pub(super) table_reads: Arc<TableReadCounter>,
}

pub(super) fn measured_context(
    snapshot: &DatabaseSnapshot,
    scope: RelationScope,
) -> Result<TableContext> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let table_reads = Arc::new(TableReadCounter::default());
    for (name, t) in &snapshot.tables {
        ctx.register_table(
            TableReference::bare(name.as_str()),
            Arc::new(RowTable::new(t.rows.clone(), Arc::clone(&table_reads))),
        )?;
    }
    if scope == RelationScope::AllRelations {
        for (name, v) in &snapshot.views {
            ctx.register_batch(TableReference::bare(name.as_str()), v.output.clone())?;
        }
    }
    Ok(TableContext {
        context: ctx,
        table_reads,
    })
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

pub(super) fn immutable(plan: &LogicalPlan) -> Result<()> {
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
            // SQL assignment casts must fail on invalid input or overflow. Arrow's
            // default safe cast silently substitutes NULL even for nullable targets.
            let a = cast_with_options(
                a,
                f.data_type(),
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )?;
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

pub(super) fn reject_mv_reads(
    snapshot: &DatabaseSnapshot,
    plan: &LogicalPlan,
    mode: TransactionMode,
) -> Result<()> {
    if mode == TransactionMode::Explicit
        && reads(plan)?.iter().any(|n| snapshot.views.contains_key(n))
    {
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

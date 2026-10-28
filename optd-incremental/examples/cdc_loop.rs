//! Local end-to-end CDC loop; generated warehouses stay outside the repository.
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use optd_incremental::cdc::{Generation, OutputField, OutputSchema, PayloadType, SourceBinding};
use optd_incremental::consumer::LocalConsumer;
use optd_incremental::iceberg_sink::{write_batch_files, CdcLogReader, CdcLoop, IcebergCdcSink};
use optd_incremental::local_catalog::LocalCatalog;
use optd_incremental::{filter, scan, Expr, Value};

fn ident(name: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new("demo".into()), name.into())
}
fn plan() -> optd_incremental::IncPlanNode {
    filter(
        scan(0),
        Expr::Gt(Box::new(Expr::Col(0)), Box::new(Expr::Lit(Value::Int(0)))),
    )
}
async fn definition(catalog: &LocalCatalog) -> Result<Generation, Box<dyn Error>> {
    let source = catalog.load_table(&ident("source")).await?;
    Ok(Generation::new(
        "00000000-0000-0000-0000-000000000001".into(),
        "00000000-0000-0000-0000-000000000002".into(),
        OutputSchema::new(vec![OutputField {
            name: "id".into(),
            data_type: PayloadType::Long,
            required: true,
        }])?,
        &plan(),
        BTreeMap::from([(
            0,
            SourceBinding {
                table_id: source.metadata().uuid().to_string(),
                reference: "main".into(),
            },
        )]),
    )?)
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let usage =
        "usage: cdc_loop <warehouse> init|append <integers...>|refresh|watch [milliseconds]|dump|peek <consumer>|consume <consumer>|compact|expire";
    let root = args.first().ok_or(usage)?;
    let command = args.get(1).ok_or(usage)?;
    let catalog = Arc::new(LocalCatalog::open(root)?);
    match command.as_str() {
        "init" => {
            catalog
                .create_namespace(&NamespaceIdent::new("demo".into()), Default::default())
                .await?;
            let schema = Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()?;
            catalog
                .create_table(
                    ident("source").namespace(),
                    TableCreation::builder()
                        .name("source".into())
                        .schema(schema)
                        .format_version(FormatVersion::V2)
                        .properties([("commit.retry.num-retries".into(), "0".into())])
                        .build(),
                )
                .await?;
            println!("Created source. The view keeps id > 0; duplicate rows retain multiplicity.");
        }
        "append" => {
            let values = args[2..]
                .iter()
                .map(|v| v.parse::<i64>())
                .collect::<Result<Vec<_>, _>>()?;
            if values.is_empty() {
                return Err(usage.into());
            }
            let table = catalog.load_table(&ident("source")).await?;
            let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
            let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])?;
            let files = write_batch_files(&table, batch).await?;
            let tx = Transaction::new(&table);
            tx.fast_append()
                .add_data_files(files)
                .apply(tx)?
                .commit(catalog.as_ref())
                .await?;
            println!("Source snapshot committed.");
        }
        "refresh" | "watch" => {
            let generation = definition(catalog.as_ref()).await?;
            let mut driver = CdcLoop::recover(
                catalog,
                ident("source"),
                ident("changes"),
                generation,
                plan(),
                format!("pid-{}", std::process::id()),
            )
            .await?;
            let milliseconds = args
                .get(2)
                .map(|s| s.parse::<u64>())
                .transpose()?
                .unwrap_or(1000);
            if milliseconds == 0 {
                return Err("poll interval must be positive".into());
            }
            loop {
                match driver.step().await? {
                    Some(committed) => println!("{}", serde_json::to_string(&committed)?),
                    None if command == "refresh" => println!("No new source snapshot."),
                    None => {}
                }
                if command == "refresh" {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(milliseconds)).await;
            }
        }
        "dump" => {
            let generation = definition(catalog.as_ref()).await?;
            let sink = CdcLogReader::open(catalog, ident("changes"), generation).await?;
            for batch in sink.read_batches().await? {
                println!("{}", serde_json::to_string(batch.envelope())?);
                for r in batch.rows() {
                    let id = match r.payload.first() {
                        Some(Value::Int(n)) => serde_json::json!(n),
                        _ => serde_json::Value::Null,
                    };
                    println!(
                        "{}",
                        serde_json::json!({"id":id,"__meta_op_type":r.op_type,"__meta_is_update":r.is_update,
                        "__meta_weight":r.weight,"__meta_row_id":r.row_id,"__meta_batch_id":batch.envelope().batch_id,
                        "__meta_batch_seq":batch.envelope().batch_seq,"__meta_ordinal":r.ordinal,"__meta_timestamp":batch.envelope().prepared_at_us})
                    );
                }
            }
        }
        "peek" | "consume" => {
            let name = args.get(2).ok_or(usage)?.clone();
            let generation = definition(catalog.as_ref()).await?;
            let reader = CdcLogReader::open(catalog, ident("changes"), generation).await?;
            let mut consumer = LocalConsumer::open(reader, name).await?;
            let pinned = consumer.peek().await?;
            if command == "consume" {
                consumer.consume(&pinned)?;
            }
            println!(
                "{}",
                serde_json::json!({"from":pinned.from(),"upper":pinned.upper(),
                "batches":pinned.batches().len(),"cursor":consumer.cursor(),
                "destination":consumer.contents().iter().collect::<Vec<_>>() })
            );
        }
        "compact" | "expire" => {
            let generation = definition(catalog.as_ref()).await?;
            let mut sink = IcebergCdcSink::open(catalog, ident("changes"), generation).await?;
            if command == "compact" {
                sink.compact().await?;
                println!("Physical files compacted; logical CDC preserved.");
            } else {
                println!(
                    "Expired {} old snapshots; CDC rows and files retained.",
                    sink.expire_snapshots().await?
                );
            }
        }
        _ => return Err(usage.into()),
    }
    Ok(())
}

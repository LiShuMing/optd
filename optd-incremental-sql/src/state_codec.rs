//! Columnar Arrow-IPC + JSON persistence codec for incremental operator states (R07b).
//!
//! All typed SQL values (including float bit patterns, NULLs and decimals) stay in
//! SHA-256-verified Arrow IPC files; JSON carries only compact structural metadata.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, RecordBatchOptions, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use serde::{Deserialize, Serialize};

use crate::bag::Bag;
use crate::error::invalid;
use crate::row_blocks::RowBlocks;
use crate::store::{json_bytes, read_batch, verified_file, write_batch, write_bytes, FileEntry};
use crate::{Error, Result};

/// Persisted operator-state format identifier; incompatible versions fail closed.
pub(crate) const STATE_CODEC_FORMAT: &str = "optd-operator-state/1";

/// In-memory snapshot of one `SqlView`'s complete derived execution state.
pub(crate) struct ViewStateSnapshot {
    pub(crate) view_epoch: u64,
    pub(crate) plan_fingerprint: String,
    pub(crate) node_relations: Vec<Option<RowBlocks>>,
    pub(crate) operators: Vec<Option<OperatorStateSnapshot>>,
}

/// Per-node derived operator state snapshot.
pub(crate) enum OperatorStateSnapshot {
    Accumulator(AccumulatorSnapshot),
    Keyed(KeyedSnapshot),
    Group(GroupSnapshot),
    Rank(RankSnapshot),
}

/// Persisted `AccumulatorState` data across all groups and aggregate functions.
pub(crate) struct AccumulatorSnapshot {
    pub(crate) group_keys_hex: Vec<String>,
    pub(crate) group_rows: Vec<i64>,
    pub(crate) keys_batch: Option<RecordBatch>,
    pub(crate) outputs_batch: RecordBatch,
    pub(crate) functions: Vec<FunctionCellsSnapshot>,
}

/// Per-aggregate-function columnar state across all groups of an `AccumulatorState`.
pub(crate) enum FunctionCellsSnapshot {
    Native {
        states_batch: Option<RecordBatch>,
    },
    ExactIntegerAverage {
        states_batch: Option<RecordBatch>,
        masses: Vec<u64>,
    },
    Moments {
        entries: Vec<MomentMeta>,
        values_batch: Option<RecordBatch>,
    },
    Values {
        values_batch: Option<RecordBatch>,
    },
    Distinct {
        values_batch: Option<RecordBatch>,
        states_batch: Option<RecordBatch>,
    },
}

/// Bit-exact centered Welford metadata for one group's moment cell.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MomentMeta {
    pub(crate) count: u64,
    pub(crate) origin_bits: u64,
    pub(crate) mean_bits: u64,
    pub(crate) m2_bits: u64,
}

/// Persisted `KeyedState` input buckets, optional residual match counts and window outputs.
pub(crate) struct KeyedSnapshot {
    pub(crate) ports: Vec<KeyedPortSnapshot>,
    pub(crate) residual: Option<[ResidualSideSnapshot; 2]>,
    pub(crate) outputs: Option<IndexedBagsSnapshot>,
}

/// One input port of `KeyedState`, preserving exact bucket row order and NULL-key flag.
pub(crate) struct KeyedPortSnapshot {
    pub(crate) buckets: Vec<KeyedBucketMeta>,
    pub(crate) rows_batch: RecordBatch,
}

/// Metadata for one bucket slice inside `KeyedPortSnapshot::rows_batch`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KeyedBucketMeta {
    pub(crate) key_hex: String,
    pub(crate) row_start: usize,
    pub(crate) row_len: usize,
    pub(crate) key_has_null: bool,
}

/// Persisted `ResidualState` per-row match counts for one join side.
pub(crate) struct ResidualSideSnapshot {
    pub(crate) keys_hex: Vec<String>,
    pub(crate) counts_batch: RecordBatch,
}

/// Generic indexed weighted-bag collection (`key_idx: UInt64`, `cols...`, `__optd_weight: Int64`).
pub(crate) struct IndexedBagsSnapshot {
    pub(crate) keys_hex: Vec<String>,
    pub(crate) rows_batch: RecordBatch,
}

/// Persisted `GroupState` buckets and exact key schema.
pub(crate) struct GroupSnapshot {
    pub(crate) key_schema: SchemaRef,
    pub(crate) keys_hex: Vec<String>,
    pub(crate) inputs_batch: RecordBatch,
    pub(crate) outputs_batch: RecordBatch,
}

/// Persisted `RankState` ordered peers and weighted rows.
pub(crate) struct RankSnapshot {
    pub(crate) peers: Vec<RankPeerMeta>,
    pub(crate) rows_batch: RecordBatch,
}

/// Metadata for one ordered peer inside `RankState`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RankPeerMeta {
    pub(crate) key_hex: String,
    pub(crate) count: u64,
    pub(crate) rank: u64,
}

// ---------------------------------------------------------------------------
// On-disk manifest structures for `states/view-<id>/state-manifest.json`
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskOperatorManifest {
    format: String,
    view_epoch: u64,
    plan_fingerprint: String,
    node_count: usize,
    operators: Vec<Option<DiskOperatorEntry>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum DiskOperatorEntry {
    Accumulator {
        group_keys_hex: Vec<String>,
        group_rows: Vec<i64>,
        keys_file: Option<FileEntry>,
        outputs_file: FileEntry,
        functions: Vec<DiskFunctionCells>,
    },
    Keyed {
        ports: Vec<DiskKeyedPort>,
        residual: Option<[DiskResidualSide; 2]>,
        outputs: Option<DiskIndexedBags>,
    },
    Group {
        key_schema_file: FileEntry,
        keys_hex: Vec<String>,
        inputs_file: FileEntry,
        outputs_file: FileEntry,
    },
    Rank {
        peers: Vec<RankPeerMeta>,
        rows_file: FileEntry,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum DiskFunctionCells {
    Native {
        states_file: Option<FileEntry>,
    },
    ExactIntegerAverage {
        states_file: Option<FileEntry>,
        masses: Vec<u64>,
    },
    Moments {
        entries: Vec<MomentMeta>,
        values_file: Option<FileEntry>,
    },
    Values {
        values_file: Option<FileEntry>,
    },
    Distinct {
        values_file: Option<FileEntry>,
        states_file: Option<FileEntry>,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskKeyedPort {
    buckets: Vec<KeyedBucketMeta>,
    rows_file: FileEntry,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskResidualSide {
    keys_hex: Vec<String>,
    counts_file: FileEntry,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskIndexedBags {
    keys_hex: Vec<String>,
    rows_file: FileEntry,
}

/// Statistics returned when reading persisted operator-state files from disk.
#[derive(Default)]
pub(crate) struct StateReadStats {
    pub(crate) files: u64,
    pub(crate) bytes: u64,
    pub(crate) rows: u64,
}

impl StateReadStats {
    fn record(&mut self, bytes: u64, rows: usize) -> Result<()> {
        self.files = self.files.checked_add(1).ok_or(Error::Overflow)?;
        self.bytes = self.bytes.checked_add(bytes).ok_or(Error::Overflow)?;
        self.rows = self.rows.checked_add(rows as u64).ok_or(Error::Overflow)?;
        Ok(())
    }
}

/// Persist operator states (everything except `node_relations`, which the database store
/// writes via its deduplicated block store) into `dir` and return the hashed manifest entry.
pub(crate) fn write_operator_states(
    dir: &Path,
    view_epoch: u64,
    plan_fingerprint: &str,
    operators: &[Option<OperatorStateSnapshot>],
) -> Result<FileEntry> {
    std::fs::create_dir_all(dir)?;
    let mut disk_ops = Vec::with_capacity(operators.len());
    for (node_id, op) in operators.iter().enumerate() {
        let entry = match op {
            None => None,
            Some(OperatorStateSnapshot::Accumulator(acc)) => {
                let keys_file = acc
                    .keys_batch
                    .as_ref()
                    .map(|b| write_batch(dir, &format!("n{node_id}-acc-keys.arrow"), b))
                    .transpose()?;
                let outputs_file = write_batch(
                    dir,
                    &format!("n{node_id}-acc-outputs.arrow"),
                    &acc.outputs_batch,
                )?;
                let mut functions = Vec::with_capacity(acc.functions.len());
                for (f_idx, func) in acc.functions.iter().enumerate() {
                    let df = match func {
                        FunctionCellsSnapshot::Native { states_batch } => {
                            DiskFunctionCells::Native {
                                states_file: states_batch
                                    .as_ref()
                                    .map(|b| {
                                        write_batch(
                                            dir,
                                            &format!("n{node_id}-acc-f{f_idx}-native.arrow"),
                                            b,
                                        )
                                    })
                                    .transpose()?,
                            }
                        }
                        FunctionCellsSnapshot::ExactIntegerAverage {
                            states_batch,
                            masses,
                        } => DiskFunctionCells::ExactIntegerAverage {
                            states_file: states_batch
                                .as_ref()
                                .map(|b| {
                                    write_batch(
                                        dir,
                                        &format!("n{node_id}-acc-f{f_idx}-avg.arrow"),
                                        b,
                                    )
                                })
                                .transpose()?,
                            masses: masses.clone(),
                        },
                        FunctionCellsSnapshot::Moments {
                            entries,
                            values_batch,
                        } => DiskFunctionCells::Moments {
                            entries: entries.clone(),
                            values_file: values_batch
                                .as_ref()
                                .map(|b| {
                                    write_batch(
                                        dir,
                                        &format!("n{node_id}-acc-f{f_idx}-moments.arrow"),
                                        b,
                                    )
                                })
                                .transpose()?,
                        },
                        FunctionCellsSnapshot::Values { values_batch } => {
                            DiskFunctionCells::Values {
                                values_file: values_batch
                                    .as_ref()
                                    .map(|b| {
                                        write_batch(
                                            dir,
                                            &format!("n{node_id}-acc-f{f_idx}-values.arrow"),
                                            b,
                                        )
                                    })
                                    .transpose()?,
                            }
                        }
                        FunctionCellsSnapshot::Distinct {
                            values_batch,
                            states_batch,
                        } => DiskFunctionCells::Distinct {
                            values_file: values_batch
                                .as_ref()
                                .map(|b| {
                                    write_batch(
                                        dir,
                                        &format!("n{node_id}-acc-f{f_idx}-dvals.arrow"),
                                        b,
                                    )
                                })
                                .transpose()?,
                            states_file: states_batch
                                .as_ref()
                                .map(|b| {
                                    write_batch(
                                        dir,
                                        &format!("n{node_id}-acc-f{f_idx}-dstates.arrow"),
                                        b,
                                    )
                                })
                                .transpose()?,
                        },
                    };
                    functions.push(df);
                }
                Some(DiskOperatorEntry::Accumulator {
                    group_keys_hex: acc.group_keys_hex.clone(),
                    group_rows: acc.group_rows.clone(),
                    keys_file,
                    outputs_file,
                    functions,
                })
            }
            Some(OperatorStateSnapshot::Keyed(keyed)) => {
                let mut ports = Vec::with_capacity(keyed.ports.len());
                for (p_idx, port) in keyed.ports.iter().enumerate() {
                    let rows_file = write_batch(
                        dir,
                        &format!("n{node_id}-keyed-p{p_idx}.arrow"),
                        &port.rows_batch,
                    )?;
                    ports.push(DiskKeyedPort {
                        buckets: port.buckets.clone(),
                        rows_file,
                    });
                }
                let residual = match &keyed.residual {
                    None => None,
                    Some([left, right]) => Some([
                        DiskResidualSide {
                            keys_hex: left.keys_hex.clone(),
                            counts_file: write_batch(
                                dir,
                                &format!("n{node_id}-keyed-res0.arrow"),
                                &left.counts_batch,
                            )?,
                        },
                        DiskResidualSide {
                            keys_hex: right.keys_hex.clone(),
                            counts_file: write_batch(
                                dir,
                                &format!("n{node_id}-keyed-res1.arrow"),
                                &right.counts_batch,
                            )?,
                        },
                    ]),
                };
                let outputs = keyed
                    .outputs
                    .as_ref()
                    .map(|out| -> Result<DiskIndexedBags> {
                        Ok(DiskIndexedBags {
                            keys_hex: out.keys_hex.clone(),
                            rows_file: write_batch(
                                dir,
                                &format!("n{node_id}-keyed-out.arrow"),
                                &out.rows_batch,
                            )?,
                        })
                    })
                    .transpose()?;
                Some(DiskOperatorEntry::Keyed {
                    ports,
                    residual,
                    outputs,
                })
            }
            Some(OperatorStateSnapshot::Group(group)) => {
                let key_schema_file = write_batch(
                    dir,
                    &format!("n{node_id}-group-key-schema.arrow"),
                    &RecordBatch::new_empty(Arc::clone(&group.key_schema)),
                )?;
                let inputs_file = write_batch(
                    dir,
                    &format!("n{node_id}-group-in.arrow"),
                    &group.inputs_batch,
                )?;
                let outputs_file = write_batch(
                    dir,
                    &format!("n{node_id}-group-out.arrow"),
                    &group.outputs_batch,
                )?;
                Some(DiskOperatorEntry::Group {
                    key_schema_file,
                    keys_hex: group.keys_hex.clone(),
                    inputs_file,
                    outputs_file,
                })
            }
            Some(OperatorStateSnapshot::Rank(rank)) => {
                let rows_file = write_batch(
                    dir,
                    &format!("n{node_id}-rank-rows.arrow"),
                    &rank.rows_batch,
                )?;
                Some(DiskOperatorEntry::Rank {
                    peers: rank.peers.clone(),
                    rows_file,
                })
            }
        };
        disk_ops.push(entry);
    }
    let manifest = DiskOperatorManifest {
        format: STATE_CODEC_FORMAT.to_owned(),
        view_epoch,
        plan_fingerprint: plan_fingerprint.to_owned(),
        node_count: operators.len(),
        operators: disk_ops,
    };
    let entry = write_bytes(dir, "operator-state.json", &json_bytes(&manifest)?)?;
    crate::store::sync_dir(dir)?;
    Ok(entry)
}

/// Verified operator-state payload loaded from `dir`.
pub(crate) struct LoadedOperatorStates {
    pub(crate) view_epoch: u64,
    pub(crate) plan_fingerprint: String,
    pub(crate) operators: Vec<Option<OperatorStateSnapshot>>,
    pub(crate) stats: StateReadStats,
}

/// Read and SHA-256-verify persisted operator states from `dir`.
pub(crate) fn read_operator_states(
    dir: &Path,
    manifest_entry: &FileEntry,
    expected_fingerprint: &str,
) -> Result<LoadedOperatorStates> {
    let mut stats = StateReadStats::default();
    let file = verified_file(dir, manifest_entry)?;
    stats.record(manifest_entry.bytes, 0)?;
    let manifest: DiskOperatorManifest = serde_json::from_reader(file)?;
    if manifest.format != STATE_CODEC_FORMAT {
        return Err(invalid(format!(
            "unsupported operator state format {}; expected {STATE_CODEC_FORMAT}",
            manifest.format
        )));
    }
    if manifest.plan_fingerprint != expected_fingerprint {
        return Err(invalid(
            "persisted operator state fingerprint differs from compiled plan",
        ));
    }
    if manifest.operators.len() != manifest.node_count {
        return Err(invalid("operator state node count differs"));
    }
    let mut load = |entry: &FileEntry| -> Result<RecordBatch> {
        let batch = read_batch(dir, entry)?;
        stats.record(entry.bytes, batch.num_rows())?;
        Ok(batch)
    };
    let mut operators = Vec::with_capacity(manifest.operators.len());
    for entry in manifest.operators {
        let op = match entry {
            None => None,
            Some(DiskOperatorEntry::Accumulator {
                group_keys_hex,
                group_rows,
                keys_file,
                outputs_file,
                functions,
            }) => {
                let keys_batch = keys_file.as_ref().map(&mut load).transpose()?;
                let outputs_batch = load(&outputs_file)?;
                let mut loaded_fns = Vec::with_capacity(functions.len());
                for f in functions {
                    loaded_fns.push(match f {
                        DiskFunctionCells::Native { states_file } => {
                            FunctionCellsSnapshot::Native {
                                states_batch: states_file.as_ref().map(&mut load).transpose()?,
                            }
                        }
                        DiskFunctionCells::ExactIntegerAverage {
                            states_file,
                            masses,
                        } => FunctionCellsSnapshot::ExactIntegerAverage {
                            states_batch: states_file.as_ref().map(&mut load).transpose()?,
                            masses,
                        },
                        DiskFunctionCells::Moments {
                            entries,
                            values_file,
                        } => FunctionCellsSnapshot::Moments {
                            entries,
                            values_batch: values_file.as_ref().map(&mut load).transpose()?,
                        },
                        DiskFunctionCells::Values { values_file } => {
                            FunctionCellsSnapshot::Values {
                                values_batch: values_file.as_ref().map(&mut load).transpose()?,
                            }
                        }
                        DiskFunctionCells::Distinct {
                            values_file,
                            states_file,
                        } => FunctionCellsSnapshot::Distinct {
                            values_batch: values_file.as_ref().map(&mut load).transpose()?,
                            states_batch: states_file.as_ref().map(&mut load).transpose()?,
                        },
                    });
                }
                Some(OperatorStateSnapshot::Accumulator(AccumulatorSnapshot {
                    group_keys_hex,
                    group_rows,
                    keys_batch,
                    outputs_batch,
                    functions: loaded_fns,
                }))
            }
            Some(DiskOperatorEntry::Keyed {
                ports,
                residual,
                outputs,
            }) => {
                let mut loaded_ports = Vec::with_capacity(ports.len());
                for p in ports {
                    loaded_ports.push(KeyedPortSnapshot {
                        buckets: p.buckets,
                        rows_batch: load(&p.rows_file)?,
                    });
                }
                let loaded_res = match residual {
                    None => None,
                    Some([left, right]) => Some([
                        ResidualSideSnapshot {
                            keys_hex: left.keys_hex,
                            counts_batch: load(&left.counts_file)?,
                        },
                        ResidualSideSnapshot {
                            keys_hex: right.keys_hex,
                            counts_batch: load(&right.counts_file)?,
                        },
                    ]),
                };
                let loaded_out = outputs
                    .map(|out| -> Result<IndexedBagsSnapshot> {
                        Ok(IndexedBagsSnapshot {
                            keys_hex: out.keys_hex,
                            rows_batch: load(&out.rows_file)?,
                        })
                    })
                    .transpose()?;
                Some(OperatorStateSnapshot::Keyed(KeyedSnapshot {
                    ports: loaded_ports,
                    residual: loaded_res,
                    outputs: loaded_out,
                }))
            }
            Some(DiskOperatorEntry::Group {
                key_schema_file,
                keys_hex,
                inputs_file,
                outputs_file,
            }) => {
                let key_schema_batch = load(&key_schema_file)?;
                if key_schema_batch.num_rows() != 0 {
                    return Err(invalid("persisted group key schema must have zero rows"));
                }
                Some(OperatorStateSnapshot::Group(GroupSnapshot {
                    key_schema: key_schema_batch.schema(),
                    keys_hex,
                    inputs_batch: load(&inputs_file)?,
                    outputs_batch: load(&outputs_file)?,
                }))
            }
            Some(DiskOperatorEntry::Rank { peers, rows_file }) => {
                Some(OperatorStateSnapshot::Rank(RankSnapshot {
                    peers,
                    rows_batch: load(&rows_file)?,
                }))
            }
        };
        operators.push(op);
    }
    Ok(LoadedOperatorStates {
        view_epoch: manifest.view_epoch,
        plan_fingerprint: manifest.plan_fingerprint,
        operators,
        stats,
    })
}

// ---------------------------------------------------------------------------
// Shared Arrow columnar packing & hex helpers for operator state modules
// ---------------------------------------------------------------------------

pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

pub(crate) fn decode_hex(text: &str) -> Result<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return Err(invalid("odd-length hex key in persisted operator state"));
    }
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = hex_nibble(pair[0])?;
        let lo = hex_nibble(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(invalid("invalid hex digit in persisted operator state")),
    }
}

/// Pack homogeneous `Vec<Vec<ScalarValue>>` rows into a typed Arrow `RecordBatch`.
pub(crate) fn scalar_rows_to_batch(
    schema: Option<SchemaRef>,
    rows: &[Vec<ScalarValue>],
) -> Result<RecordBatch> {
    if rows.is_empty() {
        let schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
        return Ok(RecordBatch::new_empty(schema));
    }
    let width = rows[0].len();
    for row in rows {
        if row.len() != width {
            return Err(invalid("scalar row width mismatch during state export"));
        }
    }
    let mut columns = Vec::with_capacity(width);
    let mut fields = Vec::with_capacity(width);
    for col_idx in 0..width {
        let array = ScalarValue::iter_to_array(rows.iter().map(|r| r[col_idx].clone()))?;
        fields.push(Field::new(
            format!("c{col_idx}"),
            array.data_type().clone(),
            true,
        ));
        columns.push(array);
    }
    let target_schema = schema.unwrap_or_else(|| Arc::new(Schema::new(fields)));
    Ok(RecordBatch::try_new_with_options(
        target_schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )?)
}

/// Unpack a `RecordBatch` into `Vec<Vec<ScalarValue>>`.
pub(crate) fn batch_to_scalar_rows(batch: &RecordBatch) -> Result<Vec<Vec<ScalarValue>>> {
    let mut out = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let mut row = Vec::with_capacity(batch.num_columns());
        for col in batch.columns() {
            row.push(ScalarValue::try_from_array(col.as_ref(), row_idx)?);
        }
        out.push(row);
    }
    Ok(out)
}

/// Pack `(group_idx, value, count)` triples from counted-value indexes into one `RecordBatch`.
pub(crate) fn counted_values_to_batch(
    entries: &[(u64, ScalarValue, i64)],
) -> Result<Option<RecordBatch>> {
    if entries.is_empty() {
        return Ok(None);
    }
    let group_idx: ArrayRef = Arc::new(UInt64Array::from_iter_values(
        entries.iter().map(|(idx, _, _)| *idx),
    ));
    let values = ScalarValue::iter_to_array(entries.iter().map(|(_, v, _)| v.clone()))?;
    let counts: ArrayRef = Arc::new(Int64Array::from_iter_values(
        entries.iter().map(|(_, _, c)| *c),
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("__optd_group_idx", DataType::UInt64, false),
        Field::new("__optd_value", values.data_type().clone(), true),
        Field::new("__optd_count", DataType::Int64, false),
    ]));
    Ok(Some(RecordBatch::try_new(
        schema,
        vec![group_idx, values, counts],
    )?))
}

/// Unpack `(group_idx, value, count)` triples from a `RecordBatch`.
pub(crate) fn batch_to_counted_values(batch: &RecordBatch) -> Result<Vec<(u64, ScalarValue, i64)>> {
    if batch.num_columns() != 3 {
        return Err(invalid("invalid persisted counted-value batch width"));
    }
    let group_idx = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("invalid counted-value group index column"))?;
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| invalid("invalid counted-value count column"))?;
    if group_idx.null_count() != 0 || counts.null_count() != 0 {
        return Err(invalid("NULL metadata in persisted counted-value batch"));
    }
    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        out.push((
            group_idx.value(i),
            ScalarValue::try_from_array(batch.column(1).as_ref(), i)?,
            counts.value(i),
        ));
    }
    Ok(out)
}

/// Pack a sequence of `(key_idx, Bag)` items sharing `row_schema` into one `RecordBatch`:
/// `(__optd_key_idx: UInt64, row_cols..., __optd_weight: Int64)`.
pub(crate) fn indexed_bags_to_batch(
    row_schema: &SchemaRef,
    bags: &[(u64, &Bag)],
) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(row_schema.fields().len() + 2);
    fields.push(Field::new("__optd_key_idx", DataType::UInt64, false));
    fields.extend(row_schema.fields().iter().map(|f| f.as_ref().clone()));
    fields.push(Field::new("__optd_weight", DataType::Int64, false));
    let combined_schema = Arc::new(Schema::new(fields));

    let mut key_indices = Vec::new();
    let mut row_batches = Vec::new();
    let mut weights = Vec::new();
    for (idx, bag) in bags {
        if bag.is_empty() {
            continue;
        }
        let weighted = bag.weighted_batch()?;
        let width = row_schema.fields().len();
        let w = weighted
            .column(width)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("invalid weighted bag batch"))?;
        for r in 0..weighted.num_rows() {
            key_indices.push(*idx);
            weights.push(w.value(r));
        }
        row_batches.push(RecordBatch::try_new_with_options(
            Arc::clone(row_schema),
            weighted.columns()[..width].to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(weighted.num_rows())),
        )?);
    }
    if key_indices.is_empty() {
        return Ok(RecordBatch::new_empty(combined_schema));
    }
    let merged_rows = datafusion::arrow::compute::concat_batches(row_schema, &row_batches)?;
    let mut columns = Vec::with_capacity(row_schema.fields().len() + 2);
    columns.push(Arc::new(UInt64Array::from(key_indices)) as ArrayRef);
    columns.extend(merged_rows.columns().iter().cloned());
    columns.push(Arc::new(Int64Array::from(weights)) as ArrayRef);
    Ok(RecordBatch::try_new_with_options(
        combined_schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(merged_rows.num_rows())),
    )?)
}

/// Unpack `(__optd_key_idx, row_cols..., __optd_weight)` into `BTreeMap<u64, Bag>`.
pub(crate) fn batch_to_indexed_bags(
    batch: &RecordBatch,
    row_schema: &SchemaRef,
) -> Result<BTreeMap<u64, Bag>> {
    let width = row_schema.fields().len();
    if batch.num_columns() != width + 2
        || batch.schema().field(0).name() != "__optd_key_idx"
        || batch.schema().field(width + 1).name() != "__optd_weight"
    {
        return Err(invalid("invalid persisted indexed-bag batch schema"));
    }
    let key_idx = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("invalid indexed-bag key index column"))?;
    let weights = batch
        .column(width + 1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| invalid("invalid indexed-bag weight column"))?;
    if key_idx.null_count() != 0 || weights.null_count() != 0 {
        return Err(invalid("NULL metadata in persisted indexed-bag batch"));
    }
    let mut grouped = BTreeMap::<u64, Vec<(Vec<ScalarValue>, i64)>>::new();
    for r in 0..batch.num_rows() {
        let mut row = Vec::with_capacity(width);
        for c in 0..width {
            row.push(ScalarValue::try_from_array(
                batch.column(c + 1).as_ref(),
                r,
            )?);
        }
        grouped
            .entry(key_idx.value(r))
            .or_default()
            .push((row, weights.value(r)));
    }
    let mut bags = BTreeMap::new();
    for (idx, entries) in grouped {
        let bag = Bag::from_unique_entries(
            Arc::clone(row_schema),
            entries.iter().map(|(r, w)| (r.as_slice(), *w)),
        )?;
        bag.validate_relation()?;
        bags.insert(idx, bag);
    }
    Ok(bags)
}

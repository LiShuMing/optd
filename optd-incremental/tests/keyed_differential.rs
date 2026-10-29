//! Differential oracle for the keyed endpoint differ.
//!
//! `differential.rs` checks the unkeyed delta rules against recompute; the
//! keyed profile (M3) had only hand-written scenarios. This file exercises
//! `PreparedBatch::from_endpoints` as a pure function of two complete endpoint
//! relations, which is where keyed pairing actually decides what a consumer
//! sees:
//!
//! * replaying the emitted changes onto `before` must reproduce `after`;
//! * every change must match an independently built expectation, record by
//!   record (ordinal, op, weight, `is_update`, `row_id`, payload image);
//! * unchanged keys emit nothing, and a key move is a DELETE plus an INSERT
//!   with two different identities, never an `is_update` pair.
//!
//! Failures print the seed and both endpoints as JSON, so a counterexample is
//! reproducible by rerunning with that seed.

use optd_incremental::cdc::*;
use optd_incremental::{scan, Multiset, Value};
use std::collections::BTreeMap;

const VIEW: &str = "00000000-0000-0000-0000-000000000001";
const GEN: &str = "00000000-0000-0000-0000-000000000002";
const SOURCE: &str = "00000000-0000-0000-0000-000000000003";

fn generation() -> Generation {
    let schema = OutputSchema::new(vec![
        OutputField {
            name: "key".into(),
            data_type: PayloadType::Long,
            required: true,
        },
        OutputField {
            name: "value".into(),
            data_type: PayloadType::Long,
            required: false,
        },
    ])
    .unwrap()
    // Key ids are 1000 + field position.
    .keyed(KeySpec(vec![1000]))
    .unwrap();
    Generation::new(
        VIEW.into(),
        GEN.into(),
        schema,
        &scan(0),
        BTreeMap::from([(
            0,
            SourceBinding {
                table_id: SOURCE.into(),
                reference: "main".into(),
            },
        )]),
    )
    .unwrap()
}

fn point(seq: i64) -> Vec<SourceCheckpoint> {
    vec![SourceCheckpoint {
        table_id: SOURCE.into(),
        reference: "main".into(),
        snapshot_id: Some(seq * 100),
        sequence_number: Some(seq),
        schema_fingerprint: format!("sha256:{}", "a".repeat(64)),
    }]
}

/// Deterministic seeded generator; every scenario is reproducible from its seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

fn value(rng: &mut Rng) -> Value {
    // Null is legal in the payload column (not required) and must not disturb pairing.
    if rng.chance(15) {
        Value::Null
    } else {
        Value::Int(rng.below(6) as i64)
    }
}

fn relation(rng: &mut Rng, universe: i64, density: u64) -> Multiset {
    let mut out = Multiset::new();
    for key in 0..universe {
        if rng.chance(density) {
            out.add_row(vec![Value::Int(key), value(rng)], 1);
        }
    }
    out
}

/// One transition: deletes, upserts, payload changes and key moves.
fn transition(rng: &mut Rng, current: &Multiset, universe: i64) -> Multiset {
    let mut rows: BTreeMap<i64, Value> = BTreeMap::new();
    for (row, weight) in current.iter() {
        assert_eq!(*weight, 1);
        match row.as_slice() {
            [Value::Int(key), value] => {
                rows.insert(*key, value.clone());
            }
            other => panic!("unexpected row shape {other:?}"),
        }
    }
    for key in 0..universe {
        match rng.below(10) {
            0 => {
                rows.remove(&key);
            }
            1 | 2 => {
                rows.insert(key, value(rng));
            }
            3 => {
                // Key move: same payload, new identity.
                if let Some(value) = rows.remove(&key) {
                    rows.insert(key + universe, value);
                }
            }
            _ => {}
        }
    }
    let mut out = Multiset::new();
    for (key, value) in rows {
        out.add_row(vec![Value::Int(key), value], 1);
    }
    out
}

/// Independent expectation, built from the two relations only.
fn expected(generation: &Generation, before: &Multiset, after: &Multiset) -> Vec<ChangeRecord> {
    let fold = |relation: &Multiset| -> BTreeMap<i64, Value> {
        relation
            .iter()
            .map(|(row, weight)| {
                assert_eq!(*weight, 1);
                match row.as_slice() {
                    [Value::Int(key), value] => (*key, value.clone()),
                    other => panic!("unexpected row shape {other:?}"),
                }
            })
            .collect()
    };
    let old = fold(before);
    let new = fold(after);
    let keys: BTreeMap<i64, ()> = old.keys().chain(new.keys()).map(|k| (*k, ())).collect();
    let mut out = Vec::new();
    for key in keys.keys() {
        let (a, b) = (old.get(key), new.get(key));
        if a == b {
            continue;
        }
        for (value, weight) in [(a, -1), (b, 1)] {
            let Some(value) = value else { continue };
            let payload = vec![Value::Int(*key), value.clone()];
            out.push(ChangeRecord {
                ordinal: i64::try_from(out.len()).unwrap(),
                op_type: i32::from(weight > 0),
                is_update: a.is_some() && b.is_some(),
                weight,
                row_id: Some(generation.row_id(&payload).unwrap()),
                payload,
            });
        }
    }
    out
}

fn render(relation: &Multiset) -> String {
    let rows: Vec<String> = relation
        .iter()
        .map(|(row, weight)| format!("{weight}:{row:?}"))
        .collect();
    format!("[{}]", rows.join(", "))
}

fn previous(generation: &Generation) -> CommitEnvelope {
    let empty = Multiset::new();
    PreparedBatch::from_endpoints(
        generation,
        None,
        point(0),
        &empty,
        &empty,
        0,
        "oracle".into(),
    )
    .unwrap()
    .envelope()
    .clone()
}

fn check(seed: u64, step: usize, before: &Multiset, after: &Multiset, batch: &PreparedBatch) {
    let context = || {
        format!(
            "seed={seed} step={step}\nbefore={}\nafter={}\nrows={:?}",
            render(before),
            render(after),
            batch.rows()
        )
    };
    let generation = generation();

    // The output is a change log, not a state: replaying it onto the old image
    // must land exactly on the new one.
    let replayed = before
        .try_union(&batch.delta().unwrap())
        .unwrap_or_else(|error| panic!("replay overflow: {error}\n{}", context()));
    assert!(
        replayed == *after,
        "replay of the emitted changes does not reproduce `after`\n{}",
        context()
    );

    let rows = batch.rows();
    assert_eq!(
        batch.envelope().record_count,
        i64::try_from(rows.len()).unwrap(),
        "record_count disagrees with the physical rows\n{}",
        context()
    );
    for (index, record) in rows.iter().enumerate() {
        assert_eq!(
            record.ordinal,
            i64::try_from(index).unwrap(),
            "{}",
            context()
        );
        assert!(record.weight == 1 || record.weight == -1, "{}", context());
        assert_eq!(
            record.op_type,
            i32::from(record.weight > 0),
            "{}",
            context()
        );
        assert_eq!(
            record.row_id,
            Some(generation.row_id(&record.payload).unwrap()),
            "row_id must be the key identity, not a full-row guess\n{}",
            context()
        );
    }

    // Strongest form: the whole sequence must equal the independent expectation.
    let wanted = expected(&generation, before, after);
    assert_eq!(rows, wanted.as_slice(), "{}", context());

    // Pairing rules spelled out, so a failure names the broken rule even when
    // the record-by-record comparison above is what fails first. Paired rows
    // are adjacent, not "every two rows": inserts and deletes sit between
    // pairs, so chunking by position would pair the wrong records.
    let mut index = 0;
    while index < rows.len() {
        if !rows[index].is_update {
            index += 1;
            continue;
        }
        // A paired row is the old image, so a pair always starts at weight -1;
        // consuming both keeps the AFTER half from starting a pair of its own.
        assert_eq!(rows[index].weight, -1, "{}", context());
        let (old, new) = (&rows[index], &rows[index + 1]);
        assert!(new.is_update, "{}", context());
        assert_eq!(new.weight, 1, "{}", context());
        assert_eq!(old.row_id, new.row_id, "{}", context());
        assert_eq!(old.ordinal + 1, new.ordinal, "{}", context());
        index += 2;
    }
}

#[test]
fn keyed_endpoints_replay_exactly_over_random_transitions() {
    let generation = generation();
    let prior = previous(&generation);
    for seed in 1..=16u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let universe = 8 + rng.below(8) as i64;
        let density = 40 + rng.below(40);
        let mut before = relation(&mut rng, universe, density);
        for step in 0..25 {
            let after = transition(&mut rng, &before, universe);
            let batch = PreparedBatch::from_endpoints(
                &generation,
                Some(&prior),
                point(i64::try_from(step).unwrap() + 1),
                &before,
                &after,
                0,
                "oracle".into(),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "seed={seed} step={step} rejected valid endpoints: {error}\nbefore={}\nafter={}",
                    render(&before),
                    render(&after)
                )
            });
            check(seed, step, &before, &after, &batch);
            before = after;
        }
    }
}

#[test]
fn keyed_output_is_deterministic_for_the_same_endpoints() {
    let generation = generation();
    let prior = previous(&generation);
    let mut rng = Rng(7);
    for _ in 0..50 {
        let before = relation(&mut rng, 12, 50);
        let after = transition(&mut rng, &before, 12);
        let first = PreparedBatch::from_endpoints(
            &generation,
            Some(&prior),
            point(1),
            &before,
            &after,
            11,
            "oracle".into(),
        )
        .unwrap();
        let second = PreparedBatch::from_endpoints(
            &generation,
            Some(&prior),
            point(1),
            &before,
            &after,
            99,
            "other".into(),
        )
        .unwrap();
        assert_eq!(first.rows(), second.rows());
        assert_eq!(
            first.envelope().delta_hash,
            second.envelope().delta_hash,
            "delta hash must not depend on time or writer"
        );
        assert_eq!(first.envelope().batch_id, second.envelope().batch_id);
    }
}

#[test]
fn duplicate_keys_fail_closed() {
    let generation = generation();
    let mut relation = Multiset::new();
    relation.add_row(vec![Value::Int(1), Value::Int(1)], 1);
    relation.add_row(vec![Value::Int(1), Value::Int(2)], 1);
    let after = Multiset::new();
    let error = PreparedBatch::from_endpoints(
        &generation,
        None,
        point(1),
        &relation,
        &after,
        0,
        "oracle".into(),
    )
    .unwrap_err();
    assert!(matches!(error, CdcError::InvalidRow), "{error:?}");
}

#[test]
fn a_null_key_fails_closed() {
    let generation = generation();
    let mut relation = Multiset::new();
    relation.add_row(vec![Value::Null, Value::Int(1)], 1);
    let after = Multiset::new();
    let error = PreparedBatch::from_endpoints(
        &generation,
        None,
        point(1),
        &relation,
        &after,
        0,
        "oracle".into(),
    )
    .unwrap_err();
    assert!(matches!(error, CdcError::InvalidRow), "{error:?}");
}

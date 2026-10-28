//! Independent protocol fixtures and state-transition regressions.
use optd_incremental::cdc::*;
use optd_incremental::{scan, Capabilities, InMemorySource, Multiset, Value, View};
use serde_json::json;
use std::collections::BTreeMap;

const VIEW: &str = "00000000-0000-0000-0000-000000000001";
const GEN: &str = "00000000-0000-0000-0000-000000000002";
const SOURCE: &str = "00000000-0000-0000-0000-000000000003";
fn schema() -> OutputSchema {
    OutputSchema::new(vec![OutputField {
        name: "id".into(),
        data_type: PayloadType::Long,
        required: false,
    }])
    .unwrap()
}
fn generation() -> Generation {
    Generation::new(
        VIEW.into(),
        GEN.into(),
        schema(),
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
fn delta(weight: i64) -> Multiset {
    let mut out = Multiset::new();
    out.add_row(vec![Value::Int(7)], weight);
    out
}

#[test]
fn c1_and_sha_match_independent_python_struct_hashlib_vectors() {
    let v = json!([null, true, -1, "中"]);
    let encoded = canonical_encode(&v).unwrap();
    let hex: String = encoded.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(
        hex,
        "05000000000000000400010102ffffffffffffffff030000000000000003e4b8ad"
    );
    assert_eq!(
        fingerprint(&v).unwrap(),
        "sha256:fde05a28c9c2900ce594eeb752386d31d7c17e7bebb350e91af2a103fe209226"
    );
    assert_eq!(
        fingerprint(&json!(["optd-delta/1", []])).unwrap(),
        "sha256:8340e89d387566d014e10771f955b2ebe109ff704edd7b3fe2afecd19eae63de"
    );
    assert_eq!(
        fingerprint(&json!({"z":null,"a":[1,false]})).unwrap(),
        "sha256:9ed4b26ec46b3992f59973d5e8f73e2ffd62a2f4b7c47cc20b2f31b91d544412"
    );
    assert!(canonical_encode(&json!(1.5)).is_err());
    assert!(canonical_encode(&json!(u64::MAX)).is_err());
}

#[test]
fn strict_json_rejects_duplicate_nested_keys_floats_and_trailing_data() {
    for invalid in [
        "{\"x\":1,\"x\":2}",
        "{\"x\":{\"a\":1,\"a\":2}}",
        "1.2",
        "18446744073709551615",
        "{}{}",
    ] {
        assert!(parse_unique_json(invalid).is_err(), "{invalid}");
    }
    assert_eq!(
        parse_unique_json("-9223372036854775808").unwrap(),
        json!(i64::MIN)
    );
}

#[test]
fn schema_rejects_reserved_names_missing_images_and_wrong_types() {
    assert!(OutputSchema::new(vec![OutputField {
        name: "__META_weight".into(),
        data_type: PayloadType::Long,
        required: false
    }])
    .is_err());
    let field = OutputField {
        name: "x".into(),
        data_type: PayloadType::String,
        required: true,
    };
    assert!(OutputSchema::new(vec![field.clone(), field]).is_err());
    assert!(schema().validate_row(&vec![]).is_err());
    assert!(schema()
        .validate_row(&vec![Value::Str("7".into())])
        .is_err());
    assert!(schema().validate_row(&vec![Value::Null]).is_ok());
    let system = system_fields();
    assert_eq!(
        system.iter().map(|x| x.0).collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    assert_eq!(system[2], (3, "__meta_weight", "long", true));
    assert_eq!(system[7], (8, "__meta_timestamp", "timestamptz", true));
}

#[test]
fn bootstrap_preserves_duplicates_and_empty_batches_have_a_checkpoint() {
    let g = generation();
    let batch = PreparedBatch::new(&g, None, point(1), &delta(3), 1, "writer".into()).unwrap();
    assert_eq!(batch.rows().len(), 1);
    assert_eq!(batch.rows()[0].weight, 3);
    assert_eq!(batch.rows()[0].op_type, 1);
    assert!(!batch.rows()[0].is_update);
    assert!(batch.rows()[0].row_id.is_none());
    assert!(batch.expanded(2).is_err());
    let expanded = batch.expanded(3).unwrap();
    assert_eq!(
        expanded.iter().map(|r| r.record.weight).collect::<Vec<_>>(),
        vec![1, 1, 1]
    );
    assert_eq!(
        expanded.iter().map(|r| r.copy_ordinal).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    let empty = PreparedBatch::new(
        &g,
        Some(batch.envelope()),
        point(2),
        &Multiset::new(),
        2,
        "writer".into(),
    )
    .unwrap();
    assert_eq!(empty.envelope().batch_seq, 2);
    assert_eq!(empty.envelope().record_count, 0);
    assert!(empty.rows().is_empty());
    assert_ne!(batch.envelope().batch_id, empty.envelope().batch_id);
    assert!(PreparedBatch::new(&g, None, point(1), &delta(-1), 1, "writer".into()).is_err());
}

#[test]
fn digest_is_not_batch_identity_and_timestamp_is_not_identity() {
    let g = generation();
    let a = PreparedBatch::new(&g, None, point(1), &delta(1), 1, "first".into()).unwrap();
    let retry = PreparedBatch::new(&g, None, point(1), &delta(1), 99, "second".into()).unwrap();
    assert_eq!(a.envelope().batch_id, retry.envelope().batch_id);
    let b = PreparedBatch::new(
        &g,
        Some(a.envelope()),
        point(2),
        &delta(1),
        2,
        "writer".into(),
    )
    .unwrap();
    assert_eq!(a.envelope().delta_hash, b.envelope().delta_hash);
    assert_ne!(a.envelope().batch_id, b.envelope().batch_id);
    let bad = PreparedBatch::new(
        &g,
        Some(a.envelope()),
        point(1),
        &delta(1),
        2,
        "writer".into(),
    );
    assert!(bad.is_err());
}

#[test]
fn readback_rejects_wrong_sign_duplicate_ordinal_payload_and_envelope_tampering() {
    let g = generation();
    let a = PreparedBatch::new(&g, None, point(1), &delta(2), 1, "writer".into()).unwrap();
    let mut rows = a.rows().to_vec();
    rows[0].op_type = 0;
    assert!(PreparedBatch::from_records(&g, a.envelope().clone(), rows).is_err());
    let mut rows = a.rows().to_vec();
    rows[0].ordinal = 1;
    assert!(PreparedBatch::from_records(&g, a.envelope().clone(), rows).is_err());
    let mut rows = a.rows().to_vec();
    rows[0].weight = 3;
    assert!(PreparedBatch::from_records(&g, a.envelope().clone(), rows).is_err());
    let mut e = a.envelope().clone();
    e.batch_seq = 8;
    assert!(CommitEnvelope::from_json(&serde_json::to_string(&e).unwrap()).is_err());
    assert!(CommitEnvelope::from_json(&serde_json::to_string(a.envelope()).unwrap()).is_ok());
}

#[test]
fn prepare_acknowledge_is_atomic_idempotent_and_rejects_stale_or_foreign_candidates() {
    let mut src = InMemorySource::new(Capabilities::change_log());
    src.insert(0, vec![Value::Int(1)]);
    let mut view = View::load(scan(0), &src, vec![0]).unwrap();
    src.insert(0, vec![Value::Int(2)]);
    let one = view.prepare_refresh(&src).unwrap().unwrap();
    assert_eq!(view.contents().total(), 1);
    assert_eq!(view.watermarks()[&0], 1);
    src.insert(0, vec![Value::Int(3)]);
    let stale = view.prepare_refresh(&src).unwrap().unwrap();
    let mut foreign = View::load(scan(0), &src, vec![0]).unwrap();
    assert!(foreign.acknowledge(&one).is_err());
    assert!(view.acknowledge(&one).unwrap());
    assert!(!view.acknowledge(&one).unwrap());
    assert!(view.acknowledge(&stale).is_err());
    assert_eq!(view.contents().total(), 2);
    view.refresh(&src).unwrap();
    assert_eq!(view.contents().total(), 3);
    assert!(view.acknowledge(&one).is_err());
}

#[test]
fn historical_load_is_exact_and_invalid_columns_or_sum_overflow_leave_state_unchanged() {
    use optd_incremental::{agg, project, AggKind};
    let mut src = InMemorySource::new(Capabilities::change_log());
    src.insert(0, vec![Value::Int(i64::MAX)]);
    src.insert(0, vec![Value::Int(1)]);
    let mut view = View::load_at(
        agg(scan(0), vec![], vec![AggKind::Sum(0)]),
        &src,
        vec![0],
        1,
    )
    .unwrap();
    let before = view.contents().clone();
    assert!(view.prepare_refresh(&src).is_err());
    assert_eq!(view.contents(), &before);
    assert_eq!(view.watermarks()[&0], 1);
    assert!(View::load(project(scan(0), vec![1]), &src, vec![0]).is_err());
    let mut count = Multiset::new();
    count.try_add_row(vec![], i64::MAX).unwrap();
    assert!(count.try_add_row(vec![], 1).is_err());
    assert_eq!(count.total(), i64::MAX);
    assert!(view.refresh(&src).is_err());
}

#[test]
fn malformed_plan_is_an_error_before_rewrite_indexes_its_children() {
    use optd_core::nodes::PlanNode;
    use optd_incremental::{delta, IncNodeType};
    use std::sync::Arc;
    let malformed = Arc::new(PlanNode {
        typ: IncNodeType::Projection(vec![0]),
        children: vec![],
        predicates: vec![],
    });
    assert!(optd_incremental::rules::rewrite(delta(malformed.clone())).is_err());
    let src = InMemorySource::new(Capabilities::change_log());
    assert!(View::load(malformed, &src, vec![]).is_err());
}

fn keyed_generation() -> Generation {
    let schema = OutputSchema::new(vec![
        OutputField {
            name: "id".into(),
            data_type: PayloadType::Long,
            required: true,
        },
        OutputField {
            name: "value".into(),
            data_type: PayloadType::String,
            required: false,
        },
    ])
    .unwrap()
    .keyed(KeySpec(vec![1000]))
    .unwrap();
    Generation::new(
        VIEW.into(),
        GEN.into(),
        schema,
        &scan(0),
        generation().bindings().clone(),
    )
    .unwrap()
}
fn keyed_rows(rows: &[(i64, &str)]) -> Multiset {
    Multiset::from_rows(
        rows.iter()
            .map(|(key, value)| vec![Value::Int(*key), Value::Str((*value).into())]),
    )
}

#[test]
fn keyed_endpoints_pair_replacements_and_reject_unchanged_duplicate_keys() {
    let gen = keyed_generation();
    let before = keyed_rows(&[(7, "old"), (8, "same"), (9, "moved")]);
    let after = keyed_rows(&[(7, "new"), (8, "same"), (10, "moved")]);
    let bootstrap = PreparedBatch::from_endpoints(
        &gen,
        None,
        point(1),
        &Multiset::new(),
        &before,
        1,
        "w".into(),
    )
    .unwrap();
    let batch = PreparedBatch::from_endpoints(
        &gen,
        Some(bootstrap.envelope()),
        point(2),
        &before,
        &after,
        2,
        "w".into(),
    )
    .unwrap();
    assert_eq!(batch.rows().len(), 4);
    assert_eq!(
        batch
            .rows()
            .iter()
            .map(|r| (r.op_type, r.is_update))
            .collect::<Vec<_>>(),
        vec![(0, true), (1, true), (0, false), (1, false)]
    );
    assert_eq!(batch.rows()[0].row_id, batch.rows()[1].row_id);
    assert_ne!(batch.rows()[2].row_id, batch.rows()[3].row_id);
    assert_eq!(batch.apply_to(&gen, &before).unwrap(), after);
    assert_eq!(
        PreparedBatch::from_records(&gen, batch.envelope().clone(), batch.rows().to_vec())
            .unwrap()
            .rows(),
        batch.rows()
    );
    let invalid = keyed_rows(&[(7, "old"), (7, "other")]);
    assert!(PreparedBatch::from_endpoints(
        &gen,
        Some(bootstrap.envelope()),
        point(2),
        &invalid,
        &invalid,
        2,
        "w".into()
    )
    .is_err());
    let duplicate = keyed_rows(&[(7, "old"), (7, "old")]);
    assert!(PreparedBatch::from_endpoints(
        &gen,
        None,
        point(1),
        &Multiset::new(),
        &duplicate,
        2,
        "w".into()
    )
    .is_err());
    let null = Multiset::from_rows([vec![Value::Null, Value::Null]]);
    assert!(PreparedBatch::from_endpoints(
        &gen,
        None,
        point(1),
        &Multiset::new(),
        &null,
        2,
        "w".into()
    )
    .is_err());
    let mut wrong = batch.rows().to_vec();
    wrong[1].is_update = false;
    assert!(PreparedBatch::from_records(&gen, batch.envelope().clone(), wrong).is_err());
    let mut wrong = batch.rows().to_vec();
    wrong[0].row_id = Some("invented".into());
    assert!(PreparedBatch::from_records(&gen, batch.envelope().clone(), wrong).is_err());
    assert!(PreparedBatch::new(&gen, None, point(1), &before, 1, "w".into()).is_err());
}

#[test]
fn keyed_identity_matches_independent_python_c1_base64url_vector() {
    let gen = keyed_generation();
    assert_eq!(gen.row_id(&vec![Value::Int(7),Value::Null]).unwrap(),
        "BQAAAAAAAAAEAwAAAAAAAAAKb3B0ZC1yb3cvMQMAAAAAAAAAJDAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMQMAAAAAAAAAJDAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMgUAAAAAAAAAAQUAAAAAAAAAAgIAAAAAAAAD6AIAAAAAAAAABw");
    for ids in [vec![], vec![999], vec![1001], vec![1000, 1000]] {
        assert!(OutputSchema::new(gen.schema().fields().to_vec())
            .unwrap()
            .keyed(KeySpec(ids))
            .is_err());
    }
    assert_ne!(
        fingerprint(&gen.schema().descriptor()).unwrap(),
        fingerprint(
            &OutputSchema::new(gen.schema().fields().to_vec())
                .unwrap()
                .descriptor()
        )
        .unwrap()
    );
}

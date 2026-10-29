#[path = "../benchmarks/harness.rs"]
mod harness;

use std::path::Path;

#[tokio::test]
#[ignore = "Full query qualification: run explicitly; coverage is emitted by the benchmark example"]
async fn repository_tpch_all_queries_agree_with_full_sql() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("benchmarks/tpch/manifest.json");
    let report = harness::run_manifest(&path, None)
        .await
        .expect("load benchmark manifest");
    assert_eq!(report["executed_variants"], 22);
    assert_eq!(report["failed_variants"], 0, "{report:#}");
    assert_eq!(report["incomplete_variants"], 0, "{report:#}");
}

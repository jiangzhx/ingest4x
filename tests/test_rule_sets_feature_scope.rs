#![cfg(feature = "ingest")]

use ingest4x::rules::RuleSets;
use std::fs;
use tempfile::tempdir;

#[test]
fn load_from_root_allows_ingest_only_root_when_only_ingest_feature_is_enabled() {
    let temp = tempdir().expect("temp dir");
    let ingest_dir = temp.path().join("ingest");
    fs::create_dir_all(&ingest_dir).expect("create ingest dir");

    fs::write(
        ingest_dir.join("default.yaml"),
        r#"
fields:
  appid:
    required: true
    type: string
"#,
    )
    .expect("write ingest default");

    let rule_sets = RuleSets::load_from_root(temp.path()).expect("load ingest-only root");
    assert!(rule_sets.ingest.event("default").is_some());
}

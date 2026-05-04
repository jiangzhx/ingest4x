use ingest4x::rules::RuleSets;
use std::fs;
use tempfile::tempdir;

#[test]
fn load_from_root_loads_ingest_rules() {
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

    let rule_sets = RuleSets::load_from_root(temp.path()).expect("load ingest rules root");
    assert!(rule_sets.ingest.event("default").is_some());
}

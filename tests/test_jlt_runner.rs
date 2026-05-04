use ingest4x::jlt::{run_scope, ExpectedResult, Scope, TestCase};
use ingest4x::rules::Rules;
use serde_json::json;
use tempfile::tempdir;

#[test]
fn run_scope_collects_failed_cases_without_stopping() {
    let temp = tempdir().expect("temp dir");
    let rules_dir = temp.path().join("rules");
    std::fs::create_dir_all(&rules_dir).expect("rules dir");

    std::fs::write(
        rules_dir.join("default.yaml"),
        r#"
fields:
  appid:
    required: true
    type: string
  xwhat:
    required: true
    type: string
"#,
    )
    .expect("write rules");

    let rules = Rules::load_from_dir(&rules_dir).expect("rules should load");
    let scope = Scope::new("demo", temp.path());
    let result = run_scope(
        &scope,
        vec![
            TestCase::new(
                "pass",
                json!({"appid":"A","xwhat":"install"}),
                ExpectedResult::Pass,
            ),
            TestCase::new("fail", json!({"appid":"A"}), ExpectedResult::Pass),
        ],
        false,
        &rules,
    )
    .expect("scope should run");

    assert_eq!(result.passed, 1);
    assert_eq!(result.failed.len(), 1);
    assert_eq!(result.failed[0].description, "fail");
}

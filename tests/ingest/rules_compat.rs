use crate::support::jlt::{
    parse_test_data_from_str, repo_scopes, run_scope_from_disk, ExpectedResult,
};
use ingest4x::db::{init_sqlite_database, seed};
use ingest4x::repositories::{
    CreateProjectInput, ProcessorRepository, ProjectRepository, RuleRepository,
};
use ingest4x::rules::Rules;

#[tokio::test]
async fn ingest_jlt_cases_match_rules() {
    let rules = seeded_rules().await;
    for scope in repo_scopes() {
        let result = run_scope_from_disk(&scope, false, &rules)
            .unwrap_or_else(|err| panic!("scope `{}`: {err}", scope.name));
        assert!(
            result.failed.is_empty(),
            "scope `{}` has failures: {:?}",
            scope.name,
            result.failed
        );
    }
}

async fn seeded_rules() -> Rules {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let rules = RuleRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);

    projects
        .create_project(CreateProjectInput {
            appid: "APPID".to_string(),
            name: "APPID".to_string(),
            enabled: true,
        })
        .await
        .expect("project should be created");
    seed::run(&projects, &rules, &processors)
        .await
        .expect("seed should run");

    rules
        .compile_project_rules("APPID")
        .await
        .expect("seeded rules should compile")
}

#[test]
fn parses_rules_result_keywords_and_error_substrings() {
    let cases = parse_test_data_from_str(
        "inline.jlt",
        r#"
# fail case
{"appid":"APPID","xcontext":{}}
----
fail
missing field `xwhat`

# pass case
{"appid":"APPID","xwhat":"custom_event","xcontext":{"installid":"iid-1","os":"ios","idfa":"idfa-1"}}
----
pass
"#,
    )
    .expect("parse inline cases");

    assert_eq!(cases.len(), 2);
    assert_eq!(cases[0].description, "inline.jlt:2 fail case");
    assert!(matches!(cases[0].expected_result, ExpectedResult::Fail));
    assert_eq!(
        cases[0].expected_error_contains.as_deref(),
        Some("missing field `xwhat`")
    );
    assert_eq!(cases[1].description, "inline.jlt:8 pass case");
    assert!(matches!(cases[1].expected_result, ExpectedResult::Pass));
    assert_eq!(cases[1].expected_error_contains, None);
}

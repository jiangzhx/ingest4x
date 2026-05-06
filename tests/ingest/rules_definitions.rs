use ingest4x::db::{init_sqlite_database, seed};
use ingest4x::repositories::{CreateProjectInput, ProjectRepository, RuleRepository};
use ingest4x::rules::Rules;
use serde_json::json;

async fn load_rules() -> Rules {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let rules = RuleRepository::new(db);

    projects
        .create_project(CreateProjectInput {
            appid: "APPID".to_string(),
            name: "APPID".to_string(),
            enabled: true,
        })
        .await
        .expect("project should be created");
    seed::run(&projects, &rules).await.expect("seed should run");

    rules
        .compile_project_rules("APPID")
        .await
        .expect("seeded rules should compile")
}

#[tokio::test]
async fn rules_validation_errors_expose_stable_codes() {
    let rules = load_rules().await;
    let missing_installid = json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xcontext": {
            "os": "ios",
            "idfa": "idfa-1"
        }
    });

    let error = rules
        .validate("custom_event", &missing_installid)
        .expect_err("missing required field should fail");

    assert_eq!(error.code(), "rules_required_field_missing");
    assert_eq!(error.path(), Some("xcontext.installid"));
}

#[tokio::test]
async fn shipped_rules_require_openid_for_toutiao_and_tiktok() {
    let rules = load_rules().await;

    let toutiao_missing = json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid",
            "os": "toutiao"
        }
    });
    let err = rules
        .validate("custom_event", &toutiao_missing)
        .expect_err("toutiao should require openid");
    assert!(err.to_string().contains("xcontext.openid"));

    let tiktok_ok = json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid",
            "os": "tiktok",
            "openid": "openid-1"
        }
    });
    rules
        .validate("custom_event", &tiktok_ok)
        .expect("tiktok with openid should pass");
}

#[tokio::test]
async fn shipped_rules_restrict_os_to_known_enum_values() {
    let rules = load_rules().await;

    let unknown_os = json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid",
            "os": "symbian"
        }
    });

    let err = rules
        .validate("custom_event", &unknown_os)
        .expect_err("unknown os should fail");
    assert!(err.to_string().contains("xcontext.os"));
}

#[tokio::test]
async fn shipped_rules_enforce_integer_level_for_levelup() {
    let rules = load_rules().await;

    let valid = json!({
        "appid": "APPID",
        "xwhat": "levelup",
        "xwho": "user-1",
        "xcontext": {
            "installid": "iid",
            "os": "ios",
            "idfa": "idfa-1",
            "level": 10
        }
    });
    rules
        .validate("levelup", &valid)
        .expect("integer level should pass");

    let float_level = json!({
        "appid": "APPID",
        "xwhat": "levelup",
        "xwho": "user-1",
        "xcontext": {
            "installid": "iid",
            "os": "ios",
            "idfa": "idfa-1",
            "level": 10.5
        }
    });
    let err = rules
        .validate("levelup", &float_level)
        .expect_err("float level should fail");
    assert!(err.to_string().to_lowercase().contains("integer"));

    let zero_level = json!({
        "appid": "APPID",
        "xwhat": "levelup",
        "xwho": "user-1",
        "xcontext": {
            "installid": "iid",
            "os": "ios",
            "idfa": "idfa-1",
            "level": 0
        }
    });
    let err = rules
        .validate("levelup", &zero_level)
        .expect_err("zero level should fail");
    assert!(err.to_string().contains("greater than 0"));
}

#[tokio::test]
async fn shipped_rules_require_xwho_for_register() {
    let rules = load_rules().await;

    let missing_xwho = json!({
        "appid": "APPID",
        "xwhat": "register",
        "xcontext": {
            "installid": "iid",
            "os": "ios",
            "idfa": "idfa-1"
        }
    });
    let err = rules
        .validate("register", &missing_xwho)
        .expect_err("register should require xwho");
    assert!(err.to_string().contains("xwho"));

    let valid = json!({
        "appid": "APPID",
        "xwhat": "register",
        "xwho": "user-1",
        "xcontext": {
            "installid": "iid",
            "os": "ios",
            "idfa": "idfa-1"
        }
    });
    rules
        .validate("register", &valid)
        .expect("register with xwho should pass");
}

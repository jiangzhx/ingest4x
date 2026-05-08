use ingest4x::db::{init_sqlite_database, seed};
use ingest4x::repositories::{
    CreateProjectInput, ProcessorRepository, ProjectRepository, RuleRepository,
};
use ingest4x::rules::Rules;
use serde_json::{json, Value};

#[tokio::test]
async fn rules_validation_matches_ingest_contract_cases() {
    let rules = load_seeded_rules().await;

    for case in cases() {
        let result = validate(&rules, &case.payload);
        assert_eq!(
            result.is_ok(),
            case.expected_ok,
            "rules result mismatch on `{}`: {:?}",
            case.name,
            result.err()
        );
    }
}

async fn load_seeded_rules() -> Rules {
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

fn validate(rules: &Rules, payload: &Value) -> Result<(), String> {
    let event_name = payload
        .get("xwhat")
        .and_then(Value::as_str)
        .unwrap_or("default");
    rules
        .validate(event_name, payload)
        .map_err(|err| err.to_string())
}

struct Case {
    name: &'static str,
    payload: Value,
    expected_ok: bool,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "install ios with idfa passes",
            payload: base_payload(
                "install",
                None,
                json!({"installid": "iid-1", "os": "ios", "idfa": "idfa-1"}),
            ),
            expected_ok: true,
        },
        Case {
            name: "install ios missing idfa and caid fails",
            payload: base_payload("install", None, json!({"installid": "iid-1", "os": "ios"})),
            expected_ok: false,
        },
        Case {
            name: "startup android with androidid passes",
            payload: base_payload(
                "startup",
                None,
                json!({"installid": "iid-1", "os": "android", "androidid": "androidid-1"}),
            ),
            expected_ok: true,
        },
        Case {
            name: "custom wechat with unionid passes",
            payload: base_payload(
                "custom_event",
                None,
                json!({"installid": "iid-1", "os": "wechat", "unionid": "union-1"}),
            ),
            expected_ok: true,
        },
        Case {
            name: "custom wechat missing ids fails",
            payload: base_payload(
                "custom_event",
                None,
                json!({"installid": "iid-1", "os": "wechat"}),
            ),
            expected_ok: false,
        },
        Case {
            name: "custom toutiao requires openid",
            payload: base_payload(
                "custom_event",
                None,
                json!({"installid": "iid-1", "os": "toutiao"}),
            ),
            expected_ok: false,
        },
        Case {
            name: "custom harmony with oaid passes",
            payload: base_payload(
                "custom_event",
                None,
                json!({"installid": "iid-1", "os": "harmony", "oaid": "oaid-1"}),
            ),
            expected_ok: true,
        },
        Case {
            name: "custom unknown os fails",
            payload: base_payload(
                "custom_event",
                None,
                json!({"installid": "iid-1", "os": "symbian"}),
            ),
            expected_ok: false,
        },
        Case {
            name: "custom missing installid fails",
            payload: base_payload("custom_event", None, json!({"os": "ios", "idfa": "idfa-1"})),
            expected_ok: false,
        },
        Case {
            name: "register missing xwho fails",
            payload: base_payload(
                "register",
                None,
                json!({"installid": "iid-1", "os": "ios", "idfa": "idfa-1"}),
            ),
            expected_ok: false,
        },
        Case {
            name: "payment valid payload passes",
            payload: base_payload(
                "payment",
                Some("user-1"),
                json!({
                    "installid": "iid-1",
                    "os": "ios",
                    "idfa": "idfa-1",
                    "transactionid": "tx-1",
                    "paymenttype": "apple",
                    "currencytype": "CNY",
                    "currencyamount": 6.5,
                    "paymentstatus": true
                }),
            ),
            expected_ok: true,
        },
        Case {
            name: "payment missing transactionid fails",
            payload: base_payload(
                "payment",
                Some("user-1"),
                json!({
                    "installid": "iid-1",
                    "os": "ios",
                    "idfa": "idfa-1",
                    "paymenttype": "apple",
                    "currencytype": "CNY",
                    "currencyamount": 6.5
                }),
            ),
            expected_ok: false,
        },
        Case {
            name: "payment invalid currency fails",
            payload: base_payload(
                "payment",
                Some("user-1"),
                json!({
                    "installid": "iid-1",
                    "os": "ios",
                    "idfa": "idfa-1",
                    "transactionid": "tx-1",
                    "paymenttype": "apple",
                    "currencytype": "INVALID",
                    "currencyamount": 6.5
                }),
            ),
            expected_ok: false,
        },
        Case {
            name: "payment wrong paymentstatus type fails",
            payload: base_payload(
                "payment",
                Some("user-1"),
                json!({
                    "installid": "iid-1",
                    "os": "ios",
                    "idfa": "idfa-1",
                    "transactionid": "tx-1",
                    "paymenttype": "apple",
                    "currencytype": "CNY",
                    "currencyamount": 6.5,
                    "paymentstatus": "success"
                }),
            ),
            expected_ok: false,
        },
        Case {
            name: "levelup positive integer passes",
            payload: base_payload(
                "levelup",
                Some("user-1"),
                json!({"installid": "iid-1", "os": "ios", "idfa": "idfa-1", "level": 3}),
            ),
            expected_ok: true,
        },
        Case {
            name: "levelup zero fails",
            payload: base_payload(
                "levelup",
                Some("user-1"),
                json!({"installid": "iid-1", "os": "ios", "idfa": "idfa-1", "level": 0}),
            ),
            expected_ok: false,
        },
        Case {
            name: "levelup float fails",
            payload: base_payload(
                "levelup",
                Some("user-1"),
                json!({"installid": "iid-1", "os": "ios", "idfa": "idfa-1", "level": 3.5}),
            ),
            expected_ok: false,
        },
    ]
}

fn base_payload(xwhat: &str, xwho: Option<&str>, xcontext: Value) -> Value {
    let mut payload = json!({
        "appid": "APPID",
        "xwhat": xwhat,
        "xcontext": xcontext,
    });

    if let Some(xwho) = xwho {
        payload["xwho"] = Value::String(xwho.to_string());
    }

    payload
}

use crate::support::sinks::create_default_event_sinks;
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

#[test]
fn rhai_rules_validation_matches_ingest_contract_cases() {
    let rules = Rules::from_rhai_script(default_rhai_rules())
        .expect("rhai validation rules should compile");

    for case in cases() {
        let result = validate(&rules, &case.payload);
        assert_eq!(
            result.is_ok(),
            case.expected_ok,
            "rhai rules result mismatch on `{}`: {:?}",
            case.name,
            result.err()
        );
    }
}

#[test]
fn rhai_rules_can_use_switch_on_field_value() {
    let rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    let os = event.field("xcontext.os");

    switch os.value() {
        "ios" => event.any(["xcontext.idfa", "xcontext.caid"]).required(),
        "android" | "harmony" => event.any(["xcontext.oaid", "xcontext.androidid"]).required(),
        _ => ()
    }

    event.result()
}
"#,
    )
    .expect("rhai validation rules should compile");

    let error = rules
        .validate(
            "ignored",
            &json!({
                "xcontext": {
                    "os": "ios"
                }
            }),
        )
        .expect_err("ios without idfa or caid should fail");
    assert!(error.to_string().contains("xcontext.idfa"));

    rules
        .validate(
            "ignored",
            &json!({
                "xcontext": {
                    "os": "harmony",
                    "oaid": "oaid-1"
                }
            }),
        )
        .expect("harmony with oaid should pass");
}

#[test]
fn rhai_rules_can_validate_without_event_name_routing() {
    let rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.field("appid").required("string");
    event.result()
}
"#,
    )
    .expect("rhai validation rules should compile");

    assert!(rules.can_validate("any_event_name"));
}

#[tokio::test]
async fn seed_imports_single_rhai_validation_rule() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    create_default_event_sinks(&db).await;
    let projects = ProjectRepository::new(db.clone());
    let rules = RuleRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);

    projects
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_test_token".to_string(),
        })
        .await
        .expect("project should be created");
    seed::run(&projects, &rules, &processors)
        .await
        .expect("seed should run");

    let default_rule_set = rules
        .list_rule_sets()
        .await
        .expect("rule sets should load")
        .into_iter()
        .find(|rule_set| rule_set.name == "Default ingest rules")
        .expect("default rule set should exist");
    let seeded_rules = rules
        .list_rules(default_rule_set.id)
        .await
        .expect("rules should load");

    assert_eq!(seeded_rules.len(), 1);
    assert_eq!(default_rule_set.wildcard_rule_id, Some(seeded_rules[0].id));
    assert!(seeded_rules[0].xwhat.is_none());
    assert!(seeded_rules[0].content.contains("fn validate(event)"));
}

async fn load_seeded_rules() -> Rules {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    create_default_event_sinks(&db).await;
    let projects = ProjectRepository::new(db.clone());
    let rules = RuleRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);

    let project = projects
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_test_token".to_string(),
        })
        .await
        .expect("project should be created");
    seed::run(&projects, &rules, &processors)
        .await
        .expect("seed should run");

    rules
        .compile_project_rules(project.id)
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

fn default_rhai_rules() -> &'static str {
    r#"
fn validate(event) {
    event.field("appid").required("string");
    event.field("xwhat").required("string");
    event.field("xcontext").required("object");
    event.field("xcontext.installid").required("string");

    let os = event.field("xcontext.os");

    os.required("string").one_of([
        "ios", "android", "harmony", "wechat", "toutiao", "tiktok",
    ]);

    if os.eq("ios") {
        event.any(["xcontext.idfa", "xcontext.caid"]).required();
    }

    if os.eq("android") || os.eq("harmony") {
        event.any(["xcontext.oaid", "xcontext.androidid"]).required();
    }

    if os.eq("wechat") {
        event.any(["xcontext.openid", "xcontext.unionid"]).required();
    }

    if os.eq("toutiao") || os.eq("tiktok") {
        event.field("xcontext.openid").required("string");
    }

    if event.field("xwhat").eq("register") {
        event.field("xwho").required("string");
    }

    if event.field("xwhat").eq("payment") {
        event.field("xwho").required("string");
        event.field("xcontext.transactionid").required("string");
        event.field("xcontext.paymenttype").required("string");

        event.field("xcontext.currencytype")
            .required("string")
            .one_of(currencies());

        event.field("xcontext.currencyamount").required("number");
        event.field("xcontext.paymentstatus").optional("boolean");
    }

    if event.field("xwhat").eq("levelup") {
        event.field("xwho").required("string");
        event.field("xcontext.level").required("integer").gt(0);
    }

    event.result()
}

fn currencies() {
    [
        "JPY", "EUR", "BRL", "HKD", "TWD", "COP", "MXN", "CHF",
        "CAD", "CLP", "AUD", "PEN", "GBP", "CRC", "PLN", "PYG",
        "BOB", "QAR", "ILS", "SEK", "RUB", "CNY", "USD", "ZAR", "SGD",
        "NZD", "BGN", "LKR", "IQD", "TRY", "AED", "DZD", "EGP",
        "IDR", "INR", "NGN", "NOK", "PHP", "PKR", "THB", "UAH",
        "VND",
    ]
}
"#
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

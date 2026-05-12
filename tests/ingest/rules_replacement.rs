use crate::support::sinks::create_default_event_sinks;
use ingest4x::db::{init_sqlite_database, seed};
use ingest4x::repositories::{
    CreateProjectInput, EventSinkRepository, ProcessorRepository, ProjectRepository, RuleRepository,
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
fn rhai_rules_do_not_require_event_result_return_value() {
    let rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.required("appid").string().min(1);
}
"#,
    )
    .expect("rhai validation rules without event.result should compile");

    rules
        .validate("ignored", &json!({"appid": "app-1"}))
        .expect("valid payload should pass without event.result");

    let error = rules
        .validate("ignored", &json!({}))
        .expect_err("missing appid should fail without event.result");
    assert!(error.to_string().contains("missing required field `appid`"));
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

#[test]
fn rhai_required_string_allows_empty_string_but_rejects_missing_null_and_non_string() {
    let rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.required("xwho").string();
    event.result()
}
"#,
    )
    .expect("new required string API should compile");

    rules
        .validate("ignored", &json!({"xwho": ""}))
        .expect("required string should allow empty string");

    let missing = rules
        .validate("ignored", &json!({}))
        .expect_err("missing required string should fail");
    assert!(missing
        .to_string()
        .contains("missing required field `xwho`"));

    let null_value = rules
        .validate("ignored", &json!({"xwho": null}))
        .expect_err("null required string should fail");
    assert!(null_value
        .to_string()
        .contains("missing required field `xwho`"));

    let wrong_type = rules
        .validate("ignored", &json!({"xwho": 1}))
        .expect_err("non-string required string should fail");
    assert!(wrong_type.to_string().contains("expected type `String`"));
}

#[test]
fn rhai_optional_string_skips_missing_and_null_but_validates_present_values() {
    let rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.optional("xwho").string();
    event.result()
}
"#,
    )
    .expect("new optional string API should compile");

    rules
        .validate("ignored", &json!({}))
        .expect("optional string should allow missing field");
    rules
        .validate("ignored", &json!({"xwho": null}))
        .expect("optional string should allow null");
    rules
        .validate("ignored", &json!({"xwho": ""}))
        .expect("optional string should allow empty string");

    let wrong_type = rules
        .validate("ignored", &json!({"xwho": 1}))
        .expect_err("present optional string should validate type");
    assert!(wrong_type.to_string().contains("expected type `String`"));
}

#[test]
fn rhai_string_min_enforces_length_only_when_value_is_present() {
    let required_rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.required("xwho").string().min(1);
    event.result()
}
"#,
    )
    .expect("required string min API should compile");

    let empty_required = required_rules
        .validate("ignored", &json!({"xwho": ""}))
        .expect_err("empty required string with min should fail");
    assert!(empty_required
        .to_string()
        .contains("field `xwho` length must be at least 1"));

    required_rules
        .validate("ignored", &json!({"xwho": "user-1"}))
        .expect("non-empty required string should pass");

    let optional_rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.optional("xwho").string().min(1);
    event.result()
}
"#,
    )
    .expect("optional string min API should compile");

    optional_rules
        .validate("ignored", &json!({}))
        .expect("missing optional string with min should pass");
    optional_rules
        .validate("ignored", &json!({"xwho": null}))
        .expect("null optional string with min should pass");

    let empty_optional = optional_rules
        .validate("ignored", &json!({"xwho": ""}))
        .expect_err("present empty optional string with min should fail");
    assert!(empty_optional
        .to_string()
        .contains("field `xwho` length must be at least 1"));
}

#[test]
fn rhai_typed_required_and_optional_fields_support_common_json_types() {
    let rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.required("xcontext").object();
    event.required("xcontext.level").integer().gt(0);
    event.required("xcontext.amount").number();
    event.optional("xcontext.enabled").boolean();
    event.result()
}
"#,
    )
    .expect("typed chain API should compile");

    rules
        .validate(
            "ignored",
            &json!({
                "xcontext": {
                    "level": 1,
                    "amount": 1.5,
                    "enabled": false
                }
            }),
        )
        .expect("valid typed payload should pass");

    let float_level = rules
        .validate(
            "ignored",
            &json!({
                "xcontext": {
                    "level": 1.5,
                    "amount": 1.5
                }
            }),
        )
        .expect_err("integer field should reject floats");
    assert!(float_level.to_string().contains("expected type `Integer`"));
}

#[test]
fn rhai_number_constraints_support_gt_gte_lt_and_lte() {
    let rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.required("level").integer().gte(1).lte(10);
    event.required("amount").number().gt(0).lt(100);
    event.result()
}
"#,
    )
    .expect("number constraints should compile");

    rules
        .validate("ignored", &json!({"level": 1, "amount": 99.9}))
        .expect("values inside inclusive and exclusive ranges should pass");

    let low_level = rules
        .validate("ignored", &json!({"level": 0, "amount": 1}))
        .expect_err("gte should reject lower values");
    assert!(low_level
        .to_string()
        .contains("field `level` must be greater than or equal to 1"));

    let high_amount = rules
        .validate("ignored", &json!({"level": 1, "amount": 100}))
        .expect_err("lt should reject equal upper boundary");
    assert!(high_amount
        .to_string()
        .contains("field `amount` must be less than 100"));
}

#[test]
fn rhai_string_enum_rejects_unknown_values_and_invalid_rule_definitions() {
    let rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.required("xcontext.os").string().enum(["ios", "android"]);
    event.result()
}
"#,
    )
    .expect("string enum API should compile");

    rules
        .validate("ignored", &json!({"xcontext": {"os": "ios"}}))
        .expect("known enum value should pass");

    let unknown = rules
        .validate("ignored", &json!({"xcontext": {"os": "symbian"}}))
        .expect_err("unknown enum value should fail");
    assert!(unknown
        .to_string()
        .contains("field `xcontext.os` must be one of [ios, android]"));

    let invalid_values = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.required("x").string().enum(["ios", 1, true]);
    event.result()
}
"#,
    )
    .expect("invalid enum value types are detected when the rule runs");

    let error = invalid_values
        .validate("ignored", &json!({"x": "ios"}))
        .expect_err("mixed enum values should be a rule definition error");
    assert!(error
        .to_string()
        .contains("enum values for field `x` must all be strings"));
}

#[test]
fn rhai_string_rules_can_explicitly_ignore_case_for_enum_and_eq() {
    let rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    let os = event.required("xcontext.os")
        .string()
        .ignore_case()
        .enum(["ios", "android"]);

    if os.eq("ios") {
        event.required("xcontext.idfa").string();
    }

    event.result()
}
"#,
    )
    .expect("ignore_case string rule should compile");

    rules
        .validate(
            "ignored",
            &json!({
                "xcontext": {
                    "os": "iOS",
                    "idfa": ""
                }
            }),
        )
        .expect("ignore_case enum and eq should accept different case");

    let strict_rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.required("xcontext.os").string().enum(["ios", "android"]);
    event.result()
}
"#,
    )
    .expect("strict string enum rule should compile");

    let err = strict_rules
        .validate("ignored", &json!({"xcontext": {"os": "iOS"}}))
        .expect_err("plain enum should stay case-sensitive");
    assert!(err
        .to_string()
        .contains("field `xcontext.os` must be one of [ios, android]"));
}

#[test]
fn rhai_string_rules_can_match_regular_expressions() {
    let rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.required("xcontext.installid")
        .string()
        .matches("^[A-Za-z0-9_-]+$");
    event.optional("xcontext.channel")
        .string()
        .matches("^ch-[0-9]+$");
    event.result()
}
"#,
    )
    .expect("regex string rules should compile");

    rules
        .validate(
            "ignored",
            &json!({
                "xcontext": {
                    "installid": "install_123"
                }
            }),
        )
        .expect("missing optional regex field should pass");

    rules
        .validate(
            "ignored",
            &json!({
                "xcontext": {
                    "installid": "install-123",
                    "channel": "ch-12"
                }
            }),
        )
        .expect("matching required and optional regex fields should pass");

    let err = rules
        .validate(
            "ignored",
            &json!({
                "xcontext": {
                    "installid": "install 123"
                }
            }),
        )
        .expect_err("non-matching regex field should fail");
    assert!(err
        .to_string()
        .contains("field `xcontext.installid` must match regex `^[A-Za-z0-9_-]+$`"));
}

#[test]
fn rhai_string_regex_can_reuse_ignore_case_and_reject_invalid_patterns() {
    let rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.required("xcontext.os")
        .string()
        .ignore_case()
        .matches("^ios$");
    event.result()
}
"#,
    )
    .expect("ignore_case regex rule should compile");

    rules
        .validate("ignored", &json!({"xcontext": {"os": "iOS"}}))
        .expect("ignore_case should make regex matching case-insensitive");

    let invalid_regex = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.required("x").string().matches("[");
    event.result()
}
"#,
    )
    .expect("invalid regex is detected when the rule runs");

    let err = invalid_regex
        .validate("ignored", &json!({"x": "value"}))
        .expect_err("invalid regex pattern should be a rule definition error");
    assert!(err.to_string().contains("invalid regex for field `x`"));
}

#[test]
fn rhai_string_rules_can_validate_dates_with_chrono_formatters() {
    let rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.required("xcontext.day").string().date("%Y-%m-%d");
    event.optional("xcontext.expires_on").string().date("%Y-%m-%d");
    event.result()
}
"#,
    )
    .expect("date string rule should compile");

    rules
        .validate(
            "ignored",
            &json!({
                "xcontext": {
                    "day": "2024-02-29"
                }
            }),
        )
        .expect("valid leap-day date should pass");

    let wrong_shape = rules
        .validate(
            "ignored",
            &json!({
                "xcontext": {
                    "day": "2024-2-29"
                }
            }),
        )
        .expect_err("date must use fixed yyyy-mm-dd shape");
    assert!(wrong_shape
        .to_string()
        .contains("field `xcontext.day` must be a valid date matching format `%Y-%m-%d`"));

    let invalid_calendar_date = rules
        .validate(
            "ignored",
            &json!({
                "xcontext": {
                    "day": "2024-02-30"
                }
            }),
        )
        .expect_err("date must be a real calendar date");
    assert!(invalid_calendar_date
        .to_string()
        .contains("field `xcontext.day` must be a valid date matching format `%Y-%m-%d`"));
}

#[test]
fn rhai_string_date_uses_the_user_supplied_chrono_formatter() {
    let slash_rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.required("x").string().date("%d/%m/%Y");
    event.result()
}
"#,
    )
    .expect("custom chrono date format should compile");

    slash_rules
        .validate("ignored", &json!({"x": "29/02/2024"}))
        .expect("custom chrono date format should pass");

    let wrong_order = slash_rules
        .validate("ignored", &json!({"x": "2024/02/29"}))
        .expect_err("date must match the configured formatter");
    assert!(wrong_order
        .to_string()
        .contains("field `x` must be a valid date matching format `%d/%m/%Y`"));
}

#[test]
fn rhai_string_rules_can_validate_times_and_datetimes_with_chrono_formatters() {
    let rules = Rules::from_rhai_script(
        r#"
fn validate(event) {
    event.required("xcontext.time").string().time("%H:%M:%S");
    event.required("xcontext.created_at").string().datetime("%Y-%m-%d %H:%M:%S");
    event.result()
}
"#,
    )
    .expect("time and datetime string rules should compile");

    rules
        .validate(
            "ignored",
            &json!({
                "xcontext": {
                    "time": "23:59:58",
                    "created_at": "2024-02-29 23:59:58"
                }
            }),
        )
        .expect("valid time and datetime should pass");

    let invalid_time = rules
        .validate(
            "ignored",
            &json!({
                "xcontext": {
                    "time": "24:00:00",
                    "created_at": "2024-02-29 23:59:58"
                }
            }),
        )
        .expect_err("invalid time should fail");
    assert!(invalid_time
        .to_string()
        .contains("field `xcontext.time` must be a valid time matching format `%H:%M:%S`"));

    let invalid_datetime = rules
        .validate(
            "ignored",
            &json!({
                "xcontext": {
                    "time": "23:59:58",
                    "created_at": "2024-02-30 23:59:58"
                }
            }),
        )
        .expect_err("invalid datetime should fail");
    assert!(invalid_datetime.to_string().contains(
        "field `xcontext.created_at` must be a valid datetime matching format `%Y-%m-%d %H:%M:%S`"
    ));
}

#[tokio::test]
async fn seed_imports_single_rhai_validation_rule() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    create_default_event_sinks(&db).await;
    let projects = ProjectRepository::new(db.clone());
    let rules = RuleRepository::new(db.clone());
    let sinks = EventSinkRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);

    projects
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_test_token".to_string(),
        })
        .await
        .expect("project should be created");
    seed::run(&projects, &rules, &sinks, &processors)
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
    let sinks = EventSinkRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);

    let project = projects
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_test_token".to_string(),
        })
        .await
        .expect("project should be created");
    seed::run(&projects, &rules, &sinks, &processors)
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
    event.required("appid").string().min(1);
    let xwhat = event.required("xwhat").string().min(1);

    event.required("xcontext").object();
    event.required("xcontext.installid").string().min(1);

    let os = event.required("xcontext.os").string().ignore_case().enum([
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
        event.required("xcontext.openid").string().min(1);
    }

    if xwhat.eq("register") {
        event.required("xwho").string().min(1);
    }

    if xwhat.eq("payment") {
        event.required("xwho").string().min(1);
        event.required("xcontext.transactionid").string().min(1);
        event.required("xcontext.paymenttype").string().min(1);

        event.required("xcontext.currencytype")
            .string()
            .enum(currencies());

        event.required("xcontext.currencyamount").number();
        event.optional("xcontext.paymentstatus").boolean();
    }

    if xwhat.eq("levelup") {
        event.required("xwho").string().min(1);
        event.required("xcontext.level").integer().gt(0);
    }
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

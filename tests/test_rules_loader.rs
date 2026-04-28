use ingest4x::rules::{FieldType, Rules};
use serde_json::json;
use std::fs;
use tempfile::tempdir;

#[test]
fn loads_rules_from_directory_defaults_chain() {
    let temp = tempdir().expect("temp dir");
    let rules_dir = temp.path().join("rules");
    let user_dir = rules_dir.join("user");
    fs::create_dir_all(&user_dir).expect("rules dir");

    fs::write(
        rules_dir.join("default.yaml"),
        r#"
fields:
  appid:
    required: true
    type: string
  xcontext:
    required: true
    type: object
  xcontext.installid:
    required: true
    type: string
  xcontext.os:
    required: true
    type: string
"#,
    )
    .expect("write root default");

    fs::write(
        user_dir.join("default.yaml"),
        r#"
fields:
  xwho:
    required: true
    type: string
"#,
    )
    .expect("write user default");

    fs::write(
        user_dir.join("payment.yaml"),
        r#"
fields:
  xcontext.transactionid:
    required: true
    type: string
  xcontext.currencyamount:
    required: true
    type: number
  xcontext.currencytype:
    required: true
    type: string
    enum: [CNY, USD]
"#,
    )
    .expect("write payment");

    let rules = Rules::load_from_dir(&rules_dir).expect("rules should load");
    let payment = rules.event("payment").expect("payment rule");

    assert_eq!(
        payment.field("appid").expect("appid").field_type(),
        Some(FieldType::String)
    );
    assert!(payment.field("appid").expect("appid").required());
    assert!(payment.field("xwho").expect("xwho").required());
    assert_eq!(
        payment
            .field("xcontext.currencytype")
            .expect("currencytype")
            .enum_values(),
        Some(&vec!["CNY".to_string(), "USD".to_string()])
    );
}

#[test]
fn supports_integer_field_type() {
    let temp = tempdir().expect("temp dir");
    let rules_dir = temp.path().join("rules");
    let user_dir = rules_dir.join("user");
    fs::create_dir_all(&user_dir).expect("rules dir");

    fs::write(
        rules_dir.join("default.yaml"),
        r#"
fields:
  xcontext.level:
    required: true
    type: integer
    gt: 0
"#,
    )
    .expect("write root default");
    fs::write(user_dir.join("levelup.yaml"), "\n").expect("write levelup");

    let rules = Rules::load_from_dir(&rules_dir).expect("rules should load");
    let levelup = rules.event("levelup").expect("levelup rule");
    assert_eq!(
        levelup.field("xcontext.level").expect("level").field_type(),
        Some(FieldType::Integer)
    );

    rules
        .validate("levelup", &json!({"xcontext": {"level": 2}}))
        .expect("integer should pass");
    let err = rules
        .validate("levelup", &json!({"xcontext": {"level": 2.5}}))
        .expect_err("float should fail integer type");
    assert!(err.to_string().to_lowercase().contains("integer"));
}

#[test]
fn supports_multiline_empty_fields_map() {
    let temp = tempdir().expect("temp dir");
    let rules_dir = temp.path().join("rules");
    fs::create_dir_all(&rules_dir).expect("rules dir");

    fs::write(rules_dir.join("default.yaml"), "fields:\n  {}\n").expect("write default");

    let rules = Rules::load_from_dir(&rules_dir).expect("rules should load");
    let default_rule = rules.event("default").expect("default rule");

    assert!(default_rule.field("appid").is_none());
}

#[test]
fn validates_conditional_required_any_rules() {
    let temp = tempdir().expect("temp dir");
    let rules_dir = temp.path().join("rules");
    let custom_dir = rules_dir.join("custom");
    fs::create_dir_all(&custom_dir).expect("rules dir");

    fs::write(
        rules_dir.join("default.yaml"),
        r#"
fields:
  appid:
    required: true
    type: string
  xcontext:
    required: true
    type: object
  xcontext.installid:
    required: true
    type: string
  xcontext.os:
    required: true
    type: string
    required_any_when:
      - equals: ios
        fields: [xcontext.idfa, xcontext.caid]
      - equals: android
        fields: [xcontext.oaid, xcontext.androidid]
"#,
    )
    .expect("write root default");

    fs::write(custom_dir.join("custom_event.yaml"), "\n").expect("write custom_event");

    let rules = Rules::load_from_dir(&rules_dir).expect("rules should load");
    let ios_missing = json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid",
            "os": "iOS"
        }
    });
    let err = rules
        .validate("custom_event", &ios_missing)
        .expect_err("ios should require idfa or caid");
    assert!(err.to_string().contains("xcontext.idfa"));
    assert!(err.to_string().contains("xcontext.caid"));

    let android_ok = json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid",
            "os": "android",
            "oaid": "oaid1"
        }
    });
    rules
        .validate("custom_event", &android_ok)
        .expect("android with oaid should pass");
}

#[test]
fn rejects_duplicate_event_names_across_directories() {
    let temp = tempdir().expect("temp dir");
    let rules_dir = temp.path().join("rules");
    let user_dir = rules_dir.join("user");
    let vip_dir = rules_dir.join("vip");
    fs::create_dir_all(&user_dir).expect("user dir");
    fs::create_dir_all(&vip_dir).expect("vip dir");

    fs::write(rules_dir.join("default.yaml"), "\n").expect("write default");
    fs::write(user_dir.join("login.yaml"), "\n").expect("write user login");
    fs::write(vip_dir.join("login.yaml"), "\n").expect("write vip login");

    let err = Rules::load_from_dir(&rules_dir).expect_err("duplicate event should fail");
    assert!(err.to_string().contains("duplicate event"));
}

#[test]
fn rejects_explicit_extends_in_directory_mode() {
    let temp = tempdir().expect("temp dir");
    let rules_dir = temp.path().join("rules");
    fs::create_dir_all(&rules_dir).expect("rules dir");

    fs::write(
        rules_dir.join("default.yaml"),
        r#"
extends: other
"#,
    )
    .expect("write default");

    let err = Rules::load_from_dir(&rules_dir).expect_err("extends should be rejected");
    assert!(err.to_string().contains("extends"));
}

#[test]
fn rejects_type_specific_constraints_on_wrong_field_type() {
    let temp = tempdir().expect("temp dir");
    let rules_dir = temp.path().join("rules");
    let custom_dir = rules_dir.join("custom");
    fs::create_dir_all(&custom_dir).expect("rules dir");

    fs::write(
        rules_dir.join("default.yaml"),
        r#"
fields:
  appid:
    required: true
    type: string
    gt: 1
"#,
    )
    .expect("write default");
    fs::write(custom_dir.join("custom_event.yaml"), "\n").expect("write custom_event");

    let err =
        Rules::load_from_dir(&rules_dir).expect_err("invalid type-specific rules should fail");
    let message = err.to_string();
    assert!(message.contains("appid"));
    assert!(message.contains("string"));
}

#[test]
fn falls_back_to_root_default_rules_for_unknown_event() {
    let temp = tempdir().expect("temp dir");
    let rules_dir = temp.path().join("rules");
    let user_dir = rules_dir.join("user");
    fs::create_dir_all(&user_dir).expect("rules dir");

    fs::write(
        rules_dir.join("default.yaml"),
        r#"
fields:
  appid:
    required: true
    type: string
  xcontext.installid:
    required: true
    type: string
"#,
    )
    .expect("write default");

    fs::write(
        user_dir.join("default.yaml"),
        r#"
fields:
  xwho:
    required: true
    type: string
"#,
    )
    .expect("write user default");

    let rules = Rules::load_from_dir(&rules_dir).expect("rules should load");
    let payload = json!({
        "appid": "APPID",
        "xwhat": "brand_new_event",
        "xcontext": {}
    });
    let err = rules
        .validate("brand_new_event", &payload)
        .expect_err("root default rules should apply");
    assert!(err.to_string().contains("xcontext.installid"));
    assert!(!err.to_string().contains("xwho"));
}

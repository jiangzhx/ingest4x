use ingest4x::settings::{EventSinkConfig, LogLevel, Settings};
use std::fs;
use tempfile::tempdir;

#[test]
fn default_settings_loads_root_ingest4x_toml() {
    let settings = Settings::init().expect("default settings should load");

    assert!(settings.events.sink.contains_key("stdout_all"));
}

#[test]
fn loads_config_without_rules_section() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("rules-config.toml");

    fs::write(
        &config_path,
        r#"
[ingest]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[events.sink.stdout]
type = "stdout"

[[events.valid.routes]]
sinks = ["stdout"]

[[events.invalid.routes]]
sinks = ["stdout"]
"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    assert_eq!(settings.logging.level, LogLevel::Info);
    assert_eq!(settings.logging.format, "json");
}

#[test]
fn loads_config_without_optional_sections() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("mock-config.toml");

    fs::write(
        &config_path,
        r#"
[ingest]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[events.sink.stdout]
type = "stdout"

[[events.valid.routes]]
sinks = ["stdout"]

[[events.invalid.routes]]
sinks = ["stdout"]
"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    assert!(settings.events.sink.contains_key("stdout"));
}

#[test]
fn local_config_can_set_logging_level_and_format() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("dev-config.toml");

    fs::write(
        &config_path,
        r#"
[ingest]
bind_address = "127.0.0.1:8090"

[logging]
level = "debug"
format = "json"

[management]
bind_address = "127.0.0.1:18090"
admin_password = "local-admin-password"

[events.sink.stdout]
type = "stdout"

[[events.valid.routes]]
sinks = ["stdout"]

[[events.invalid.routes]]
sinks = ["stdout"]
"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    assert_eq!(settings.logging.level, LogLevel::Debug);
    assert_eq!(settings.logging.format, "json");
    assert_eq!(
        settings.management.admin_password.as_deref(),
        Some("local-admin-password")
    );
}

#[test]
fn loads_event_sinks_and_status_routes_from_config_file() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("events-config.toml");

    fs::write(
        &config_path,
        r#"
[ingest]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[events.sink.kafka_payment]
type = "kafka"
bootstrap_servers = "127.0.0.1:9092"
topic = "ingest4x-payment-events"

[events.sink.stdout_invalid]
type = "stdout"

[[events.valid.routes]]
appid = ["game-a"]
xwhat = ["payment"]
sinks = ["kafka_payment"]

[[events.valid.routes]]
sinks = ["kafka_payment"]

[[events.invalid.routes]]
sinks = ["stdout_invalid"]
"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    assert_eq!(settings.events.sink.len(), 2);
    assert!(matches!(
        settings.events.sink.get("kafka_payment"),
        Some(EventSinkConfig::Kafka { topic, .. }) if topic == "ingest4x-payment-events"
    ));
    assert!(matches!(
        settings.events.sink.get("stdout_invalid"),
        Some(EventSinkConfig::Stdout)
    ));
    assert_eq!(settings.events.valid.routes.len(), 2);
    assert_eq!(
        settings.events.valid.routes[0].appid.as_deref(),
        Some(&["game-a".to_string()][..])
    );
    assert_eq!(
        settings.events.valid.routes[0].xwhat.as_deref(),
        Some(&["payment".to_string()][..])
    );
    assert_eq!(
        settings.events.valid.routes[0].sinks,
        vec!["kafka_payment".to_string()]
    );
    assert_eq!(
        settings.events.invalid.routes[0].sinks,
        vec!["stdout_invalid".to_string()]
    );
}

use ingest4x::settings::{AutoOffsetReset, EventSinkConfig, LogLevel, Settings};
use std::fs;
use tempfile::tempdir;

#[test]
fn default_settings_loads_root_ingest4x_toml() {
    let settings = Settings::init().expect("default settings should load");

    assert!(settings.events.sink.contains_key("events"));
}

#[test]
fn example_settings_loads_mysql_kafka_wal_profile() {
    let settings =
        Settings::init_with_file("ingest4x.example.toml").expect("example settings should load");

    assert_eq!(
        settings
            .database
            .as_ref()
            .map(|database| database.url.as_str()),
        Some("mysql://root:root@127.0.0.1:3306/ingest4x")
    );
    assert_eq!(settings.wal.dir, "./wal");
    assert!(
        settings.events.sink.is_empty(),
        "example config should rely on DB-managed sinks instead of [events.sink.*]"
    );
}

#[test]
fn rejects_config_without_wal_section() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("rules-config.toml");

    fs::write(
        &config_path,
        r#"
[ingest]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[events.sink.events]
type = "stdout"

[events.sink.events_error]
type = "stdout"
"#,
    )
    .expect("write config");

    let error = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect_err("settings without wal should fail");

    assert!(error.to_string().contains("wal"));
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

[wal]
dir = "./wal"

[events.sink.events]
type = "stdout"

[events.sink.events_error]
type = "stdout"
"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    assert!(settings.events.sink.contains_key("events"));
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

[wal]
dir = "./wal"

[events.sink.events]
type = "stdout"

[events.sink.events_error]
type = "stdout"
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
fn loads_event_sinks_from_config_file() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("events-config.toml");

    fs::write(
        &config_path,
        r#"
[ingest]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "./wal"

[events.sink.kafka_payment]
type = "kafka"
bootstrap_servers = "127.0.0.1:9092"
topic = "ingest4x-payment-events"

[events.sink.stdout_events_error]
type = "stdout"
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
        settings.events.sink.get("stdout_events_error"),
        Some(EventSinkConfig::Stdout { .. })
    ));
}

#[test]
fn event_sink_auto_offset_reset_defaults_to_latest_and_allows_earliest() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("offset-reset-config.toml");

    fs::write(
        &config_path,
        r#"
[ingest]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "./wal"

[events.sink.events]
type = "stdout"

[events.sink.events_error]
type = "stdout"
auto_offset_reset = "earliest"
"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    assert_eq!(
        settings
            .events
            .sink
            .get("events")
            .map(EventSinkConfig::auto_offset_reset),
        Some(AutoOffsetReset::Latest)
    );
    assert_eq!(
        settings
            .events
            .sink
            .get("events_error")
            .map(EventSinkConfig::auto_offset_reset),
        Some(AutoOffsetReset::Earliest)
    );
}

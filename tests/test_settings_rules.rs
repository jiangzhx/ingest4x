use ingest4x::settings::{EventSinkConfig, FileSinkRotation, LogLevel, Settings};
use std::fs;
use tempfile::tempdir;
use tracing_appender::non_blocking::DEFAULT_BUFFERED_LINES_LIMIT;

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
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[events.sink.stdout]
type = "stdout"

[[events.valid.routes]]
sinks = ["stdout"]
ack = ["stdout"]

[[events.invalid.routes]]
sinks = ["stdout"]
ack = ["stdout"]

[redis]
address = "redis://localhost:6379"
connections_max_size = 10
connections_min_size = 1
"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    assert_eq!(settings.server.log_level, LogLevel::Info);
    assert_eq!(settings.server.log_format, "json");
}

#[test]
fn loads_config_without_redis_section() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("mock-config.toml");

    fs::write(
        &config_path,
        r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[events.sink.stdout]
type = "stdout"

[[events.valid.routes]]
sinks = ["stdout"]
ack = ["stdout"]

[[events.invalid.routes]]
sinks = ["stdout"]
ack = ["stdout"]
"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    assert!(settings.redis.is_none());
    assert!(settings.events.sink.contains_key("stdout"));
}

#[test]
fn local_config_can_set_log_level_and_format() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("dev-config.toml");

    fs::write(
        &config_path,
        r#"
[server]
bind_address = "127.0.0.1:8090"
log_level = "debug"
log_format = "json"

[management]
bind_address = "127.0.0.1:18090"
admin_password = "local-admin-password"

[events.sink.stdout]
type = "stdout"

[[events.valid.routes]]
sinks = ["stdout"]
ack = ["stdout"]

[[events.invalid.routes]]
sinks = ["stdout"]
ack = ["stdout"]
"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    assert_eq!(settings.server.log_level, LogLevel::Debug);
    assert_eq!(settings.server.log_format, "json");
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
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[events.sink.kafka_payment]
type = "kafka"
bootstrap_servers = "127.0.0.1:9092"
topic = "ingest4x-payment-events"

[events.sink.file_accepted]
type = "file"
path = "logs/events.jsonl"
format = "jsonl"
lossy = true
buffered_lines_limit = 64

[events.sink.file_rejected]
type = "file"
path = "logs/events-rejected.jsonl"
format = "jsonl"

[[events.valid.routes]]
appid = ["game-a"]
xwhat = ["payment"]
sinks = ["kafka_payment", "file_accepted"]
ack = ["kafka_payment"]

[[events.valid.routes]]
sinks = ["file_accepted"]
ack = ["file_accepted"]

[[events.invalid.routes]]
sinks = ["file_rejected"]
ack = ["file_rejected"]
"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    assert_eq!(settings.events.sink.len(), 3);
    assert!(matches!(
        settings.events.sink.get("kafka_payment"),
        Some(EventSinkConfig::Kafka { topic, .. }) if topic == "ingest4x-payment-events"
    ));
    assert!(matches!(
        settings.events.sink.get("file_accepted"),
        Some(EventSinkConfig::File {
            lossy: true,
            buffered_lines_limit: 64,
            ..
        })
    ));
    assert!(matches!(
        settings.events.sink.get("file_rejected"),
        Some(EventSinkConfig::File {
            lossy: false,
            buffered_lines_limit: DEFAULT_BUFFERED_LINES_LIMIT,
            ..
        })
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
        vec!["kafka_payment".to_string(), "file_accepted".to_string()]
    );
    assert_eq!(
        settings.events.valid.routes[0].ack,
        vec!["kafka_payment".to_string()]
    );
    assert_eq!(
        settings.events.invalid.routes[0].sinks,
        vec!["file_rejected".to_string()]
    );
}

#[test]
fn file_sink_defaults_to_hourly_rotation_with_24_retained_files() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("file-sink-defaults.toml");

    fs::write(
        &config_path,
        r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[events.sink.file_valid]
type = "file"
path = "logs/events-valid.jsonl"

[[events.valid.routes]]
sinks = ["file_valid"]
ack = ["file_valid"]
"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    assert!(matches!(
        settings.events.sink.get("file_valid"),
        Some(EventSinkConfig::File {
            rotation: FileSinkRotation::Hourly,
            retention_files: 24,
            ..
        })
    ));
}

#[test]
fn file_sink_can_override_rotation_and_retention_files() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("file-sink-rotation.toml");

    fs::write(
        &config_path,
        r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[events.sink.file_valid]
type = "file"
path = "logs/events-valid.jsonl"
rotation = "daily"
retention_files = 7

[[events.valid.routes]]
sinks = ["file_valid"]
ack = ["file_valid"]
"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    assert!(matches!(
        settings.events.sink.get("file_valid"),
        Some(EventSinkConfig::File {
            rotation: FileSinkRotation::Daily,
            retention_files: 7,
            ..
        })
    ));
}

use ingest4x::settings::{LogLevel, Settings};
use std::fs;
use tempfile::tempdir;

#[test]
fn default_settings_loads_root_ingest4x_toml() {
    let settings = Settings::init().expect("default settings should load");

    assert!(!settings.ingest.bind_address.is_empty());
    assert!(!settings.wal.dir.is_empty());
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

"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    assert_eq!(settings.ingest.bind_address, "127.0.0.1:8090");
    assert_eq!(settings.wal.dir, "./wal");
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

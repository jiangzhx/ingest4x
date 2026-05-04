use ingest4x::settings::{default_database_refresh_interval_secs, Settings};
use std::fs;
use tempfile::tempdir;

#[test]
fn settings_reads_database_section() {
    let settings = Settings::init_with_file("tests/fixtures/configs/database-only.toml")
        .expect("settings should load");

    let database = settings.database.expect("database config");
    assert_eq!(database.url, "sqlite://tmp/admin.db?mode=rwc");
    assert_eq!(database.refresh_interval_secs, 7);
}

#[test]
fn settings_reads_management_section() {
    let settings = load_settings_with_management_section(
        r#"
[management]
bind_address = "127.0.0.1:18091"
admin_password = "configured-password"
"#,
    );

    assert_eq!(settings.management.bind_address, "127.0.0.1:18091");
    assert_eq!(
        settings.management.admin_password.as_deref(),
        Some("configured-password")
    );
}

#[test]
fn settings_uses_default_database_refresh_interval_when_omitted() {
    let settings = load_settings(
        r#"
[database]
url = "sqlite://tmp/admin.db?mode=rwc"
"#,
    );

    let database = settings.database.expect("database config");
    assert_eq!(database.url, "sqlite://tmp/admin.db?mode=rwc");
    assert_eq!(
        database.refresh_interval_secs,
        default_database_refresh_interval_secs()
    );
}

#[test]
fn settings_leaves_database_none_when_section_missing() {
    let settings = load_settings("");

    assert!(settings.database.is_none());
}

#[test]
fn settings_reads_wal_flush_group_commit_fields() {
    let settings = load_settings(
        r#"
[wal]
dir = "./wal"
flush_max_interval = "10ms"
flush_max_records = 1000
flush_max_bytes = 4194304
"#,
    );

    let wal = settings.wal.expect("wal config");
    assert_eq!(wal.flush_max_interval, "10ms");
    assert_eq!(wal.flush_max_records, 1000);
    assert_eq!(wal.flush_max_bytes, 4 * 1024 * 1024);
}

fn load_settings(database_section: &str) -> Settings {
    load_settings_with_management_section(&format!(
        r#"
[management]
bind_address = "127.0.0.1:18090"

{database_section}
"#
    ))
}

fn load_settings_with_management_section(extra_sections: &str) -> Settings {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("database-only.toml");

    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"

{extra_sections}
"#,
        ),
    )
    .expect("write config");

    Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load")
}

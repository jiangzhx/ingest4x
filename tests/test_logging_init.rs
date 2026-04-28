use ingest4x::logging::init_logging_with_console_writers;
use ingest4x::settings::Settings;
use std::fs;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[test]
fn init_logging_writes_standard_log_file() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("logging-config.toml");

    fs::write(
        &config_path,
        r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"
"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    init_logging_with_console_writers(&settings, std::io::sink, std::io::sink)
        .expect("init logging");

    tracing::info!(target: "ingest4x::server", "log file probe");

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let files = fs::read_dir("logs")
            .ok()
            .into_iter()
            .flat_map(|entries| entries.flatten())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();

        if files.iter().any(|name| name.starts_with("ingest4x.")) {
            return;
        }

        thread::sleep(Duration::from_millis(50));
    }

    panic!("expected ingest4x log files under logs, found none");
}

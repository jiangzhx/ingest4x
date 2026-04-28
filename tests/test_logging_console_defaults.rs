use ingest4x::logging::init_logging_with_console_writers;
use ingest4x::settings::Settings;
use std::fs;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Default)]
struct SharedBuffer {
    bytes: Arc<Mutex<Vec<u8>>>,
}

struct SharedBufferWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    fn contents(&self) -> String {
        String::from_utf8(self.bytes.lock().expect("buffer lock").clone()).expect("utf8")
    }
}

impl<'writer> MakeWriter<'writer> for SharedBuffer {
    type Writer = SharedBufferWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        SharedBufferWriter {
            bytes: self.bytes.clone(),
        }
    }
}

impl Write for SharedBufferWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes
            .lock()
            .expect("buffer lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn logging_writes_info_logs_to_console_by_default() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("production-config.toml");

    fs::write(
        &config_path,
        r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[database]
url = "sqlite::memory:"
"#,
    )
    .expect("write config");

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");
    let console = SharedBuffer::default();

    init_logging_with_console_writers(&settings, console.clone(), std::io::sink)
        .expect("init logging");

    tracing::info!(target: "ingest4x::server", "server startup log");
    tracing::warn!(
        target: "ingest4x::ingest",
        appid = "test_app",
        xwhat = "install",
        "project not found",
    );

    assert!(
        console.contents().contains("server startup log"),
        "expected INFO log on console, got: {:?}",
        console.contents(),
    );

    let ingest_log = console
        .contents()
        .lines()
        .find_map(|line| {
            let value = serde_json::from_str::<serde_json::Value>(line).ok()?;
            (value.get("target")?.as_str()? == "ingest4x::ingest").then_some(value)
        })
        .expect("ingest log should be json");

    assert_eq!(ingest_log["appid"], "test_app");
    assert_eq!(ingest_log["xwhat"], "install");
    assert!(ingest_log.get("body").is_none());
}

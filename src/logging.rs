use crate::settings::Settings;
use anyhow::Result;
use humantime::format_rfc3339_micros;
use serde_json::{Map, Value};
use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::fmt::writer::MakeWriter;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, registry::Registry};

const LOG_DIR: &str = "logs";
const LOG_PREFIX: &str = "ingest4x";

fn should_emit_log(level: &tracing::Level, configured_level: Option<tracing::Level>) -> bool {
    configured_level
        .map(|max_level| *level <= max_level)
        .unwrap_or(false)
}

pub fn init_logging(settings: &Settings) -> Result<()> {
    init_logging_with_console_writers(settings, std::io::stdout, std::io::stdout)
}

pub fn init_logging_with_console_writers<ConsoleWriter, AccessConsoleWriter>(
    settings: &Settings,
    console_writer: ConsoleWriter,
    _console_access_writer: AccessConsoleWriter,
) -> Result<()>
where
    ConsoleWriter: for<'writer> MakeWriter<'writer> + Send + Sync + Clone + 'static,
    AccessConsoleWriter: for<'writer> MakeWriter<'writer> + Send + Sync + Clone + 'static,
{
    let server = &settings.server;

    ensure_log_dir(LOG_DIR)?;
    let (file_writer, file_guard) = tracing_appender::non_blocking(build_rolling_appender(
        LOG_DIR,
        LOG_PREFIX,
        Rotation::DAILY,
        7,
    )?);
    let log_level = server.log_level.as_tracing_level();
    let console_filter = filter_fn(move |meta| should_emit_log(meta.level(), log_level));
    let file_filter = filter_fn(move |meta| should_emit_log(meta.level(), log_level));

    match server.log_format.as_str() {
        "json" => Registry::default()
            .with(JsonLogLayer::new(console_writer).with_filter(console_filter))
            .with(JsonLogLayer::new(file_writer).with_filter(file_filter))
            .try_init()?,
        _ => Registry::default()
            .with(
                fmt::layer()
                    .with_writer(console_writer)
                    .with_filter(console_filter),
            )
            .with(
                fmt::layer()
                    .with_writer(file_writer)
                    .with_filter(file_filter),
            )
            .try_init()?,
    }

    set_logging_guard(file_guard);
    Ok(())
}

#[derive(Clone)]
struct JsonLogLayer<W> {
    writer: Arc<W>,
}

impl<W> JsonLogLayer<W> {
    fn new(writer: W) -> Self {
        Self {
            writer: Arc::new(writer),
        }
    }
}

impl<S, W> Layer<S> for JsonLogLayer<W>
where
    S: Subscriber,
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = JsonFieldVisitor::default();
        event.record(&mut fields);

        let metadata = event.metadata();
        let mut line = Map::new();
        line.insert(
            "timestamp".to_string(),
            Value::String(format_rfc3339_micros(SystemTime::now()).to_string()),
        );
        line.insert(
            "level".to_string(),
            Value::String(metadata.level().as_str().to_string()),
        );
        line.insert(
            "target".to_string(),
            Value::String(metadata.target().to_string()),
        );

        for (key, value) in fields.values {
            line.insert(key, value);
        }

        if let Ok(serialized) = serde_json::to_string(&Value::Object(line)) {
            let mut writer = self.writer.make_writer();
            let _ = writer.write_all(serialized.as_bytes());
            let _ = writer.write_all(b"\n");
            let _ = writer.flush();
        }
    }
}

#[derive(Default)]
struct JsonFieldVisitor {
    values: Map<String, Value>,
}

impl JsonFieldVisitor {
    fn insert_string_or_json(&mut self, field: &Field, value: &str) {
        let value =
            serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()));
        self.values.insert(field.name().to_string(), value);
    }
}

impl Visit for JsonFieldVisitor {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.values
            .insert(field.name().to_string(), Value::Bool(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.values
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.values
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.values.insert(
            field.name().to_string(),
            serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number),
        );
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.insert_string_or_json(field, value);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let value = format!("{value:?}");
        let unquoted = value
            .strip_prefix('"')
            .and_then(|text| text.strip_suffix('"'))
            .unwrap_or(value.as_str());
        self.insert_string_or_json(field, unquoted);
    }
}

fn set_logging_guard(guard: WorkerGuard) {
    static GUARD: std::sync::OnceLock<WorkerGuard> = std::sync::OnceLock::new();
    let _ = GUARD.set(guard);
}

fn ensure_log_dir(path: &str) -> std::io::Result<()> {
    fs::create_dir_all(Path::new(path))
}

fn build_rolling_appender(
    log_dir: &str,
    prefix: &str,
    rotation: Rotation,
    max_files: usize,
) -> Result<RollingFileAppender> {
    Ok(RollingFileAppender::builder()
        .rotation(rotation)
        .filename_prefix(prefix)
        .filename_suffix("log")
        .max_log_files(max_files)
        .build(log_dir)?)
}

#[cfg(test)]
mod tests {
    use super::should_emit_log;

    #[test]
    fn log_filter_uses_configured_max_level() {
        assert!(should_emit_log(
            &tracing::Level::INFO,
            Some(tracing::Level::INFO),
        ));
        assert!(!should_emit_log(
            &tracing::Level::DEBUG,
            Some(tracing::Level::INFO),
        ));
    }
}

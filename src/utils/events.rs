use crate::settings::{
    EventRouteSet, EventRouteSettings, EventSinkConfig, EventsSettings, FileSinkRotation,
};
use crate::utils::kafka::KafkaProducer;
use actix_web::web::Data;
use anyhow::{anyhow, Context, Result};
use log::warn;
use rdkafka::config::ClientConfig;
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing_appender::non_blocking::{NonBlocking, NonBlockingBuilder, WorkerGuard};
use tracing_appender::rolling::{self, RollingFileAppender, Rotation};

#[derive(Clone, Copy)]
pub enum EventStatus {
    Valid,
    Invalid,
}

#[derive(Clone)]
pub struct EventSinkState {
    router: Arc<EventRouter>,
}

impl EventSinkState {
    pub async fn send_json<T: Serialize>(
        &self,
        status: EventStatus,
        appid: &str,
        xwhat: &str,
        payload: &T,
    ) -> Result<()> {
        let payload = serde_json::to_vec(payload)?;
        self.router.send(status, appid, xwhat, &payload).await
    }

    pub async fn check_alive(&self) -> Result<()> {
        self.router.check_alive().await
    }
}

pub fn init_event_sinks(settings: &EventsSettings) -> Result<Data<EventSinkState>> {
    Ok(Data::new(EventSinkState {
        router: Arc::new(EventRouter::from_settings(settings)?),
    }))
}

struct EventRouter {
    sinks: HashMap<String, EventSink>,
    valid: EventRouteSet,
    invalid: EventRouteSet,
}

impl EventRouter {
    fn from_settings(settings: &EventsSettings) -> Result<Self> {
        let mut sinks = HashMap::new();

        for (name, config) in &settings.sink {
            sinks.insert(name.clone(), EventSink::from_config(config)?);
        }

        validate_routes("events.valid", &settings.valid, &sinks)?;
        validate_routes("events.invalid", &settings.invalid, &sinks)?;

        Ok(Self {
            sinks,
            valid: settings.valid.clone(),
            invalid: settings.invalid.clone(),
        })
    }

    async fn send(
        &self,
        status: EventStatus,
        appid: &str,
        xwhat: &str,
        payload: &[u8],
    ) -> Result<()> {
        let route_set = match status {
            EventStatus::Valid => &self.valid,
            EventStatus::Invalid => &self.invalid,
        };
        let route = route_set
            .routes
            .iter()
            .find(|route| route.matches(appid, xwhat))
            .ok_or_else(|| anyhow!("no event route matched appid={appid} xwhat={xwhat}"))?;
        let ack: HashSet<&str> = route.ack.iter().map(String::as_str).collect();
        let mut ack_errors = Vec::new();

        for sink_name in &route.sinks {
            let Some(sink) = self.sinks.get(sink_name) else {
                continue;
            };

            if let Err(err) = sink.send(payload).await {
                if ack.contains(sink_name.as_str()) {
                    ack_errors.push(format!("{sink_name}: {err}"));
                } else {
                    warn!("non-ack event sink `{sink_name}` failed: {err}");
                }
            }
        }

        if ack_errors.is_empty() {
            Ok(())
        } else {
            Err(anyhow!("ack event sinks failed: {}", ack_errors.join("; ")))
        }
    }

    async fn check_alive(&self) -> Result<()> {
        for (name, sink) in &self.sinks {
            sink.check_alive()
                .await
                .with_context(|| format!("event sink `{name}` is not alive"))?;
        }
        Ok(())
    }
}

enum EventSink {
    Kafka {
        producer: KafkaProducer,
        topic: String,
    },
    File {
        path: PathBuf,
        rotation: FileSinkRotation,
        retention_files: usize,
        writer: NonBlocking,
        _guard: WorkerGuard,
    },
    Stdout,
}

impl EventSink {
    fn from_config(config: &EventSinkConfig) -> Result<Self> {
        match config {
            EventSinkConfig::Kafka {
                bootstrap_servers,
                topic,
                delivery_timeout_ms,
                queue_buffering_max_ms,
                batch_num_messages,
                queue_buffering_max_messages,
                linger_ms,
            } => {
                let producer = KafkaProducer::new(
                    ClientConfig::new()
                        .set("bootstrap.servers", bootstrap_servers)
                        .set("queue.buffering.max.ms", queue_buffering_max_ms)
                        .set("delivery.timeout.ms", delivery_timeout_ms)
                        .set("batch.num.messages", batch_num_messages)
                        .set("queue.buffering.max.messages", queue_buffering_max_messages)
                        .set("linger.ms", linger_ms)
                        .set("compression.type", "snappy")
                        .clone(),
                );
                Ok(Self::Kafka {
                    producer,
                    topic: topic.clone(),
                })
            }
            EventSinkConfig::File {
                path,
                rotation,
                retention_files,
                lossy,
                buffered_lines_limit,
                ..
            } => {
                let path = PathBuf::from(path);
                let (writer, guard) = build_file_event_writer(
                    &path,
                    *rotation,
                    *retention_files,
                    *lossy,
                    *buffered_lines_limit,
                )?;
                Ok(Self::File {
                    path,
                    rotation: *rotation,
                    retention_files: *retention_files,
                    writer,
                    _guard: guard,
                })
            }
            EventSinkConfig::Stdout => Ok(Self::Stdout),
        }
    }

    async fn send(&self, payload: &[u8]) -> Result<()> {
        match self {
            Self::Kafka { producer, topic } => producer.send_value(topic, payload.to_vec()).await,
            Self::File { writer, .. } => {
                let mut line = Vec::with_capacity(payload.len() + 1);
                line.extend_from_slice(payload);
                line.push(b'\n');

                let mut writer = writer.clone();
                writer.write_all(&line)?;
                Ok(())
            }
            Self::Stdout => {
                let payload = serde_json::from_slice::<Value>(payload)
                    .map(|value| value.to_string())
                    .unwrap_or_else(|_| String::from_utf8_lossy(payload).into_owned());
                println!("{payload}");
                Ok(())
            }
        }
    }

    async fn check_alive(&self) -> Result<()> {
        match self {
            Self::Kafka { producer, .. } => producer
                .check_alive()
                .await
                .map_err(|error| anyhow::Error::from(error)),
            Self::File {
                path,
                rotation,
                retention_files,
                ..
            } => {
                let _ = build_rolling_file_appender(path, *rotation, *retention_files)?;
                Ok(())
            }
            Self::Stdout => Ok(()),
        }
    }
}

fn build_file_event_writer(
    path: &Path,
    rotation: FileSinkRotation,
    retention_files: usize,
    lossy: bool,
    buffered_lines_limit: usize,
) -> Result<(NonBlocking, WorkerGuard)> {
    let appender = build_rolling_file_appender(path, rotation, retention_files)?;
    Ok(NonBlockingBuilder::default()
        .lossy(lossy)
        .buffered_lines_limit(buffered_lines_limit)
        .thread_name("ingest4x-file-event-sink")
        .finish(appender))
}

fn build_rolling_file_appender(
    path: &Path,
    rotation: FileSinkRotation,
    retention_files: usize,
) -> Result<RollingFileAppender> {
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .filter(|file_name| !file_name.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "file event sink path must include a file name: {}",
                path.display()
            )
        })?;

    std::fs::create_dir_all(directory)?;
    if rotation == FileSinkRotation::Never {
        return Ok(rolling::never(directory, file_name));
    }

    Ok(RollingFileAppender::builder()
        .rotation(to_tracing_rotation(rotation))
        .filename_prefix(file_name.to_string_lossy())
        .max_log_files(retention_files)
        .build(directory)?)
}

const fn to_tracing_rotation(rotation: FileSinkRotation) -> Rotation {
    match rotation {
        FileSinkRotation::Never => Rotation::NEVER,
        FileSinkRotation::Minutely => Rotation::MINUTELY,
        FileSinkRotation::Hourly => Rotation::HOURLY,
        FileSinkRotation::Daily => Rotation::DAILY,
        FileSinkRotation::Weekly => Rotation::WEEKLY,
    }
}

fn validate_routes(
    name: &str,
    route_set: &EventRouteSet,
    sinks: &HashMap<String, EventSink>,
) -> Result<()> {
    for (index, route) in route_set.routes.iter().enumerate() {
        if route.sinks.is_empty() {
            return Err(anyhow!(
                "{name}.routes[{index}] must declare at least one sink"
            ));
        }

        for sink_name in route.sinks.iter().chain(route.ack.iter()) {
            if !sinks.contains_key(sink_name) {
                return Err(anyhow!(
                    "{name}.routes[{index}] references unknown sink `{sink_name}`"
                ));
            }
        }

        for ack_name in &route.ack {
            if !route.sinks.contains(ack_name) {
                return Err(anyhow!(
                    "{name}.routes[{index}] ack sink `{ack_name}` must also be listed in sinks"
                ));
            }
        }
    }

    Ok(())
}

trait EventRouteMatch {
    fn matches(&self, appid: &str, xwhat: &str) -> bool;
}

impl EventRouteMatch for EventRouteSettings {
    fn matches(&self, appid: &str, xwhat: &str) -> bool {
        self.appid
            .as_ref()
            .map(|values| values.iter().any(|value| value == appid))
            .unwrap_or(true)
            && self
                .xwhat
                .as_ref()
                .map(|values| values.iter().any(|value| value == xwhat))
                .unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use super::{init_event_sinks, EventStatus};
    use crate::settings::{
        default_file_sink_buffered_lines_limit, default_file_sink_retention_files, EventRouteSet,
        EventRouteSettings, EventSinkConfig, EventsSettings, FileSinkRotation,
    };
    use serde_json::json;
    use std::collections::HashMap;
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;

    #[tokio::test]
    async fn routes_valid_events_by_appid_and_xwhat_before_fallback() {
        let temp = tempdir().expect("temp dir");
        let payment_path = temp.path().join("payment.jsonl");
        let default_path = temp.path().join("default.jsonl");
        let settings = EventsSettings {
            sink: HashMap::from([
                (
                    "file_payment".to_string(),
                    EventSinkConfig::File {
                        path: payment_path.display().to_string(),
                        format: Default::default(),
                        rotation: FileSinkRotation::Never,
                        retention_files: default_file_sink_retention_files(),
                        lossy: false,
                        buffered_lines_limit: default_file_sink_buffered_lines_limit(),
                    },
                ),
                (
                    "file_default".to_string(),
                    EventSinkConfig::File {
                        path: default_path.display().to_string(),
                        format: Default::default(),
                        rotation: FileSinkRotation::Never,
                        retention_files: default_file_sink_retention_files(),
                        lossy: false,
                        buffered_lines_limit: default_file_sink_buffered_lines_limit(),
                    },
                ),
            ]),
            valid: EventRouteSet {
                routes: vec![
                    EventRouteSettings {
                        appid: Some(vec!["game-a".to_string()]),
                        xwhat: Some(vec!["payment".to_string()]),
                        sinks: vec!["file_payment".to_string()],
                        ack: vec!["file_payment".to_string()],
                    },
                    EventRouteSettings {
                        sinks: vec!["file_default".to_string()],
                        ack: vec!["file_default".to_string()],
                        ..Default::default()
                    },
                ],
            },
            invalid: EventRouteSet::default(),
        };
        let sinks = init_event_sinks(&settings).expect("event sinks should initialize");

        sinks
            .send_json(
                EventStatus::Valid,
                "game-a",
                "payment",
                &json!({"id": "payment"}),
            )
            .await
            .expect("payment event should route");
        sinks
            .send_json(
                EventStatus::Valid,
                "game-a",
                "startup",
                &json!({"id": "startup"}),
            )
            .await
            .expect("fallback event should route");

        assert_eq!(
            read_file_eventually(&payment_path),
            "{\"id\":\"payment\"}\n"
        );
        assert_eq!(
            read_file_eventually(&default_path),
            "{\"id\":\"startup\"}\n"
        );
    }

    fn read_file_eventually(path: &std::path::Path) -> String {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Ok(content) = std::fs::read_to_string(path) {
                if !content.is_empty() {
                    return content;
                }
            }

            if Instant::now() >= deadline {
                return std::fs::read_to_string(path).expect("event file");
            }

            thread::sleep(Duration::from_millis(20));
        }
    }
}

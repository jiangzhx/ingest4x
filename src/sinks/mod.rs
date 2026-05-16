use crate::repositories::{DeliveryTargetType, RuntimeEventSink};
use anyhow::Result;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod blackhole;
pub mod kafka;
pub mod parquet;
pub mod state;
pub mod stdout;

pub use state::{init_event_sinks_from_runtime_sinks, EventSinkState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkTypeMetadata {
    pub target_type: &'static str,
    pub label: &'static str,
}

pub trait EventSink: Send + Sync {
    type DeliveryTargetConfig: SinkConfig;
    type EventSinkConfig: SinkConfig;

    fn from_config(
        target_config: Self::DeliveryTargetConfig,
        sink_config: Self::EventSinkConfig,
    ) -> Result<Self>
    where
        Self: Sized;

    fn from_config_with_context(
        target_config: Self::DeliveryTargetConfig,
        sink_config: Self::EventSinkConfig,
        _context: EventSinkBuildContext<'_>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        Self::from_config(target_config, sink_config)
    }

    /// Writes the events to this sink and returns only after the sink-defined
    /// reliable commit point has been reached.
    ///
    /// WAL replay can advance the pipeline checkpoint after all emitted sinks return `Ok`.
    /// Implementations must return an error for partial, temporary, or
    /// uncommitted downstream states.
    fn send_batch<'a>(&'a self, events: &'a [Value]) -> BoxFuture<'a, Result<()>>;

    fn send_batch_with_metadata<'a>(
        &'a self,
        batch: EventSinkBatch<'a>,
    ) -> BoxFuture<'a, Result<()>> {
        self.send_batch(batch.events)
    }

    fn check_alive(&self) -> BoxFuture<'_, Result<()>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventSinkBatchMetadata {
    pub node_id: String,
    pub lsn_start: u64,
    pub lsn_end: u64,
}

#[derive(Clone, Debug)]
pub struct EventSinkBatch<'a> {
    pub events: &'a [Value],
    pub metadata: Option<EventSinkBatchMetadata>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventSinkBuildContext<'a> {
    pub sink_id: &'a str,
}

pub trait EventSinkRuntime: Send + Sync {
    /// See [`EventSink::send_batch`] for the checkpoint and commit contract.
    fn send_batch<'a>(&'a self, events: &'a [Value]) -> BoxFuture<'a, Result<()>>;

    fn send_batch_with_metadata<'a>(
        &'a self,
        batch: EventSinkBatch<'a>,
    ) -> BoxFuture<'a, Result<()>>;

    fn check_alive(&self) -> BoxFuture<'_, Result<()>>;
}

impl<T> EventSinkRuntime for T
where
    T: EventSink,
{
    fn send_batch<'a>(&'a self, events: &'a [Value]) -> BoxFuture<'a, Result<()>> {
        EventSink::send_batch(self, events)
    }

    fn send_batch_with_metadata<'a>(
        &'a self,
        batch: EventSinkBatch<'a>,
    ) -> BoxFuture<'a, Result<()>> {
        EventSink::send_batch_with_metadata(self, batch)
    }

    fn check_alive(&self) -> BoxFuture<'_, Result<()>> {
        EventSink::check_alive(self)
    }
}

pub trait EventSinkProvider: Send + Sync {
    type Sink: EventSink + 'static;

    fn sink_type(&self) -> SinkTypeMetadata;

    fn normalize_delivery_target_config(&self, config_json: Value) -> Result<String, String> {
        <Self::Sink as EventSink>::DeliveryTargetConfig::normalize(config_json)
    }

    fn normalize_event_sink_config(&self, config_json: Value) -> Result<String, String> {
        <Self::Sink as EventSink>::EventSinkConfig::normalize(config_json)
    }
}

pub trait SinkConfig: DeserializeOwned + Serialize {
    fn validate(&self) -> Result<(), String> {
        Ok(())
    }

    fn parse(config_json: Value) -> Result<Self, String>
    where
        Self: Sized,
    {
        let config =
            serde_json::from_value::<Self>(config_json).map_err(|error| error.to_string())?;
        config.validate()?;
        Ok(config)
    }

    fn normalize(config_json: Value) -> Result<String, String>
    where
        Self: Sized,
    {
        let config = Self::parse(config_json)?;
        serde_json::to_string(&config).map_err(|error| error.to_string())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyConfig {}

impl SinkConfig for EmptyConfig {}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EventSinkBatchConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_events: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
}

impl EventSinkBatchConfig {
    fn validate(&self) -> Result<(), String> {
        if self.max_events == Some(0) {
            return Err("batch.max_events must be greater than 0".to_string());
        }
        if self.max_bytes == Some(0) {
            return Err("batch.max_bytes must be greater than 0".to_string());
        }
        if let Some(timeout) = &self.timeout {
            humantime::parse_duration(timeout)
                .map_err(|error| format!("batch.timeout invalid duration: {error}"))?;
        }
        Ok(())
    }
}

trait ErasedEventSinkProvider: Send + Sync {
    fn sink_type(&self) -> SinkTypeMetadata;

    fn normalize_delivery_target_config(&self, config_json: Value) -> Result<String, String>;

    fn normalize_event_sink_config(&self, config_json: Value) -> Result<String, String>;

    fn build_sink(&self, sink: &RuntimeEventSink) -> Result<Box<dyn EventSinkRuntime>>;
}

impl<T> ErasedEventSinkProvider for T
where
    T: EventSinkProvider,
{
    fn sink_type(&self) -> SinkTypeMetadata {
        EventSinkProvider::sink_type(self)
    }

    fn normalize_delivery_target_config(&self, config_json: Value) -> Result<String, String> {
        EventSinkProvider::normalize_delivery_target_config(self, config_json)
    }

    fn normalize_event_sink_config(&self, config_json: Value) -> Result<String, String> {
        let (sink_config_json, batch_config) = split_event_sink_batch_config(config_json)?;
        let normalized = EventSinkProvider::normalize_event_sink_config(self, sink_config_json)?;
        append_event_sink_batch_config(normalized, batch_config)
    }

    fn build_sink(&self, sink: &RuntimeEventSink) -> Result<Box<dyn EventSinkRuntime>> {
        let target_config =
            <T::Sink as EventSink>::DeliveryTargetConfig::parse(sink.target.config_json.clone())
                .map_err(anyhow::Error::msg)?;
        let (destination_json, _) = split_event_sink_batch_config(sink.destination_json.clone())
            .map_err(anyhow::Error::msg)?;
        let sink_config = <T::Sink as EventSink>::EventSinkConfig::parse(destination_json)
            .map_err(anyhow::Error::msg)?;

        Ok(Box::new(T::Sink::from_config_with_context(
            target_config,
            sink_config,
            EventSinkBuildContext {
                sink_id: sink.sink_id.as_str(),
            },
        )?))
    }
}

pub fn event_sink_batch_config(
    destination_json: &Value,
) -> Result<Option<EventSinkBatchConfig>, String> {
    let Some(batch_json) = destination_json.get("batch") else {
        return Ok(None);
    };
    let batch_config = serde_json::from_value::<EventSinkBatchConfig>(batch_json.clone())
        .map_err(|error| error.to_string())?;
    batch_config.validate()?;
    Ok(Some(batch_config))
}

fn split_event_sink_batch_config(
    config_json: Value,
) -> Result<(Value, Option<EventSinkBatchConfig>), String> {
    let Value::Object(mut object) = config_json else {
        return Ok((config_json, None));
    };
    let batch_json = object.remove("batch");
    let batch_config = match batch_json {
        Some(batch_json) => {
            let batch_config = serde_json::from_value::<EventSinkBatchConfig>(batch_json)
                .map_err(|error| error.to_string())?;
            batch_config.validate()?;
            Some(batch_config)
        }
        None => None,
    };
    Ok((Value::Object(object), batch_config))
}

fn append_event_sink_batch_config(
    normalized: String,
    batch_config: Option<EventSinkBatchConfig>,
) -> Result<String, String> {
    let Some(batch_config) = batch_config else {
        return Ok(normalized);
    };
    let mut normalized_json =
        serde_json::from_str::<Value>(&normalized).map_err(|error| error.to_string())?;
    let Value::Object(object) = &mut normalized_json else {
        return Err("event sink config must normalize to a JSON object".to_string());
    };
    let batch_json = serde_json::to_value(batch_config).map_err(|error| error.to_string())?;
    object.insert("batch".to_string(), batch_json);
    serde_json::to_string(&normalized_json).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{provider_for, SinkTypeMetadata};
    use serde_json::json;

    #[test]
    fn kafka_provider_exposes_metadata_and_normalizes_json_configs() {
        let provider = provider_for("kafka").expect("kafka provider should be registered");

        assert_eq!(
            provider.sink_type(),
            SinkTypeMetadata {
                target_type: "kafka",
                label: "Kafka",
            }
        );
        assert_eq!(
            provider
                .normalize_delivery_target_config(json!({
                    "bootstrap_servers": "127.0.0.1:9092"
                }))
                .expect("target config should normalize"),
            r#"{"bootstrap_servers":"127.0.0.1:9092","delivery_timeout_ms":"3000","queue_buffering_max_ms":"0","batch_num_messages":"100","queue_buffering_max_messages":"300","linger_ms":"100"}"#
        );
        assert_eq!(
            provider
                .normalize_event_sink_config(json!({
                    "topic": "events"
                }))
                .expect("destination should normalize"),
            r#"{"topic":"events"}"#
        );
        assert!(provider
            .normalize_event_sink_config(json!({
                "table": "events"
            }))
            .is_err());
    }

    #[test]
    fn blackhole_provider_exposes_metadata_and_normalizes_json_configs() {
        let provider = provider_for("blackhole").expect("blackhole provider should be registered");

        assert_eq!(
            provider.sink_type(),
            SinkTypeMetadata {
                target_type: "blackhole",
                label: "Blackhole",
            }
        );
        assert_eq!(
            provider
                .normalize_delivery_target_config(json!({}))
                .expect("target config should normalize"),
            r#"{}"#
        );
        assert_eq!(
            provider
                .normalize_event_sink_config(json!({
                    "mode": "slow",
                    "delay_ms": 25
                }))
                .expect("destination config should normalize"),
            r#"{"mode":"slow","delay_ms":25}"#
        );
        assert!(provider
            .normalize_event_sink_config(json!({
                "mode": "slow",
                "delay_ms": 25,
                "unknown": true
            }))
            .is_err());
    }

    #[test]
    fn parquet_provider_exposes_metadata_and_normalizes_json_configs() {
        let provider = provider_for("parquet").expect("parquet provider should be registered");

        assert_eq!(
            provider.sink_type(),
            SinkTypeMetadata {
                target_type: "parquet",
                label: "Parquet",
            }
        );
        assert_eq!(
            provider
                .normalize_delivery_target_config(json!({
                    "scheme": "fs",
                    "options": {
                        "root": "/tmp/ingest4x-parquet"
                    }
                }))
                .expect("target config should normalize"),
            r#"{"scheme":"fs","options":{"root":"/tmp/ingest4x-parquet"}}"#
        );
        assert_eq!(
            provider
                .normalize_event_sink_config(json!({
                    "path_prefix": "events",
                    "columns": [
                        {
                            "name": "appid",
                            "path": "appid",
                            "type": "string"
                        },
                        {
                            "name": "currencyamount",
                            "path": "xcontext.currencyamount",
                            "type": "number",
                            "nullable": true
                        }
                    ]
                }))
                .expect("destination should normalize"),
            r#"{"path_prefix":"events","columns":[{"name":"appid","path":"appid","type":"string","nullable":false},{"name":"currencyamount","path":"xcontext.currencyamount","type":"number","nullable":true}],"include_event_json":true}"#
        );
        assert!(provider
            .normalize_event_sink_config(json!({
                "path_prefix": "events",
                "unknown": true
            }))
            .is_err());
    }

    #[test]
    fn event_sink_config_accepts_common_batch_override() {
        let provider = provider_for("parquet").expect("parquet provider should be registered");

        let normalized = provider
            .normalize_event_sink_config(json!({
                "path_prefix": "events",
                "batch": {
                    "max_events": 2,
                    "timeout": "5s"
                },
                "columns": [
                    {
                        "name": "installid",
                        "path": "xcontext.installid",
                        "type": "string"
                    }
                ]
            }))
            .expect("destination should normalize with batch override");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&normalized).expect("normalized json"),
            json!({
                "path_prefix": "events",
                "columns": [
                    {
                        "name": "installid",
                        "path": "xcontext.installid",
                        "type": "string",
                        "nullable": false
                    }
                ],
                "include_event_json": true,
                "batch": {
                    "max_events": 2,
                    "timeout": "5s"
                }
            })
        );
    }
}

pub fn normalize_delivery_target_config(
    target_type: &DeliveryTargetType,
    config_json: Value,
) -> Result<String, String> {
    provider_for(target_type.as_str())
        .ok_or_else(|| format!("unknown delivery target type `{}`", target_type.as_str()))?
        .normalize_delivery_target_config(config_json)
}

pub fn normalize_event_sink_config(
    target_type: &DeliveryTargetType,
    destination_json: Value,
) -> Result<String, String> {
    provider_for(target_type.as_str())
        .ok_or_else(|| format!("unknown delivery target type `{}`", target_type.as_str()))?
        .normalize_event_sink_config(destination_json)
}

pub fn build_sink(sink: &RuntimeEventSink) -> Result<Box<dyn EventSinkRuntime>> {
    provider_for(sink.target.target_type.as_str())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "unknown delivery target type `{}`",
                sink.target.target_type.as_str()
            )
        })?
        .build_sink(sink)
}

pub fn registered_sink_types() -> &'static [SinkTypeMetadata] {
    static TYPES: std::sync::OnceLock<Vec<SinkTypeMetadata>> = std::sync::OnceLock::new();
    TYPES.get_or_init(|| {
        providers()
            .iter()
            .map(|provider| provider.sink_type())
            .collect()
    })
}

pub fn is_registered_sink_type(target_type: &str) -> bool {
    provider_for(target_type).is_some()
}

fn provider_for(target_type: &str) -> Option<&'static dyn ErasedEventSinkProvider> {
    providers()
        .iter()
        .copied()
        .find(|provider| provider.sink_type().target_type == target_type)
}

fn providers() -> &'static [&'static dyn ErasedEventSinkProvider] {
    static PROVIDERS: [&dyn ErasedEventSinkProvider; 4] = [
        &blackhole::PROVIDER,
        &kafka::PROVIDER,
        &parquet::PROVIDER,
        &stdout::PROVIDER,
    ];
    &PROVIDERS
}

fn validate_required_string(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} must not be empty"));
    }

    Ok(())
}

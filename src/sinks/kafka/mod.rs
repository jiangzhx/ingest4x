use crate::sinks::{
    validate_required_string, EventSink, EventSinkProvider, SinkConfig, SinkTypeMetadata,
};
use anyhow::Result;
use futures::future::BoxFuture;
use rdkafka::config::ClientConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod producer;

use producer::KafkaProducer;

pub static PROVIDER: KafkaProvider = KafkaProvider;

pub struct KafkaProvider;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub bootstrap_servers: String,
    #[serde(default = "default_kafka_delivery_timeout_ms")]
    pub delivery_timeout_ms: String,
    #[serde(default = "default_kafka_queue_buffering_max_ms")]
    pub queue_buffering_max_ms: String,
    #[serde(default = "default_kafka_batch_num_messages")]
    pub batch_num_messages: String,
    #[serde(default = "default_kafka_queue_buffering_max_messages")]
    pub queue_buffering_max_messages: String,
    #[serde(default = "default_kafka_linger_ms")]
    pub linger_ms: String,
}

impl SinkConfig for TargetConfig {
    fn validate(&self) -> Result<(), String> {
        validate_required_string("bootstrap_servers", &self.bootstrap_servers)?;
        validate_required_string("delivery_timeout_ms", &self.delivery_timeout_ms)?;
        validate_required_string("queue_buffering_max_ms", &self.queue_buffering_max_ms)?;
        validate_required_string("batch_num_messages", &self.batch_num_messages)?;
        validate_required_string(
            "queue_buffering_max_messages",
            &self.queue_buffering_max_messages,
        )?;
        validate_required_string("linger_ms", &self.linger_ms)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationConfig {
    pub topic: String,
}

impl SinkConfig for DestinationConfig {
    fn validate(&self) -> Result<(), String> {
        validate_required_string("topic", &self.topic)
    }

    fn parse(config_json: Value) -> Result<Self, String> {
        if config_json.get("topic").is_none() {
            return Err("missing field `topic`".to_string());
        }

        let config =
            serde_json::from_value::<Self>(config_json).map_err(|error| error.to_string())?;
        config.validate()?;
        Ok(config)
    }
}

pub struct KafkaSink {
    producer: KafkaProducer,
    topic: String,
}

impl KafkaSink {
    fn from_parts(target_config: TargetConfig, sink_config: DestinationConfig) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &target_config.bootstrap_servers)
            .set(
                "queue.buffering.max.ms",
                &target_config.queue_buffering_max_ms,
            )
            .set("delivery.timeout.ms", &target_config.delivery_timeout_ms)
            .set("batch.num.messages", &target_config.batch_num_messages)
            .set(
                "queue.buffering.max.messages",
                &target_config.queue_buffering_max_messages,
            )
            .set("linger.ms", &target_config.linger_ms)
            .set("compression.type", "snappy")
            .clone();

        Ok(Self {
            producer: KafkaProducer::new(producer)?,
            topic: sink_config.topic,
        })
    }
}

impl EventSink for KafkaSink {
    type DeliveryTargetConfig = TargetConfig;
    type EventSinkConfig = DestinationConfig;

    fn from_config(target_config: TargetConfig, sink_config: DestinationConfig) -> Result<Self> {
        Self::from_parts(target_config, sink_config)
    }

    fn send_batch<'a>(&'a self, events: &'a [Value]) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            for event in events {
                let payload = serde_json::to_vec(event)?;
                self.producer.send_value(&self.topic, payload).await?;
            }
            Ok(())
        })
    }

    fn check_alive(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.producer.check_alive().await })
    }
}

impl EventSinkProvider for KafkaProvider {
    type Sink = KafkaSink;

    fn sink_type(&self) -> SinkTypeMetadata {
        SinkTypeMetadata {
            target_type: "kafka",
            label: "Kafka",
        }
    }
}

pub fn default_kafka_delivery_timeout_ms() -> String {
    "3000".to_string()
}

pub fn default_kafka_queue_buffering_max_ms() -> String {
    "0".to_string()
}

pub fn default_kafka_batch_num_messages() -> String {
    "100".to_string()
}

pub fn default_kafka_queue_buffering_max_messages() -> String {
    "300".to_string()
}

pub fn default_kafka_linger_ms() -> String {
    "100".to_string()
}

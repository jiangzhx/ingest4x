use crate::repositories::{DeliveryTargetType, EventSinkRepository, RuntimeEventSink};
use crate::rhai_ctx::ProcessorDelivery;
use crate::settings::{AutoOffsetReset, EventSinkConfig, EventsSettings};
use crate::utils::kafka::KafkaProducer;
use actix_web::web::Data;
use anyhow::{anyhow, Context, Result};
use futures::lock::Mutex as AsyncMutex;
use rdkafka::config::ClientConfig;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Clone)]
pub struct EventSinkState {
    router: Arc<RwLock<Arc<EventRouter>>>,
    repository: Option<EventSinkRepository>,
    version: Arc<AtomicU64>,
    refresh_lock: Arc<AsyncMutex<()>>,
}

impl EventSinkState {
    pub async fn send_deliveries(&self, deliveries: &[ProcessorDelivery]) -> Result<()> {
        let router = self.current_router();
        router.send_deliveries(deliveries).await
    }

    pub async fn send_delivery(&self, delivery: &ProcessorDelivery) -> Result<()> {
        let router = self.current_router();
        router.send_delivery(delivery).await
    }

    pub fn sink_names(&self) -> Vec<String> {
        self.current_router().sink_names()
    }

    pub fn contains_sink(&self, name: &str) -> bool {
        self.current_router().contains_sink(name)
    }

    pub fn auto_offset_reset(&self, name: &str) -> Option<AutoOffsetReset> {
        self.current_router().auto_offset_reset(name)
    }

    pub async fn check_alive(&self) -> Result<()> {
        let router = self.current_router();
        router.check_alive().await
    }

    pub async fn load(repository: EventSinkRepository) -> Result<Self> {
        let (router, version) = load_router_snapshot(&repository).await?;

        Ok(Self {
            router: Arc::new(RwLock::new(Arc::new(router))),
            repository: Some(repository),
            version: Arc::new(AtomicU64::new(version)),
            refresh_lock: Arc::new(AsyncMutex::new(())),
        })
    }

    pub async fn refresh_if_needed(&self) -> Result<bool> {
        let Some(repository) = self.repository.as_ref() else {
            return Ok(false);
        };

        let _guard = self.refresh_lock.lock().await;
        let current_version = self.version.load(Ordering::Acquire);
        let latest_version = repository.event_sinks_version().await?;

        if latest_version == current_version {
            return Ok(false);
        }

        let (router, version) = load_router_snapshot(repository).await?;
        if version <= self.version.load(Ordering::Acquire) {
            return Ok(false);
        }

        let mut guard = self
            .router
            .write()
            .expect("event sink router write lock poisoned");
        *guard = Arc::new(router);
        self.version.store(version, Ordering::Release);

        Ok(true)
    }

    fn current_router(&self) -> Arc<EventRouter> {
        self.router
            .read()
            .expect("event sink router read lock poisoned")
            .clone()
    }
}

pub fn init_event_sinks(settings: &EventsSettings) -> Result<Data<EventSinkState>> {
    Ok(Data::new(EventSinkState {
        router: Arc::new(RwLock::new(Arc::new(EventRouter::from_settings(settings)?))),
        repository: None,
        version: Arc::new(AtomicU64::new(0)),
        refresh_lock: Arc::new(AsyncMutex::new(())),
    }))
}

async fn load_router_snapshot(repository: &EventSinkRepository) -> Result<(EventRouter, u64)> {
    loop {
        let version_before = repository.event_sinks_version().await?;
        let runtime_sinks = repository.list_enabled_runtime_sinks().await?;
        let version_after = repository.event_sinks_version().await?;

        if version_before == version_after {
            return Ok((
                EventRouter::from_runtime_sinks(runtime_sinks)?,
                version_after,
            ));
        }
    }
}

struct EventRouter {
    sinks: HashMap<String, EventSinkEntry>,
}

struct EventSinkEntry {
    sink: EventSink,
    auto_offset_reset: AutoOffsetReset,
}

impl EventRouter {
    fn from_settings(settings: &EventsSettings) -> Result<Self> {
        let mut sinks = HashMap::new();

        for (name, config) in &settings.sink {
            sinks.insert(
                name.clone(),
                EventSinkEntry {
                    sink: EventSink::from_config(config)?,
                    auto_offset_reset: config.auto_offset_reset(),
                },
            );
        }

        Ok(Self { sinks })
    }

    fn from_runtime_sinks(runtime_sinks: Vec<RuntimeEventSink>) -> Result<Self> {
        let mut sinks = HashMap::new();

        for runtime_sink in runtime_sinks {
            sinks.insert(
                runtime_sink.sink_id.clone(),
                EventSinkEntry {
                    auto_offset_reset: runtime_sink.auto_offset_reset,
                    sink: EventSink::from_runtime_sink(&runtime_sink)?,
                },
            );
        }

        Ok(Self { sinks })
    }

    async fn send_deliveries(&self, deliveries: &[ProcessorDelivery]) -> Result<()> {
        let mut sinks = Vec::with_capacity(deliveries.len());
        for delivery in deliveries {
            if delivery.target.trim().is_empty() {
                tracing::warn!("processor delivery ignored empty sink target");
                continue;
            }
            let sink = self.sinks.get(&delivery.target).or_else(|| {
                tracing::warn!(
                    target = delivery.target.as_str(),
                    "processor delivery ignored unknown sink target"
                );
                None
            });
            let Some(sink) = sink else {
                continue;
            };
            let payload = serde_json::to_vec(&delivery.event)?;
            sinks.push((delivery.target.as_str(), &sink.sink, payload));
        }

        for (target, sink, payload) in sinks {
            sink.send(&payload)
                .await
                .with_context(|| format!("event sink `{target}` failed"))?;
        }

        Ok(())
    }

    async fn send_delivery(&self, delivery: &ProcessorDelivery) -> Result<()> {
        let sink = self
            .sinks
            .get(&delivery.target)
            .ok_or_else(|| anyhow!("unknown event sink target `{}`", delivery.target))?;
        let payload = serde_json::to_vec(&delivery.event)?;
        sink.sink
            .send(&payload)
            .await
            .with_context(|| format!("event sink `{}` failed", delivery.target))
    }

    fn sink_names(&self) -> Vec<String> {
        let mut names = self.sinks.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    fn contains_sink(&self, name: &str) -> bool {
        self.sinks.contains_key(name)
    }

    fn auto_offset_reset(&self, name: &str) -> Option<AutoOffsetReset> {
        self.sinks.get(name).map(|entry| entry.auto_offset_reset)
    }

    async fn check_alive(&self) -> Result<()> {
        for (name, sink) in &self.sinks {
            sink.sink
                .check_alive()
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
    Stdout,
}

impl EventSink {
    fn from_config(config: &EventSinkConfig) -> Result<Self> {
        match config {
            EventSinkConfig::Kafka {
                bootstrap_servers,
                topic,
                auto_offset_reset: _,
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
            EventSinkConfig::Stdout {
                auto_offset_reset: _,
            } => Ok(Self::Stdout),
        }
    }

    async fn send(&self, payload: &[u8]) -> Result<()> {
        match self {
            Self::Kafka { producer, topic } => producer.send_value(topic, payload.to_vec()).await,
            Self::Stdout => {
                let payload = serde_json::from_slice::<Value>(payload)
                    .map(|value| value.to_string())
                    .unwrap_or_else(|_| String::from_utf8_lossy(payload).into_owned());
                println!("{payload}");
                Ok(())
            }
        }
    }

    fn from_runtime_sink(sink: &RuntimeEventSink) -> Result<Self> {
        match sink.target.target_type {
            DeliveryTargetType::Kafka => {
                let bootstrap_servers =
                    required_string(&sink.target.config_json, "bootstrap_servers")?;
                let delivery_timeout_ms =
                    required_string(&sink.target.config_json, "delivery_timeout_ms")?;
                let queue_buffering_max_ms =
                    required_string(&sink.target.config_json, "queue_buffering_max_ms")?;
                let batch_num_messages =
                    required_string(&sink.target.config_json, "batch_num_messages")?;
                let queue_buffering_max_messages =
                    required_string(&sink.target.config_json, "queue_buffering_max_messages")?;
                let linger_ms = required_string(&sink.target.config_json, "linger_ms")?;
                let topic = required_string(&sink.destination_json, "topic")?;

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
                    topic: topic.to_string(),
                })
            }
            DeliveryTargetType::Stdout => Ok(Self::Stdout),
        }
    }

    async fn check_alive(&self) -> Result<()> {
        match self {
            Self::Kafka { producer, .. } => producer
                .check_alive()
                .await
                .map_err(|error| anyhow::Error::from(error)),
            Self::Stdout => Ok(()),
        }
    }
}

fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("missing required string field `{field}`"))
}

#[cfg(test)]
mod tests {
    use super::init_event_sinks;
    use crate::rhai_ctx::ProcessorDelivery;
    use crate::settings::{AutoOffsetReset, EventSinkConfig, EventsSettings};
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::DefaultProducerContext;
    use rdkafka::{ClientConfig, Message};
    use serde_json::json;
    use std::collections::HashMap;

    #[tokio::test]
    async fn sends_processed_event_to_all_declared_targets() {
        let kafka = create_kafka_cluster(&["raw-events", "payment-events"]);
        let raw_consumer = create_consumer(&kafka, "raw-target", "raw-events");
        let payment_consumer = create_consumer(&kafka, "payment-target", "payment-events");
        let settings = EventsSettings {
            sink: HashMap::from([
                (
                    "kafka_raw".to_string(),
                    EventSinkConfig::Kafka {
                        bootstrap_servers: kafka.bootstrap_servers.clone(),
                        topic: "raw-events".to_string(),
                        auto_offset_reset: AutoOffsetReset::Latest,
                        delivery_timeout_ms: "5000".to_string(),
                        queue_buffering_max_ms: "0".to_string(),
                        batch_num_messages: "1".to_string(),
                        queue_buffering_max_messages: "300".to_string(),
                        linger_ms: "0".to_string(),
                    },
                ),
                (
                    "kafka_payment".to_string(),
                    EventSinkConfig::Kafka {
                        bootstrap_servers: kafka.bootstrap_servers.clone(),
                        topic: "payment-events".to_string(),
                        auto_offset_reset: AutoOffsetReset::Latest,
                        delivery_timeout_ms: "5000".to_string(),
                        queue_buffering_max_ms: "0".to_string(),
                        batch_num_messages: "1".to_string(),
                        queue_buffering_max_messages: "300".to_string(),
                        linger_ms: "0".to_string(),
                    },
                ),
            ]),
        };
        let sinks = init_event_sinks(&settings).expect("event sinks should initialize");

        sinks
            .send_deliveries(&[
                ProcessorDelivery {
                    target: "kafka_raw".to_string(),
                    event: json!({"id": "raw"}),
                },
                ProcessorDelivery {
                    target: "kafka_payment".to_string(),
                    event: json!({"id": "payment"}),
                },
            ])
            .await
            .expect("fan-out targets should send");

        assert_eq!(
            read_message_payload(&raw_consumer).await,
            "{\"id\":\"raw\"}"
        );
        assert_eq!(
            read_message_payload(&payment_consumer).await,
            "{\"id\":\"payment\"}"
        );
    }

    #[tokio::test]
    async fn ignores_unknown_declared_target_and_sends_known_targets() {
        let kafka = create_kafka_cluster(&["raw-events"]);
        let raw_consumer = create_consumer(&kafka, "unknown-target-ignored", "raw-events");
        let settings = EventsSettings {
            sink: HashMap::from([(
                "kafka_raw".to_string(),
                EventSinkConfig::Kafka {
                    bootstrap_servers: kafka.bootstrap_servers.clone(),
                    topic: "raw-events".to_string(),
                    auto_offset_reset: AutoOffsetReset::Latest,
                    delivery_timeout_ms: "5000".to_string(),
                    queue_buffering_max_ms: "0".to_string(),
                    batch_num_messages: "1".to_string(),
                    queue_buffering_max_messages: "300".to_string(),
                    linger_ms: "0".to_string(),
                },
            )]),
        };
        let sinks = init_event_sinks(&settings).expect("event sinks should initialize");

        sinks
            .send_deliveries(&[
                ProcessorDelivery {
                    target: "kafka_raw".to_string(),
                    event: json!({"id": "raw"}),
                },
                ProcessorDelivery {
                    target: "missing_sink".to_string(),
                    event: json!({"id": "payment"}),
                },
            ])
            .await
            .expect("unknown target should be ignored");

        assert_eq!(
            read_message_payload(&raw_consumer).await,
            "{\"id\":\"raw\"}"
        );
    }

    struct TestKafkaCluster {
        bootstrap_servers: String,
        _kafka_cluster: MockCluster<'static, DefaultProducerContext>,
    }

    fn create_kafka_cluster(topics: &[&str]) -> TestKafkaCluster {
        let kafka_cluster = MockCluster::new(3).expect("create kafka mock cluster");
        for topic in topics {
            kafka_cluster
                .create_topic(topic, 1, 1)
                .expect("create kafka mock topic");
        }

        TestKafkaCluster {
            bootstrap_servers: kafka_cluster.bootstrap_servers(),
            _kafka_cluster: kafka_cluster,
        }
    }

    fn create_consumer(kafka: &TestKafkaCluster, group_id: &str, topic: &str) -> StreamConsumer {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &kafka.bootstrap_servers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("heartbeat.interval.ms", "2000")
            .create()
            .expect("consumer creation error");
        consumer.subscribe(&[topic]).expect("subscribe topic");
        consumer
    }

    async fn read_message_payload(consumer: &StreamConsumer) -> String {
        let message = consumer.recv().await.expect("read kafka message");
        std::str::from_utf8(message.payload().expect("payload"))
            .expect("utf8 payload")
            .to_string()
    }
}

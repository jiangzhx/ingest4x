use crate::repositories::{EventSinkRepository, RuntimeEventSink};
use crate::rhai_ctx::ProcessorDelivery;
use crate::settings::AutoOffsetReset;
use crate::sinks::{
    build_sink, event_sink_batch_config, EventSinkBatch, EventSinkBatchConfig,
    EventSinkBatchMetadata, EventSinkRuntime,
};
use actix_web::web::Data;
use anyhow::{anyhow, Context, Result};
use futures::lock::Mutex as AsyncMutex;
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

    pub(crate) async fn send_event_batch_to_sink(
        &self,
        target: &str,
        events: &[Value],
        metadata: EventSinkBatchMetadata,
    ) -> Result<()> {
        let router = self.current_router();
        router
            .send_event_batch_to_sink(target, events, metadata)
            .await
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

    pub(crate) fn batch_config(&self, name: &str) -> Option<EventSinkBatchConfig> {
        self.current_router().batch_config(name)
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

pub fn init_event_sinks_from_runtime_sinks(
    runtime_sinks: Vec<RuntimeEventSink>,
) -> Result<Data<EventSinkState>> {
    Ok(Data::new(EventSinkState {
        router: Arc::new(RwLock::new(Arc::new(EventRouter::from_runtime_sinks(
            runtime_sinks,
        )?))),
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
    sink: Box<dyn EventSinkRuntime>,
    auto_offset_reset: AutoOffsetReset,
    batch_config: Option<EventSinkBatchConfig>,
}

impl EventRouter {
    fn from_runtime_sinks(runtime_sinks: Vec<RuntimeEventSink>) -> Result<Self> {
        let mut sinks = HashMap::new();

        for runtime_sink in runtime_sinks {
            let batch_config = event_sink_batch_config(&runtime_sink.destination_json)
                .map_err(anyhow::Error::msg)?;
            sinks.insert(
                runtime_sink.sink_id.clone(),
                EventSinkEntry {
                    auto_offset_reset: runtime_sink.auto_offset_reset,
                    batch_config,
                    sink: build_sink(&runtime_sink)?,
                },
            );
        }

        Ok(Self { sinks })
    }

    async fn send_deliveries(&self, deliveries: &[ProcessorDelivery]) -> Result<()> {
        let mut deliveries_by_sink: HashMap<&str, Vec<Value>> = HashMap::new();
        for delivery in deliveries {
            if delivery.target.trim().is_empty() {
                tracing::warn!("processor delivery ignored empty sink target");
                continue;
            }
            let Some(_sink) = self.sinks.get(&delivery.target) else {
                tracing::warn!(
                    target = delivery.target.as_str(),
                    "processor delivery ignored unknown sink target"
                );
                continue;
            };
            deliveries_by_sink
                .entry(delivery.target.as_str())
                .or_default()
                .push(delivery.event.clone());
        }

        for (target, payloads) in deliveries_by_sink {
            let sink = self
                .sinks
                .get(target)
                .ok_or_else(|| anyhow!("unknown event sink target `{target}`"))?;
            sink.sink
                .send_batch(&payloads)
                .await
                .with_context(|| format!("event sink `{target}` failed"))?;
        }

        Ok(())
    }

    async fn send_delivery(&self, delivery: &ProcessorDelivery) -> Result<()> {
        let events = [delivery.event.clone()];
        self.send_events_to_sink(&delivery.target, &events).await
    }

    async fn send_events_to_sink(&self, target: &str, events: &[Value]) -> Result<()> {
        let sink = self
            .sinks
            .get(target)
            .ok_or_else(|| anyhow!("unknown event sink target `{target}`"))?;
        sink.sink
            .send_batch(events)
            .await
            .with_context(|| format!("event sink `{target}` failed"))
    }

    async fn send_event_batch_to_sink(
        &self,
        target: &str,
        events: &[Value],
        metadata: EventSinkBatchMetadata,
    ) -> Result<()> {
        let sink = self
            .sinks
            .get(target)
            .ok_or_else(|| anyhow!("unknown event sink target `{target}`"))?;
        sink.sink
            .send_batch_with_metadata(EventSinkBatch {
                events,
                metadata: Some(metadata),
            })
            .await
            .with_context(|| format!("event sink `{target}` failed"))
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

    fn batch_config(&self, name: &str) -> Option<EventSinkBatchConfig> {
        self.sinks
            .get(name)
            .and_then(|entry| entry.batch_config.clone())
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

#[cfg(test)]
mod tests {
    use super::init_event_sinks_from_runtime_sinks;
    use crate::repositories::{DeliveryTarget, DeliveryTargetType, RuntimeEventSink};
    use crate::rhai_ctx::ProcessorDelivery;
    use crate::settings::AutoOffsetReset;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::DefaultProducerContext;
    use rdkafka::{ClientConfig, Message};
    use serde_json::json;

    #[tokio::test]
    async fn sends_processed_event_to_all_declared_targets() {
        let kafka = create_kafka_cluster(&["raw-events", "payment-events"]);
        let raw_consumer = create_consumer(&kafka, "raw-target", "raw-events");
        let payment_consumer = create_consumer(&kafka, "payment-target", "payment-events");
        let sinks = init_event_sinks_from_runtime_sinks(vec![
            kafka_runtime_sink("kafka_raw", &kafka.bootstrap_servers, "raw-events"),
            kafka_runtime_sink("kafka_payment", &kafka.bootstrap_servers, "payment-events"),
        ])
        .expect("event sinks should initialize");

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
        let sinks = init_event_sinks_from_runtime_sinks(vec![kafka_runtime_sink(
            "kafka_raw",
            &kafka.bootstrap_servers,
            "raw-events",
        )])
        .expect("event sinks should initialize");

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

    fn kafka_runtime_sink(sink_id: &str, bootstrap_servers: &str, topic: &str) -> RuntimeEventSink {
        RuntimeEventSink {
            sink_id: sink_id.to_string(),
            name: sink_id.to_string(),
            destination_json: json!({ "topic": topic }),
            auto_offset_reset: AutoOffsetReset::Latest,
            target: DeliveryTarget {
                id: 1,
                target_id: format!("{sink_id}_target"),
                name: format!("{sink_id} target"),
                target_type: DeliveryTargetType::kafka(),
                config_json: json!({
                    "bootstrap_servers": bootstrap_servers,
                    "delivery_timeout_ms": "5000",
                    "queue_buffering_max_ms": "0",
                    "batch_num_messages": "1",
                    "queue_buffering_max_messages": "300",
                    "linger_ms": "0"
                }),
                enabled: true,
                created_at: 0,
                updated_at: 0,
            },
        }
    }
}

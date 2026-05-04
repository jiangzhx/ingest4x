use crate::settings::{EventRouteSet, EventRouteSettings, EventSinkConfig, EventsSettings};
use crate::utils::kafka::KafkaProducer;
use actix_web::web::Data;
use anyhow::{anyhow, Context, Result};
use log::warn;
use rdkafka::config::ClientConfig;
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
            EventSinkConfig::Stdout => Ok(Self::Stdout),
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
    use crate::settings::{EventRouteSet, EventRouteSettings, EventSinkConfig, EventsSettings};
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::DefaultProducerContext;
    use rdkafka::{ClientConfig, Message};
    use serde_json::json;
    use std::collections::HashMap;

    #[tokio::test]
    async fn routes_valid_events_by_appid_and_xwhat_before_fallback() {
        let kafka = create_kafka_cluster(&["payment-events", "default-events"]);
        let payment_consumer = create_consumer(&kafka, "payment-route", "payment-events");
        let default_consumer = create_consumer(&kafka, "default-route", "default-events");
        let settings = EventsSettings {
            sink: HashMap::from([
                (
                    "kafka_payment".to_string(),
                    EventSinkConfig::Kafka {
                        bootstrap_servers: kafka.bootstrap_servers.clone(),
                        topic: "payment-events".to_string(),
                        delivery_timeout_ms: "5000".to_string(),
                        queue_buffering_max_ms: "0".to_string(),
                        batch_num_messages: "1".to_string(),
                        queue_buffering_max_messages: "300".to_string(),
                        linger_ms: "0".to_string(),
                    },
                ),
                (
                    "kafka_default".to_string(),
                    EventSinkConfig::Kafka {
                        bootstrap_servers: kafka.bootstrap_servers.clone(),
                        topic: "default-events".to_string(),
                        delivery_timeout_ms: "5000".to_string(),
                        queue_buffering_max_ms: "0".to_string(),
                        batch_num_messages: "1".to_string(),
                        queue_buffering_max_messages: "300".to_string(),
                        linger_ms: "0".to_string(),
                    },
                ),
            ]),
            valid: EventRouteSet {
                routes: vec![
                    EventRouteSettings {
                        appid: Some(vec!["game-a".to_string()]),
                        xwhat: Some(vec!["payment".to_string()]),
                        sinks: vec!["kafka_payment".to_string()],
                        ack: vec!["kafka_payment".to_string()],
                    },
                    EventRouteSettings {
                        sinks: vec!["kafka_default".to_string()],
                        ack: vec!["kafka_default".to_string()],
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
            read_message_payload(&payment_consumer).await,
            "{\"id\":\"payment\"}"
        );
        assert_eq!(
            read_message_payload(&default_consumer).await,
            "{\"id\":\"startup\"}"
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

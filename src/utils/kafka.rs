use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::time::Duration;

#[derive(Clone)]
pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub fn new(config: ClientConfig) -> Self {
        let producer: FutureProducer<DefaultClientContext, _> =
            config.create().expect("Producer creation error");
        Self { producer }
    }

    pub async fn check_alive(&self) -> Result<(), KafkaError> {
        self.producer
            .client()
            .fetch_metadata(None, Duration::from_secs(5))?;
        Ok(())
    }

    ///
    /// Customizable send for producer.
    pub async fn send(
        &self,
        topic_name: String,
        partition: usize,
        key: Vec<u8>,
        payload: Vec<u8>,
    ) -> OwnedDeliveryResult {
        self.producer
            .send(
                FutureRecord::to(topic_name.as_str())
                    .partition(partition as _)
                    .payload(payload.as_slice())
                    .key(key.as_slice())
                    .headers(OwnedHeaders::new()),
                Timeout::After(Duration::from_secs(0)),
            )
            .await
    }

    ///
    /// Send value only to a topic without key being defined.
    pub async fn send_value<T: AsRef<str>>(
        &self,
        topic_name: T,
        payload: Vec<u8>,
    ) -> anyhow::Result<()> {
        self.producer
            .send::<Vec<u8>, _, _>(
                FutureRecord::to(topic_name.as_ref())
                    .payload(payload.as_slice())
                    .headers(OwnedHeaders::new()),
                Timeout::After(Duration::from_secs(0)),
            )
            .await
            .map(|_| ())
            .map_err(|(err, _)| anyhow::Error::from(err))
    }
}

use anyhow::{Context, Result};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::time::Duration;

#[derive(Clone)]
pub(super) struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub(super) fn new(config: ClientConfig) -> Result<Self> {
        let producer: FutureProducer<DefaultClientContext, _> =
            config.create().context("kafka producer creation failed")?;
        Ok(Self { producer })
    }

    pub(super) async fn check_alive(&self) -> Result<()> {
        self.fetch_metadata().map_err(Into::into)
    }

    pub(super) async fn send_value(&self, topic_name: &str, payload: Vec<u8>) -> Result<()> {
        self.producer
            .send::<Vec<u8>, _, _>(
                FutureRecord::to(topic_name)
                    .payload(payload.as_slice())
                    .headers(OwnedHeaders::new()),
                Timeout::After(Duration::from_secs(0)),
            )
            .await
            .map(|_| ())
            .map_err(|(err, _)| anyhow::Error::from(err))
    }

    fn fetch_metadata(&self) -> Result<(), KafkaError> {
        self.producer
            .client()
            .fetch_metadata(None, Duration::from_secs(5))?;
        Ok(())
    }
}

use crate::sinks::{EmptyConfig, EventSink, EventSinkProvider, SinkTypeMetadata};
use anyhow::Result;
use futures::future::BoxFuture;
use serde_json::Value;

pub static PROVIDER: StdoutProvider = StdoutProvider;

pub struct StdoutProvider;

pub struct StdoutSink;

impl EventSink for StdoutSink {
    type DeliveryTargetConfig = EmptyConfig;
    type EventSinkConfig = EmptyConfig;

    fn from_config(_target_config: EmptyConfig, _sink_config: EmptyConfig) -> Result<Self> {
        Ok(StdoutSink)
    }

    fn send_batch<'a>(&'a self, events: &'a [Value]) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            for event in events {
                println!("{event}");
            }
            Ok(())
        })
    }

    fn check_alive(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

impl EventSinkProvider for StdoutProvider {
    type Sink = StdoutSink;

    fn sink_type(&self) -> SinkTypeMetadata {
        SinkTypeMetadata {
            target_type: "stdout",
            label: "stdout",
        }
    }
}

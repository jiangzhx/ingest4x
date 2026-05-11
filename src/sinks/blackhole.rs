use crate::sinks::{EmptyConfig, EventSink, EventSinkProvider, SinkConfig, SinkTypeMetadata};
use anyhow::{anyhow, Result};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

pub static PROVIDER: BlackholeProvider = BlackholeProvider;

pub struct BlackholeProvider;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BlackholeMode {
    Ok,
    Slow,
    Fail,
}

fn default_mode() -> BlackholeMode {
    BlackholeMode::Ok
}

fn is_default_mode(mode: &BlackholeMode) -> bool {
    *mode == BlackholeMode::Ok
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationConfig {
    #[serde(default = "default_mode", skip_serializing_if = "is_default_mode")]
    pub mode: BlackholeMode,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub delay_ms: u64,
}

impl SinkConfig for DestinationConfig {}

pub struct BlackholeSink {
    mode: BlackholeMode,
    delay: Duration,
}

impl BlackholeSink {
    async fn maybe_delay(&self) {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
    }

    fn fail_if_configured(&self) -> Result<()> {
        if self.mode == BlackholeMode::Fail {
            return Err(anyhow!("blackhole sink configured to fail"));
        }
        Ok(())
    }
}

impl EventSink for BlackholeSink {
    type DeliveryTargetConfig = EmptyConfig;
    type EventSinkConfig = DestinationConfig;

    fn from_config(_target_config: EmptyConfig, sink_config: DestinationConfig) -> Result<Self> {
        Ok(Self {
            mode: sink_config.mode,
            delay: Duration::from_millis(sink_config.delay_ms),
        })
    }

    fn send_batch<'a>(&'a self, _events: &'a [Value]) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.maybe_delay().await;
            self.fail_if_configured()
        })
    }

    fn check_alive(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.fail_if_configured() })
    }
}

impl EventSinkProvider for BlackholeProvider {
    type Sink = BlackholeSink;

    fn sink_type(&self) -> SinkTypeMetadata {
        SinkTypeMetadata {
            target_type: "blackhole",
            label: "Blackhole",
        }
    }
}

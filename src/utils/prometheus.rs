use crate::settings::Settings;
use crate::utils::get_host_ip;
use crate::wal::WalWriter;
use actix_web_prom::{PrometheusMetrics, PrometheusMetricsBuilder};
use prometheus::{
    Counter, CounterVec, Gauge, HistogramOpts, HistogramVec, IntGaugeVec, Opts, Registry,
};
use std::collections::HashMap;

pub fn init_private_prometheus(registry: Registry) -> PrometheusMetrics {
    let mut labels = HashMap::new();
    labels.insert("version".to_string(), env!("CARGO_PKG_VERSION").to_string());

    PrometheusMetricsBuilder::new("metrics")
        .registry(registry)
        .endpoint("/metrics")
        .const_labels(labels)
        .build()
        .unwrap()
}

pub fn init_public_prometheus(registry: Registry) -> PrometheusMetrics {
    PrometheusMetricsBuilder::new("api")
        .registry(registry.clone())
        .build()
        .unwrap()
}

#[derive(Clone)]
pub struct IngestPrometheusMetrics {
    events_total: CounterVec,
    event_duration_seconds: HistogramVec,
}

impl IngestPrometheusMetrics {
    pub fn register(registry: &Registry) -> Result<Self, prometheus::Error> {
        let metrics = Self {
            events_total: CounterVec::new(
                Opts::new(
                    "ingest_events_total",
                    "Total ingest events by appid, xwhat, and processing result.",
                ),
                &["appid", "xwhat", "result"],
            )?,
            event_duration_seconds: HistogramVec::new(
                HistogramOpts::new(
                    "ingest_event_duration_seconds",
                    "Ingest event processing duration by appid, xwhat, and processing result.",
                ),
                &["appid", "xwhat", "result"],
            )?,
        };

        registry.register(Box::new(metrics.events_total.clone()))?;
        registry.register(Box::new(metrics.event_duration_seconds.clone()))?;

        Ok(metrics)
    }

    pub fn observe_event(&self, appid: &str, xwhat: &str, result: &str, duration_seconds: f64) {
        self.events_total
            .with_label_values(&[appid, xwhat, result])
            .inc();
        self.event_duration_seconds
            .with_label_values(&[appid, xwhat, result])
            .observe(duration_seconds);
    }
}

#[derive(Clone)]
pub struct WalPrometheusMetrics {
    node_info: IntGaugeVec,
    enabled: Gauge,
    ready: Gauge,
    reliable_ack: Gauge,
    no_sync: Gauge,
    available_bytes: Gauge,
    min_free_bytes: Gauge,
    active_segment_id: Gauge,
    active_segment_bytes: Gauge,
    max_lsn: Gauge,
    checkpoint_lsn: Gauge,
    replay_lag_lsn: Gauge,
    append_errors_total: Counter,
    replay_errors_total: Counter,
}

impl WalPrometheusMetrics {
    pub fn register(registry: &Registry) -> Result<Self, prometheus::Error> {
        let metrics = Self {
            node_info: IntGaugeVec::new(
                Opts::new("wal_node_info", "Static WAL node identity information."),
                &["machine_ip", "node_id"],
            )?,
            enabled: gauge("wal_enabled", "Whether WAL is configured for this node.")?,
            ready: gauge(
                "wal_ready",
                "Whether WAL is currently ready to accept durable writes.",
            )?,
            reliable_ack: gauge(
                "wal_reliable_ack",
                "Whether WAL ACKs represent the strong durable ACK contract.",
            )?,
            no_sync: gauge("wal_no_sync", "Whether WAL no_sync mode is enabled.")?,
            available_bytes: gauge(
                "wal_available_bytes",
                "Available bytes on the WAL filesystem.",
            )?,
            min_free_bytes: gauge(
                "wal_min_free_bytes",
                "Minimum required free bytes for WAL writes.",
            )?,
            active_segment_id: gauge("wal_active_segment_id", "Current WAL active segment id.")?,
            active_segment_bytes: gauge(
                "wal_active_segment_bytes",
                "Current WAL active segment size in bytes.",
            )?,
            max_lsn: gauge("wal_max_lsn", "Highest WAL LSN assigned by this node.")?,
            checkpoint_lsn: gauge("wal_checkpoint_lsn", "Last durable WAL checkpoint LSN.")?,
            replay_lag_lsn: gauge(
                "wal_replay_lag_lsn",
                "Difference between wal_max_lsn and wal_checkpoint_lsn.",
            )?,
            append_errors_total: counter(
                "wal_append_errors_total",
                "Total WAL append errors observed by ingest requests.",
            )?,
            replay_errors_total: counter(
                "wal_replay_errors_total",
                "Total WAL replay loop errors observed by this node.",
            )?,
        };

        registry.register(Box::new(metrics.node_info.clone()))?;
        registry.register(Box::new(metrics.enabled.clone()))?;
        registry.register(Box::new(metrics.ready.clone()))?;
        registry.register(Box::new(metrics.reliable_ack.clone()))?;
        registry.register(Box::new(metrics.no_sync.clone()))?;
        registry.register(Box::new(metrics.available_bytes.clone()))?;
        registry.register(Box::new(metrics.min_free_bytes.clone()))?;
        registry.register(Box::new(metrics.active_segment_id.clone()))?;
        registry.register(Box::new(metrics.active_segment_bytes.clone()))?;
        registry.register(Box::new(metrics.max_lsn.clone()))?;
        registry.register(Box::new(metrics.checkpoint_lsn.clone()))?;
        registry.register(Box::new(metrics.replay_lag_lsn.clone()))?;
        registry.register(Box::new(metrics.append_errors_total.clone()))?;
        registry.register(Box::new(metrics.replay_errors_total.clone()))?;

        Ok(metrics)
    }

    pub fn observe(&self, settings: &Settings, wal: &WalWriter) {
        self.enabled.set(1.0);
        self.no_sync.set(bool_value(settings.wal.no_sync));
        self.min_free_bytes.set(settings.wal.min_free_bytes as f64);

        let sink_names = settings.events.sink.keys().cloned().collect::<Vec<_>>();
        match wal.snapshot_for_sinks(&sink_names) {
            Ok(snapshot) => {
                self.node_info.reset();
                self.node_info
                    .with_label_values(&[get_host_ip().as_str(), snapshot.node_id.as_str()])
                    .set(1);
                self.ready.set(bool_value(snapshot.ready));
                self.reliable_ack
                    .set(bool_value(snapshot.ready && !snapshot.no_sync));
                self.no_sync.set(bool_value(snapshot.no_sync));
                self.available_bytes.set(snapshot.available_bytes as f64);
                self.min_free_bytes.set(snapshot.min_free_bytes as f64);
                self.active_segment_id
                    .set(snapshot.active_segment_id as f64);
                self.active_segment_bytes
                    .set(snapshot.active_segment_bytes as f64);
                self.max_lsn.set(snapshot.max_lsn as f64);
                self.checkpoint_lsn.set(snapshot.checkpoint_lsn as f64);
                self.replay_lag_lsn
                    .set(snapshot.max_lsn.saturating_sub(snapshot.checkpoint_lsn) as f64);
            }
            Err(_) => {
                self.ready.set(0.0);
                self.reliable_ack.set(0.0);
            }
        }
    }

    pub fn inc_append_errors(&self) {
        self.append_errors_total.inc();
    }

    pub fn inc_replay_errors(&self) {
        self.replay_errors_total.inc();
    }
}

fn gauge(name: &str, help: &str) -> Result<Gauge, prometheus::Error> {
    Gauge::with_opts(Opts::new(name, help))
}

fn counter(name: &str, help: &str) -> Result<Counter, prometheus::Error> {
    Counter::with_opts(Opts::new(name, help))
}

fn bool_value(value: bool) -> f64 {
    if value {
        1.0
    } else {
        0.0
    }
}

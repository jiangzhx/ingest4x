use actix_web_prom::{PrometheusMetrics, PrometheusMetricsBuilder};
use prometheus::Registry;
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

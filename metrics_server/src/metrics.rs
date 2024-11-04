// this file is the declaration of the metrics that will be used in the application
use std::str::FromStr;

use actix_web::{web, App, HttpServer};
use lazy_static::lazy_static;
use prometheus::{register_histogram, register_int_counter};
use prometheus::{Histogram, IntCounter};
use serde::Deserialize;

// market eng params
#[derive(Deserialize)]
struct QueryParams {
    start_time: String,
    end_time: String,
    count: u32,
}



lazy_static! {
    pub static ref AVG_PRICE_PER_MINUTE: Histogram =
        register_histogram!("avg_price_per_minute", "Average price per minute",)
            .expect("Can't create a metric");
}

lazy_static! {
    pub static ref MISSING_VALUES_COUNT: IntCounter =
        register_int_counter!("missing_values_count", "Number of missing values")
            .expect("Can't create a metric");
}

lazy_static! {
    pub static ref MISSING_VALUE_BY_CHANNEL: IntCounter = register_int_counter!(
        "missing_value_by_channel",
        "Number of missing values due to bounded buffer"
    )
    .expect("Can't create a metric");
}

pub async fn metrics() -> impl actix_web::Responder {
    let encoder = prometheus::TextEncoder::new();

    let metric_families = prometheus::gather();
    tracing::info!("Metrics: {:?}", metric_families.len());

    let mut buffer = String::new();
    encoder
        .encode_utf8(&metric_families, &mut buffer)
        .expect("Failed to encode metrics");

    buffer
}

pub async fn start_server(port: usize) -> anyhow::Result<()> {
    HttpServer::new(|| App::new().route("/metrics", web::get().to(metrics)))
        .workers(1)
        .bind(format!("0.0.0.0:{}", port.clone()))
        .unwrap_or_else(|_| panic!("Can't bind to port {}", port))
        .run()
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

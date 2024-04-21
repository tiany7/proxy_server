mod metrics;

use tokio::time::{sleep, Duration};
use actix_web::{web, App, HttpServer, Responder};
use metrics::{AVG_PRICE_PER_MINUTE, MISSING_VALUES_COUNT};


async fn metrics() -> impl actix_web::Responder {

    let encoder = prometheus::TextEncoder::new();
    
    let metric_families = prometheus::gather();
    tracing::info!("Metrics: {:?}", metric_families.len());

    let mut buffer = String::new();
    encoder.encode_utf8(&metric_families, &mut buffer).expect("Failed to encode metrics");

    buffer
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // TODO(yuanhan): next step is to use heartbeat function to monitor the server
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set log subscriber");
    tracing::info!("Starting metrics server");
    HttpServer::new(|| {
        App::new()
            .route("/metrics", web::get().to(metrics))
    })
    .bind("0.0.0.0:8080")? 
    .run()
    .await
}

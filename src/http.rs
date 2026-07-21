use axum::{extract::State, routing::get, Json, Router};
use chrono::Utc;
use serde::Serialize;
use std::sync::Arc;
use dashmap::DashMap;
use crate::BrokerMetrics;

//  Response types 
#[derive(Serialize)]
pub struct BrokerMetricsResponse {
    pub broker_id: String,
    pub clients_connected: Option<u64>,
    pub messages_sent: Option<u64>,
    pub messages_received: Option<u64>,
    pub bytes_sent: Option<u64>,
    pub bytes_received: Option<u64>,
    pub cpu_percent: Option<f64>,
    pub mem_usage_mb: Option<f64>,
    pub net_rx_bytes: Option<u64>,
    pub net_tx_bytes: Option<u64>,
    // Unix timestamp (seconds) of the last metric update for this broker.
    pub last_updated_secs: Option<i64>,
    // True when the most recent data is older than 2 × mqtt_scrape_secs.
    pub stale: bool,
}

#[derive(Serialize)]
pub struct MetricsEnvelope {
    pub brokers: Vec<BrokerMetricsResponse>,
    pub count: usize,
}

//  Shared state passed into axum 

pub struct AppState {
    pub metrics: Arc<DashMap<String, BrokerMetrics>>,
    /// 2 × mqtt_scrape_secs — precomputed so the handler doesn't need config.
    pub stale_threshold_secs: i64,
}

//  Handler 

pub async fn get_metrics(
    State(state): State<Arc<AppState>>,
) -> Json<MetricsEnvelope> {
    let now = Utc::now().timestamp();

    let mut brokers: Vec<BrokerMetricsResponse> = state
        .metrics
        .iter()
        .map(|entry| {
            let (broker_id, m) = (entry.key().clone(), entry.value());

            let stale = match m.last_updated_secs {
                Some(ts) => (now - ts) > state.stale_threshold_secs,
                // No timestamp yet means we haven't received a single scrape —
                // treat as stale rather than serving zeroed-out data silently.
                None => true,
            };

            BrokerMetricsResponse {
                broker_id,
                clients_connected: m.clients_connected,
                messages_sent: m.messages_sent,
                messages_received: m.messages_received,
                bytes_sent: m.bytes_sent,
                bytes_received: m.bytes_received,
                cpu_percent: m.cpu_percent,
                mem_usage_mb: m.mem_usage_mb,
                net_rx_bytes: m.net_rx_bytes,
                net_tx_bytes: m.net_tx_bytes,
                last_updated_secs: m.last_updated_secs,
                stale,
            }
        })
        .collect();

    // Deterministic ordering so repeated calls are easy to diff.
    brokers.sort_by(|a, b| a.broker_id.cmp(&b.broker_id));

    let count = brokers.len();
    Json(MetricsEnvelope { brokers, count })
}

//  Router factory 

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/metrics", get(get_metrics))
        .with_state(state)
}
use dashmap::DashMap;
use influxdb2_derive::WriteDataPoint;
use serde::Serialize;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::task::JoinHandle;
use std::collections::HashMap;

#[derive(Default, WriteDataPoint)]
#[measurement = "broker_metrics"]
pub struct BrokerMetricsPoint {
    #[influxdb(tag)]
    pub broker_id: String,
    #[influxdb(tag)]
    pub host: String,
    #[influxdb(field)]
    pub clients_connected: i64,
    #[influxdb(field)]
    pub messages_sent: i64,
    #[influxdb(field)]
    pub messages_received: i64,
    #[influxdb(field)]
    pub bytes_sent: i64,
    #[influxdb(field)]
    pub bytes_received: i64,
    #[influxdb(field)]
    pub cpu_percent: f64,
    #[influxdb(field)]
    pub mem_usage_mb: f64,
    #[influxdb(timestamp)]
    pub time: i64,
}

#[derive(Debug, Default, Clone)]
pub struct BrokerMetrics {                                 // ← B1-06: made `pub` so http.rs can read it
    pub clients_connected: Option<u64>,                   // ← B1-06: fields made `pub`
    pub messages_sent: Option<u64>,
    pub messages_received: Option<u64>,
    pub bytes_sent: Option<u64>,
    pub bytes_received: Option<u64>,
    pub cpu_percent: Option<f64>,
    pub mem_usage_mb: Option<f64>,
    pub net_rx_bytes: Option<u64>,
    pub net_tx_bytes: Option<u64>,
    pub last_updated_secs: Option<i64>,                   // ← B1-06: staleness timestamp
    pub mqtt_online: bool,                                 // ← B1-07
    pub docker_online: bool,                              // ← B1-07
}

// ── B2-02: fired alert record ────────────────────────────────────────────────
// This is intentionally separate from BrokerMetrics (which is scrape-owned) —
// the alert task owns writes to this store exclusively, same reasoning as the
// cooldown map below. B2-04 (GET /alerts) will read this store read-only.
#[derive(Debug, Clone, Serialize)]
pub struct FiredAlert {
    pub id: u64,
    pub broker_id: String,
    pub metric: String,
    pub operator: String,
    pub value: f64,
    pub threshold: f64,
    pub fired_at: i64,
    pub acknowledged: bool,
}

pub type AlertStore = Arc<Mutex<Vec<FiredAlert>>>;
pub type CooldownState = Arc<DashMap<String, Instant>>;
pub type SharedState = Arc<DashMap<String, BrokerMetrics>>;  // ← B1-06: made `pub`


// ── B3-0X: dynamic broker registry ───────────────────────────────────────────
// Maps broker_id -> the running task handles for that broker (one MQTT task,
// one Docker task). Registering a broker inserts an entry; deregistering
// aborts the handles and removes the entry. This is the single source of
// truth for "which brokers is the agent currently watching."
pub type BrokerHandles = Vec<JoinHandle<()>>;
pub type BrokerRegistry = Arc<Mutex<HashMap<String, BrokerHandles>>>;
use dashmap::DashMap;
use influxdb2_derive::WriteDataPoint;
use serde::Serialize;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::task::JoinHandle;
use std::collections::HashMap;
use tokio::sync::RwLock;

use crate::config::AlertRule;

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
pub struct BrokerMetrics {
    pub clients_connected: Option<u64>,
    pub messages_sent: Option<u64>,
    pub messages_received: Option<u64>,
    pub bytes_sent: Option<u64>,
    pub bytes_received: Option<u64>,
    pub cpu_percent: Option<f64>,
    pub mem_usage_mb: Option<f64>,
    pub net_rx_bytes: Option<u64>,
    pub net_tx_bytes: Option<u64>,
    pub last_updated_secs: Option<i64>,
    pub mqtt_online: bool,
    pub docker_online: bool,
}

// ── Fired alert record ────────────────────────────────────────────────────────
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
pub type SharedState = Arc<DashMap<String, BrokerMetrics>>;

// ── RulesStore ────────────────────────────────────────────────────────────────
// Shared, reloadable list of alert rules. The alert task holds a read lock
// on every eval tick; POST /reload takes a write lock only for the swap —
// so reads are never blocked except during the brief moment of replacement.
// tokio::sync::RwLock is used (not std) because the reload handler is async.
pub type RulesStore = Arc<RwLock<Vec<AlertRule>>>;

// ── Dynamic broker registry ───────────────────────────────────────────────────
pub type BrokerHandles = Vec<JoinHandle<()>>;
pub type BrokerRegistry = Arc<Mutex<HashMap<String, BrokerHandles>>>;
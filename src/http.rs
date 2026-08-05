use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use chrono::Utc;
use serde::Serialize;
use std::sync::Arc;
use dashmap::DashMap;

use crate::config::{BrokerConfig, RulesConfig};
use crate::registry::{self, BrokerRuntime};
use crate::types::{BrokerMetrics, BrokerRegistry, CooldownState, RulesStore};
use crate::types::AlertStore;
use axum::extract::Query;

#[derive(serde::Deserialize)]
pub struct AlertsQuery {
    pub acknowledged: Option<bool>,
    pub broker_id: Option<String>,
    pub metric: Option<String>,
}

#[derive(Serialize)]
pub struct AlertsEnvelope {
    pub alerts: Vec<crate::types::FiredAlert>,
    pub count: usize,
}

pub async fn get_alerts(
    State(state): State<Arc<AppState>>,
    Query(q): Query<AlertsQuery>,
) -> Json<AlertsEnvelope> {
    let alerts: Vec<_> = state
        .alerts
        .lock()
        .unwrap()
        .iter()
        .filter(|a| q.acknowledged.map_or(true, |v| a.acknowledged == v))
        .filter(|a| q.broker_id.as_deref().map_or(true, |b| a.broker_id == b))
        .filter(|a| q.metric.as_deref().map_or(true, |m| a.metric == m))
        .cloned()
        .collect();

    let count = alerts.len();
    Json(AlertsEnvelope { alerts, count })
}

// ── Response types ────────────────────────────────────────────────────────────

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
    pub last_updated_secs: Option<i64>,
    pub stale: bool,
    pub mqtt_online: bool,
    pub docker_online: bool,
    pub online: bool,
}

#[derive(Serialize)]
pub struct MetricsEnvelope {
    pub brokers: Vec<BrokerMetricsResponse>,
    pub count: usize,
}

#[derive(Serialize)]
pub struct BrokersEnvelope {
    pub brokers: Vec<String>,
    pub count: usize,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// ── Shared state passed into axum ─────────────────────────────────────────────

pub struct AppState {
    pub metrics: Arc<DashMap<String, BrokerMetrics>>,
    pub stale_threshold_secs: i64,
    pub registry: BrokerRegistry,
    pub runtime: BrokerRuntime,
    pub alerts: AlertStore,
    pub rules_store: RulesStore,       // shared with the alert task
    pub cooldowns: CooldownState,      // shared with the alert task; cleared on reload
    pub rules_path: String,            // path to rules.toml, resolved once at startup
}

// ── GET /metrics ──────────────────────────────────────────────────────────────

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
                mqtt_online: m.mqtt_online,
                docker_online: m.docker_online,
                online: m.mqtt_online && m.docker_online,
            }
        })
        .collect();

    brokers.sort_by(|a, b| a.broker_id.cmp(&b.broker_id));
    let count = brokers.len();
    Json(MetricsEnvelope { brokers, count })
}

// ── GET /brokers ──────────────────────────────────────────────────────────────

pub async fn get_brokers(State(state): State<Arc<AppState>>) -> Json<BrokersEnvelope> {
    let mut brokers = registry::list_brokers(&state.registry);
    brokers.sort();
    let count = brokers.len();
    Json(BrokersEnvelope { brokers, count })
}

// ── POST /brokers ─────────────────────────────────────────────────────────────

pub async fn post_broker(
    State(state): State<Arc<AppState>>,
    Json(broker): Json<BrokerConfig>,
) -> (StatusCode, Json<serde_json::Value>) {
    if broker.id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "broker id must not be empty" })),
        );
    }

    registry::spawn_broker(broker.clone(), &state.runtime, &state.registry);

    (
        StatusCode::CREATED,
        Json(serde_json::json!({ "registered": broker.id })),
    )
}

// ── DELETE /brokers/:id ───────────────────────────────────────────────────────

pub async fn delete_broker(
    State(state): State<Arc<AppState>>,
    Path(broker_id): Path<String>,
) -> StatusCode {
    let stopped = registry::stop_broker(&broker_id, state.metrics.clone(), &state.registry);
    if stopped {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

// ── POST /reload ──────────────────────────────────────────────────────────────
// Reloads rules.toml without restarting the agent (Prometheus-style).
//
// The file is validated before anything is mutated — if validation fails,
// the currently-active rules stay in place and the caller gets a 400 with
// the error. Only on success are the rules swapped and cooldowns cleared.
//
// Response 200: { "reloaded": <n>, "rules": [...] }
// Response 400: { "error": "<validation message>" }

#[derive(Serialize)]
struct ReloadResponse {
    reloaded: usize,
    rules: Vec<ReloadedRule>,
}

#[derive(Serialize)]
struct ReloadedRule {
    metric: String,
    operator: String,
    threshold: f64,
    cooldown_secs: u64,
}

pub async fn post_reload(
    State(state): State<Arc<AppState>>,
) -> (StatusCode, Json<serde_json::Value>) {
    // 1. Parse + validate — no side effects yet.
    let new_rules = match RulesConfig::load(&state.rules_path) {
        Ok(r) => r.alert_rules,
        Err(e) => {
            eprintln!("[reload] rejected: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            );
        }
    };

    let count = new_rules.len();

    // 2. Swap rules atomically under the write lock.
    //    The alert task's read lock is unblocked as soon as this scope exits.
    {
        let mut store = state.rules_store.write().await;
        *store = new_rules.clone();
    }

    // 3. Clear cooldowns — clean slate so rules with changed thresholds
    //    aren't suppressed by cooldowns from the previous rule set.
    state.cooldowns.clear();

    println!("[reload] loaded {} rule(s) from '{}'", count, state.rules_path);

    let summary: Vec<ReloadedRule> = new_rules
        .into_iter()
        .map(|r| ReloadedRule {
            metric: r.metric,
            operator: r.operator,
            threshold: r.threshold,
            cooldown_secs: r.cooldown_secs,
        })
        .collect();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "reloaded": count,
            "rules": summary,
        })),
    )
}

// ── Router factory ────────────────────────────────────────────────────────────

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/metrics", get(get_metrics))
        .route("/brokers", get(get_brokers).post(post_broker))
        .route("/brokers/:id", delete(delete_broker))
        .route("/alerts", get(get_alerts))
        .route("/reload", post(post_reload))
        .with_state(state)
}
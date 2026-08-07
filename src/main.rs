use bollard::Docker;
use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;
use std::net::SocketAddr;

mod alerts;
mod config;
mod docker;
mod http;
mod influx;
mod mqtt;
mod types;
mod registry;
mod discovery;

use config::{Config, RulesConfig};
use http::{build_router, AppState};
use types::{AlertStore, CooldownState, RulesStore, SharedState};
use crate::registry::BrokerRuntime;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok(); 
    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());
    let config = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Config error: {}", e);
            std::process::exit(1);
        }
    };

    // Load rules.toml separately — supports RULES_PATH env override.
    let rules_path = std::env::var("RULES_PATH").unwrap_or_else(|_| "rules.toml".to_string());
    let initial_rules = match RulesConfig::load(&rules_path) {
        Ok(r) => r.alert_rules,
        Err(e) => {
            eprintln!("Rules error: {}", e);
            std::process::exit(1);
        }
    };
    println!("Loaded {} alert rule(s) from '{}'", initial_rules.len(), rules_path);

    let state: SharedState = Arc::new(DashMap::new());
    let docker = Docker::connect_with_local_defaults().expect("failed to connect to Docker");

    let mut handles = vec![];

    let broker_runtime = BrokerRuntime {
        state: state.clone(),
        docker: docker.clone(),
        mqtt_interval: config.intervals.mqtt_scrape_secs,
        docker_interval: config.intervals.docker_scrape_secs,
    };
    let broker_registry = registry::new_registry();

    for broker in &config.brokers {
        registry::spawn_broker(broker.clone(), &broker_runtime, &broker_registry);
    }

    // Pass to writer task as before (adjust spawn to use influx_cfg directly
    // or clone the fields — whichever fits your current move semantics).
    let influx_cfg = config.influxdb;
    let influx_interval = config.intervals.influx_write_secs;
    // Build the InfluxDB client once — shared between the writer task and the
    // HTTP history handler. The writer task keeps its own clone (cheap, it's
    // Arc-backed internally), so no ownership conflict.
    let influx_client = Arc::new(influxdb2::Client::new(
        &influx_cfg.url,
        &influx_cfg.org,
        &influx_cfg.token,
    ));
    let influx_bucket = influx_cfg.bucket.clone();

    let state_clone = state.clone();
    handles.push(tokio::spawn(async move {
        influx::spawn_influx_writer(state_clone, influx_cfg, influx_interval).await;
    }));

    let discovery_docker = docker.clone();
    let discovery_runtime = broker_runtime.clone();
    let discovery_registry = broker_registry.clone();
    handles.push(tokio::spawn(async move {
        discovery::run_discovery_task(discovery_docker, discovery_runtime, discovery_registry, 15).await;
    }));

    // Shared between the alert task and the HTTP reload handler.
    let rules_store: RulesStore = Arc::new(RwLock::new(initial_rules));
    let cooldowns: CooldownState = Arc::new(DashMap::new());
    let alert_store: AlertStore = Arc::new(Mutex::new(Vec::new()));
    
    // Rate limit config — from env with sane defaults.
    // per_second: how many requests replenish per second per IP.
    // burst: how many requests above the steady rate are allowed before blocking.
    let rate_limit_per_second: u64 = std::env::var("RATE_LIMIT_PER_SECOND")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    let rate_limit_burst: u32 = std::env::var("RATE_LIMIT_BURST")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);

    // API key — read from env at startup. Fail fast if absent so the agent
    // never starts in an inadvertently open state.
    let api_key = std::env::var("API_KEY").unwrap_or_else(|_| {
        eprintln!("Error: API_KEY env var must be set");
        std::process::exit(1);
    });

    let app_state = Arc::new(AppState {
        metrics: state.clone(),
        stale_threshold_secs: (config.intervals.docker_scrape_secs * 2) as i64,
        registry: broker_registry,
        runtime: broker_runtime,
        alerts: alert_store.clone(),
        rules_store: rules_store.clone(),
        cooldowns: cooldowns.clone(),
        rules_path,
        influx_client,
        influx_bucket,
        api_key,
        rate_limit_per_second,   
        rate_limit_burst,
    });

    let router = build_router(app_state);
    //CRITICAL: must use into_make_service_with_connect_info — PeerIpKeyExtractor
    // reads SocketAddr from request extensions, which axum only injects when
    // connect_info is enabled. Without this, every request fails with
    // GovernorError::UnableToExtractKey and gets a 500.
    handles.push(tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
            .await
            .expect("failed to bind HTTP server to port 3000");
        println!("HTTP server listening on http://0.0.0.0:3000");
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),  // ← changed
        )
        .await
        .expect("HTTP server error");
    }));

    let alert_eval_interval = config.intervals.mqtt_scrape_secs;
    let email_cfg = config.email.clone();
    handles.push(tokio::spawn(async move {
        alerts::run_alert_task(
            state.clone(),
            cooldowns,
            alert_store,
            rules_store,      // ← RulesStore, not a Vec
            email_cfg,
            alert_eval_interval,
        ).await;
    }));

    for handle in handles {
        let _ = handle.await;
    }
}
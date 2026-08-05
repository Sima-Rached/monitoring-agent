use bollard::Docker;
use dashmap::DashMap;
use std::sync::{Arc, Mutex};

mod alerts;
mod config;
mod docker;
mod http;
mod influx;
mod mqtt;
mod types;
mod registry;   
mod discovery; 

use config::Config;
use http::{build_router, AppState};
use types::{AlertStore, CooldownState, SharedState};
use crate::registry::BrokerRuntime;

#[tokio::main]
async fn main() {
    // main.rs
    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());
    let config = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Config error: {}", e);
            std::process::exit(1);
        }
    };

    let state: SharedState = Arc::new(DashMap::new());
    let docker = Docker::connect_with_local_defaults().expect("failed to connect to Docker");

    let mut handles = vec![];

    // ── B3-0X: broker registry replaces the direct spawn loop ────────────────
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


    let state_clone = state.clone();
    let influx_cfg = config.influxdb;
    let influx_interval = config.intervals.influx_write_secs;
    handles.push(tokio::spawn(async move {
        influx::spawn_influx_writer(state_clone, influx_cfg, influx_interval).await;
    }));

    // after broker_runtime and broker_registry are built, and after the
    // initial config.toml brokers are spawned:
    let discovery_docker = docker.clone();
    let discovery_runtime = broker_runtime.clone();
    let discovery_registry = broker_registry.clone();
    handles.push(tokio::spawn(async move {
        discovery::run_discovery_task(discovery_docker, discovery_runtime, discovery_registry, 15).await;
    }));

    // ── B1-06: HTTP server task ───────────────────────────────────────────────
     let cooldowns: CooldownState = Arc::new(DashMap::new());
    let alert_store: AlertStore = Arc::new(Mutex::new(Vec::new()));
    let app_state = Arc::new(AppState {
        metrics: state.clone(),
        stale_threshold_secs: (config.intervals.docker_scrape_secs * 2) as i64,
        registry: broker_registry,
        runtime: broker_runtime,
        alerts: alert_store.clone(),   // ← add, before alert_store is moved into the alert task

    });
    let router = build_router(app_state);
    handles.push(tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
            .await
            .expect("failed to bind HTTP server to port 3000");
        println!("HTTP server listening on http://0.0.0.0:3000");
        axum::serve(listener, router)
            .await
            .expect("HTTP server error");
    }));

    // ── B2-02 + B2-03: alert evaluation task ─────────────────────────────────
   
    let alert_rules = config.alert_rules.clone();
    let email_cfg = config.email.clone();                  // ← B2-03
    let alert_eval_interval = config.intervals.mqtt_scrape_secs; // reuse existing cadence
    
    handles.push(tokio::spawn(async move {
        alerts::run_alert_task(
            state.clone(),
            cooldowns,
            alert_store,
            alert_rules,
            email_cfg,     //B2-03
            alert_eval_interval,
        ).await;
    }));

    for handle in handles {
        let _ = handle.await;
    }
}

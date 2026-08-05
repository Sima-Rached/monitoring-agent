use chrono::Utc;
use futures::stream;
use influxdb2::Client;
use std::time::Duration;

use crate::config::InfluxConfig;
use crate::types::{BrokerMetricsPoint, SharedState};

pub async fn spawn_influx_writer(state: SharedState, influx_cfg: InfluxConfig, write_interval_secs: u64) {
    let client = Client::new(influx_cfg.url, influx_cfg.org, influx_cfg.token);

    // Resolve once at task startup — the hostname won't change while the
    // agent is running, and failing loudly here is better than silently
    // tagging every point with a wrong or empty host.
    let host = std::env::var("AGENT_HOSTNAME")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| {
            hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| {
                    eprintln!("[influx] warning: could not determine hostname, using 'unknown'");
                    "unknown".to_string()
                })
        });

    println!("[influx] tagging points with host='{}'", host);

    loop {
        tokio::time::sleep(Duration::from_secs(write_interval_secs)).await;

        let points: Vec<BrokerMetricsPoint> = state
            .iter()
            .map(|entry| {
                let (broker_id, m) = (entry.key().clone(), entry.value());
                BrokerMetricsPoint {
                    broker_id,
                    host: host.clone(),
                    clients_connected: m.clients_connected.unwrap_or(0) as i64,
                    messages_sent: m.messages_sent.unwrap_or(0) as i64,
                    messages_received: m.messages_received.unwrap_or(0) as i64,
                    bytes_sent: m.bytes_sent.unwrap_or(0) as i64,
                    bytes_received: m.bytes_received.unwrap_or(0) as i64,
                    cpu_percent: m.cpu_percent.unwrap_or(0.0),
                    mem_usage_mb: m.mem_usage_mb.unwrap_or(0.0),
                    time: Utc::now().timestamp_nanos_opt().unwrap_or(0),
                }
            })
            .collect();

        let count = points.len();
        if let Err(e) = client.write(&influx_cfg.bucket, stream::iter(points)).await {
            eprintln!("[influx] write error: {:?}", e);
        } else {
            println!("[influx] wrote {} broker metric points to InfluxDB", count);
        }
    }
}
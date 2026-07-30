use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Packet};
use std::time::Duration;
use std::sync::Arc;
use bollard::Docker;
use bollard::container::StatsOptions;
use futures_util::stream::StreamExt;
use dashmap::DashMap;
use influxdb2::Client;
use influxdb2_derive::WriteDataPoint;
use futures::stream;
use chrono::Utc;
mod config;
mod http;                                                   // ← B1-06
use config::{Config, BrokerConfig, AlertRule};
use http::{AppState, build_router};                        // ← B1-06
use std::time::Instant;
use std::sync::Mutex;
use serde::Serialize;      // needed for FiredAlert — you may already have `serde::Deserialize` imported elsewhere, this adds Serialize
use lettre::{
    Message, SmtpTransport, Transport,
    message::header::ContentType,
    transport::smtp::authentication::Credentials,
};
use config::EmailConfig;


#[derive(Default, WriteDataPoint)]
#[measurement = "broker_metrics"]
struct BrokerMetricsPoint {
    #[influxdb(tag)]
    broker_id: String,
    #[influxdb(tag)]
    host: String,
    #[influxdb(field)]
    clients_connected: i64,
    #[influxdb(field)]
    messages_sent: i64,
    #[influxdb(field)]
    messages_received: i64,
    #[influxdb(field)]
    bytes_sent: i64,
    #[influxdb(field)]
    bytes_received: i64,
    #[influxdb(field)]
    cpu_percent: f64,
    #[influxdb(field)]
    mem_usage_mb: f64,
    #[influxdb(timestamp)]
    time: i64,
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

async fn run_mqtt_task(broker: BrokerConfig, state: SharedState, scrape_interval_secs: u64) {
    loop {
        // Build a fresh client on every connection attempt.
        // The old client/eventloop pair is dropped when we fall through to
        // the sleep at the bottom, so there is no resource leak.
        let mut mqttoptions = MqttOptions::new(
            format!("cloud-monitoring-agent-{}", broker.id),
            broker.mqtt_host.clone(),
            broker.mqtt_port,
        );
        mqttoptions.set_keep_alive(Duration::from_secs(30));

        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

        if let Err(e) = client.subscribe("$SYS/#", QoS::AtMostOnce).await {
            eprintln!("[{}] failed to subscribe to $SYS/#: {:?}", broker.id, e);
            // Mark offline before sleeping — the subscribe itself failed,
            // so we never had a working connection this attempt.       // ← B1-07
            state.entry(broker.id.clone()).or_default().mqtt_online = false; // ← B1-07
            tokio::time::sleep(Duration::from_secs(scrape_interval_secs)).await;
            continue; // restart the outer loop → rebuild client
        }

        println!("[{}] agent subscribed to $SYS/# on {}:{}", broker.id, broker.mqtt_host, broker.mqtt_port);
        // Subscription confirmed — broker is reachable.               // ← B1-07
        state.entry(broker.id.clone()).or_default().mqtt_online = true; // ← B1-07

        // Inner poll loop — runs until the connection breaks.
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    let topic = publish.topic.as_str();
                    let payload = String::from_utf8_lossy(&publish.payload);

                    let mut entry = state.entry(broker.id.clone()).or_default();

                    match topic {
                        "$SYS/broker/clients/connected" => {
                            entry.clients_connected = payload.trim().parse().ok();
                        }
                        "$SYS/broker/messages/sent" => {
                            entry.messages_sent = payload.trim().parse().ok();
                        }
                        "$SYS/broker/messages/received" => {
                            entry.messages_received = payload.trim().parse().ok();
                        }
                        "$SYS/broker/bytes/sent" => {
                            entry.bytes_sent = payload.trim().parse().ok();
                        }
                        "$SYS/broker/bytes/received" => {
                            entry.bytes_received = payload.trim().parse().ok();
                        }
                        _ => {}
                    }

                    entry.last_updated_secs = Some(Utc::now().timestamp());
                    println!("[{}] {:?}", broker.id, *entry);
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[{}] MQTT connection lost: {:?}", broker.id, e);
                    // Mark offline immediately — within this scrape cycle. // ← B1-07
                    state.entry(broker.id.clone()).or_default().mqtt_online = false; // ← B1-07
                    tokio::time::sleep(Duration::from_secs(scrape_interval_secs)).await;
                    break; // exit inner loop → outer loop rebuilds client
                }
            }
        }
    }
} 

async fn run_docker_task(broker: BrokerConfig, state: SharedState, docker: Docker, scrape_interval_secs: u64) {
    loop {
        let mut stream = docker.stats(
            &broker.container_name,
            Some(StatsOptions { stream: false, ..Default::default() }),
        );

        match stream.next().await {                                    // ← B1-07: was `if let Some(Ok(...))`
            Some(Ok(stats)) => {
                let cpu_delta = stats.cpu_stats.cpu_usage.total_usage as f64
                    - stats.precpu_stats.cpu_usage.total_usage as f64;
                let system_delta = stats.cpu_stats.system_cpu_usage.unwrap_or(0) as f64
                    - stats.precpu_stats.system_cpu_usage.unwrap_or(0) as f64;
                let num_cpus = stats.cpu_stats.online_cpus.unwrap_or(1) as f64;

                let cpu_percent = if system_delta > 0.0 && cpu_delta > 0.0 {
                    (cpu_delta / system_delta) * num_cpus * 100.0
                } else {
                    0.0
                };

                let mem_usage_mb = stats.memory_stats.usage.unwrap_or(0) as f64 / (1024.0 * 1024.0);

                let (mut rx_bytes, mut tx_bytes) = (0u64, 0u64);
                if let Some(networks) = &stats.networks {
                    for (_iface, net) in networks {
                        rx_bytes += net.rx_bytes;
                        tx_bytes += net.tx_bytes;
                    }
                }

                let mut entry = state.entry(broker.id.clone()).or_default();
                entry.cpu_percent = Some(cpu_percent);
                entry.mem_usage_mb = Some(mem_usage_mb);
                entry.net_rx_bytes = Some(rx_bytes);
                entry.net_tx_bytes = Some(tx_bytes);
                entry.last_updated_secs = Some(Utc::now().timestamp());
                entry.docker_online = true;                            // ← B1-07

                println!(
                    "[{}] container stats -> cpu: {:.2}%, mem: {:.2} MB, net_rx: {}, net_tx: {}",
                    broker.id, cpu_percent, mem_usage_mb, rx_bytes, tx_bytes
                );
            }
            Some(Err(e)) => {                                          // ← B1-07
                eprintln!("[{}] Docker stats error: {:?}", broker.id, e);
                state.entry(broker.id.clone()).or_default().docker_online = false; // ← B1-07
            }
            None => {                                                  // ← B1-07
                eprintln!("[{}] Docker stats stream ended unexpectedly", broker.id);
                state.entry(broker.id.clone()).or_default().docker_online = false; // ← B1-07
            }
        }

        tokio::time::sleep(Duration::from_secs(scrape_interval_secs)).await;
    }
}

// ── B2-02: alert evaluation loop ─────────────────────────────────────────────
async fn run_alert_task(
    state: SharedState,          // existing metrics map — read only
    cooldowns: CooldownState,    // this task owns all writes
    alert_store: AlertStore,     // this task owns all writes
    rules: Vec<AlertRule>,       // cloned from config at startup, immutable
    email_cfg: EmailConfig,            // ← B2-03
    eval_interval_secs: u64,
) {
    // Build the SMTP transport once — reused for every alert fire.
    // lettre's SmtpTransport is cheaply cloneable; building it once
    // avoids reconnecting to the SMTP server on every evaluation tick.
    let creds = Credentials::new(email_cfg.username.clone(), email_cfg.password.clone());
    let mailer = SmtpTransport::starttls_relay(&email_cfg.smtp_host)
        .expect("failed to build SMTP transport")
        .port(email_cfg.smtp_port)
        .credentials(creds)
        .build();

    let mut next_id: u64 = 0;

    loop {
        tokio::time::sleep(Duration::from_secs(eval_interval_secs)).await;

        for entry in state.iter() {
            let (broker_id, metrics) = (entry.key().clone(), entry.value());

            for rule in &rules {
                // Config::load already guarantees rule.metric ∈ VALID_METRICS
                // and rule.operator ∈ VALID_OPERATORS (B2-01), so no `_ => None`
                // fallback masking a typo here — every arm is a real field.
                let value: Option<f64> = match rule.metric.as_str() {
                    "clients_connected" => metrics.clients_connected.map(|v| v as f64),
                    "messages_sent" => metrics.messages_sent.map(|v| v as f64),
                    "messages_received" => metrics.messages_received.map(|v| v as f64),
                    "bytes_sent" => metrics.bytes_sent.map(|v| v as f64),
                    "bytes_received" => metrics.bytes_received.map(|v| v as f64),
                    "cpu_percent" => metrics.cpu_percent,
                    "mem_usage_mb" => metrics.mem_usage_mb,
                    "net_rx_bytes" => metrics.net_rx_bytes.map(|v| v as f64),
                    "net_tx_bytes" => metrics.net_tx_bytes.map(|v| v as f64),
                    _ => None, // unreachable given B2-01 validation, kept for exhaustiveness
                };

                let Some(value) = value else { continue }; // no data yet for this metric

                let breached = match rule.operator.as_str() {
                    ">" => value > rule.threshold,
                    "<" => value < rule.threshold,
                    "==" => (value - rule.threshold).abs() < f64::EPSILON,
                    _ => false, // unreachable given B2-01 validation
                };
                if !breached {
                    continue;
                }

                let cooldown_key = format!("{}:{}", broker_id, rule.metric);
                let now = Instant::now();

                if let Some(last_fired) = cooldowns.get(&cooldown_key) {
                    if now.duration_since(*last_fired).as_secs() < rule.cooldown_secs {
                        continue; // still in cooldown — suppress duplicate
                    }
                }
                cooldowns.insert(cooldown_key, now);

                let alert = FiredAlert {
                    id: next_id,
                    broker_id: broker_id.clone(),
                    metric: rule.metric.clone(),
                    operator: rule.operator.clone(),
                    value,
                    threshold: rule.threshold,
                    fired_at: Utc::now().timestamp(),
                    acknowledged: false,
                };
                next_id += 1;

                eprintln!(
                    "[ALERT] broker={} metric={} value={:.2} {} {} (id={})",
                    alert.broker_id, alert.metric, alert.value, alert.operator, alert.threshold, alert.id
                );

                // Lock is held only for this push — no .await inside the
                // critical section, so std::sync::Mutex is safe here and
                // cheaper than tokio::sync::Mutex.
                alert_store.lock().unwrap().push(alert.clone());

                // ── B2-03: send email notification ───────────────────────────
                // Build one email per recipient — lettre requires a separate
                // Message per address; the loop is cheap (usually 1 recipient).
                for recipient in &email_cfg.to {
                    let body = format!(
                        "ProgressBox Alert\n\
                         ─────────────────\n\
                         Broker:    {}\n\
                         Metric:    {}\n\
                         Condition: {} {} {}\n\
                         Value:     {:.4}\n\
                         Time:      {} (Unix)\n\
                         Alert ID:  {}",
                        alert.broker_id,
                        alert.metric,
                        alert.metric, alert.operator, alert.threshold,
                        alert.value,
                        alert.fired_at,
                        alert.id,
                    );

                    let email = match Message::builder()
                        .from(email_cfg.from.parse().unwrap())
                        .to(recipient.parse().unwrap())
                        .subject(format!(
                            "[ProgressBox] ALERT — {} {} {} {} on {}",
                            alert.metric, alert.operator, alert.threshold,
                            alert.value, alert.broker_id
                        ))
                        .header(ContentType::TEXT_PLAIN)
                        .body(body)
                    {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!("[ALERT] failed to build email: {:?}", e);
                            continue;
                        }
                    };

                    match mailer.send(&email) {
                        Ok(_)  => println!("[ALERT] email sent to {}", recipient),
                        Err(e) => eprintln!("[ALERT] email send failed to {}: {:?}", recipient, e),
                    }
                }
            }
        }
    }
}
async fn spawn_influx_writer(state: SharedState, influx_cfg: config::InfluxConfig, write_interval_secs: u64) {
    // unchanged — no B1-06 edits needed here
    let client = Client::new(influx_cfg.url, influx_cfg.org, influx_cfg.token);

    loop {
        tokio::time::sleep(Duration::from_secs(write_interval_secs)).await;

        let points: Vec<BrokerMetricsPoint> = state
            .iter()
            .map(|entry| {
                let (broker_id, m) = (entry.key().clone(), entry.value());
                BrokerMetricsPoint {
                    broker_id,
                    host: "sima-ThinkPad-E16-Gen-1".to_string(),
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
            eprintln!("InfluxDB write error: {:?}", e);
        } else {
            println!("wrote {} broker metric points to InfluxDB", count);
        }
    }
}

#[tokio::main]
async fn main() {
    let config = match Config::load("config.toml") {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Config error: {}", e);
            std::process::exit(1);
        }
    };

    let state: SharedState = Arc::new(DashMap::new());
    let docker = Docker::connect_with_local_defaults().expect("failed to connect to Docker");

    let mut handles = vec![];

    for broker in &config.brokers {
        let state_clone = state.clone();
        let broker_clone = broker.clone();
        let mqtt_interval = config.intervals.mqtt_scrape_secs;
        handles.push(tokio::spawn(async move {
            run_mqtt_task(broker_clone, state_clone, mqtt_interval).await;
        }));

        let state_clone = state.clone();
        let broker_clone = broker.clone();
        let docker_clone = docker.clone();
        let docker_interval = config.intervals.docker_scrape_secs;
        handles.push(tokio::spawn(async move {
            run_docker_task(broker_clone, state_clone, docker_clone, docker_interval).await;
        }));
    }

    let state_clone = state.clone();
    let influx_cfg = config.influxdb;
    let influx_interval = config.intervals.influx_write_secs;
    handles.push(tokio::spawn(async move {
        spawn_influx_writer(state_clone, influx_cfg, influx_interval).await;
    }));

    // ── B1-06: HTTP server task ───────────────────────────────────────────────
    let app_state = Arc::new(AppState {
        metrics: state.clone(),
        stale_threshold_secs: (config.intervals.docker_scrape_secs * 2) as i64,
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
    // ── B2-02 + B2-03: alert evaluation task ─────────────────────────────────────────
    let cooldowns: CooldownState = Arc::new(DashMap::new());
    let alert_store: AlertStore = Arc::new(Mutex::new(Vec::new()));
    let alert_rules = config.alert_rules.clone();
    let email_cfg = config.email.clone();                  // ← B2-03
    let alert_eval_interval = config.intervals.mqtt_scrape_secs; // reuse existing cadence
    handles.push(tokio::spawn(async move {
        run_alert_task(
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
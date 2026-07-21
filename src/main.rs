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
use config::{Config, BrokerConfig};

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
struct BrokerMetrics {
    clients_connected: Option<u64>,
    messages_sent: Option<u64>,
    messages_received: Option<u64>,
    bytes_sent: Option<u64>,
    bytes_received: Option<u64>,
    cpu_percent: Option<f64>,
    mem_usage_mb: Option<f64>,
    net_rx_bytes: Option<u64>,
    net_tx_bytes: Option<u64>,
}

// Shared state, keyed by broker_id. Every task below writes into this;
// nothing reads it yet (that's B1-06's GET /metrics endpoint).
type SharedState = Arc<DashMap<String, BrokerMetrics>>;

async fn run_mqtt_task(broker: BrokerConfig, state: SharedState, scrape_interval_secs: u64) {
    let mut mqttoptions = MqttOptions::new(
        format!("cloud-monitoring-agent-{}", broker.id),
        broker.mqtt_host.clone(),
        broker.mqtt_port,
    );
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    if let Err(e) = client.subscribe("$SYS/#", QoS::AtMostOnce).await {
        eprintln!("[{}] failed to subscribe to $SYS/#: {:?}", broker.id, e);
        return;
    }

    println!("[{}] agent subscribed to $SYS/# on {}:{}", broker.id, broker.mqtt_host, broker.mqtt_port);

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                let topic = publish.topic.as_str();
                let payload = String::from_utf8_lossy(&publish.payload);

                // Grab-or-create this broker's entry, mutate just the touched field.
                // DashMap gives us per-key locking, so this task never blocks
                // the other broker's task even under contention.
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

                println!("[{}] {:?}", broker.id, *entry);
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("[{}] MQTT event loop error: {:?}", broker.id, e);
                tokio::time::sleep(Duration::from_secs(scrape_interval_secs)).await;
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

        if let Some(Ok(stats)) = stream.next().await {
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

            println!(
                "[{}] container stats -> cpu: {:.2}%, mem: {:.2} MB, net_rx: {}, net_tx: {}",
                broker.id, cpu_percent, mem_usage_mb, rx_bytes, tx_bytes
            );
        }

        tokio::time::sleep(Duration::from_secs(scrape_interval_secs)).await;
    }
}

async fn spawn_influx_writer(state: SharedState, influx_cfg: config::InfluxConfig, write_interval_secs: u64) {
    let client = Client::new(influx_cfg.url, influx_cfg.org, influx_cfg.token);

    loop {
        tokio::time::sleep(Duration::from_secs(write_interval_secs)).await;

        let points: Vec<BrokerMetricsPoint> = state
            .iter()
            .map(|entry| {
                let (broker_id, m) = (entry.key().clone(), entry.value());
                BrokerMetricsPoint {
                    broker_id,
                    host: "sima-ThinkPad-E16-Gen-1".to_string().clone(),
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

    for handle in handles {
        let _ = handle.await;
    }
}
use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Packet};
use std::time::Duration;
use std::sync::Arc;
use bollard::Docker;
use bollard::container::StatsOptions;
use futures_util::stream::StreamExt;
use dashmap::DashMap;

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

// TODO(B1-05): this whole struct + the hardcoded list below moves into TOML config
#[derive(Debug, Clone)]
struct BrokerConfig {
    id: String,
    host: String,
    port: u16,
    container_name: String,
}

// Shared state, keyed by broker_id. Every task below writes into this;
// nothing reads it yet (that's B1-06's GET /metrics endpoint).
type SharedState = Arc<DashMap<String, BrokerMetrics>>;

async fn run_mqtt_task(broker: BrokerConfig, state: SharedState) {
    let mut mqttoptions = MqttOptions::new(
        format!("cloud-monitoring-agent-{}", broker.id),
        broker.host.clone(),
        broker.port,
    );
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    if let Err(e) = client.subscribe("$SYS/#", QoS::AtMostOnce).await {
        eprintln!("[{}] failed to subscribe to $SYS/#: {:?}", broker.id, e);
        return;
    }

    println!("[{}] agent subscribed to $SYS/# on {}:{}", broker.id, broker.host, broker.port);

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
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn run_docker_task(broker: BrokerConfig, state: SharedState, docker: Docker) {
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

        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

#[tokio::main]
async fn main() {
    // TODO(B1-05): load this list from TOML instead of hardcoding
    let brokers = vec![
        BrokerConfig {
            id: "broker-1".to_string(),
            host: "localhost".to_string(),
            port: 1883,
            container_name: "mosquitto".to_string(),
        },
        BrokerConfig {
            id: "broker-2".to_string(),
            host: "localhost".to_string(),
            port: 1884,
            container_name: "mosquitto2".to_string(),
        },
    ];

    let state: SharedState = Arc::new(DashMap::new());
    let docker = Docker::connect_with_local_defaults().expect("failed to connect to Docker socket");

    println!("agent starting up, spawning tasks for {} broker(s)", brokers.len());

    let mut handles = Vec::new();

    for broker in brokers {
        // One MQTT task + one Docker task per broker, each with its own
        // clone of the shared state and its own clone of the Docker client
        // (bollard's Docker handle is cheap to clone, backed by a shared connection).
        let mqtt_state = Arc::clone(&state);
        let mqtt_broker = broker.clone();
        handles.push(tokio::spawn(async move {
            run_mqtt_task(mqtt_broker, mqtt_state).await;
        }));

        let docker_state = Arc::clone(&state);
        let docker_clone = docker.clone();
        let docker_broker = broker.clone();
        handles.push(tokio::spawn(async move {
            run_docker_task(docker_broker, docker_state, docker_clone).await;
        }));
    }

    // Keep main alive; if any task panics, propagate that as a loud failure
    // rather than silently running with fewer brokers than expected.
    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("a broker task panicked: {:?}", e);
        }
    }
}
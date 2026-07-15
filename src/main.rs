use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Packet};
use std::time::Duration;
use bollard::Docker;
use bollard::container::StatsOptions;
use futures_util::stream::StreamExt;

#[derive(Debug, Default)]
struct BrokerMetrics {
    clients_connected: Option<u64>,
    messages_sent: Option<u64>,
    messages_received: Option<u64>,
    bytes_sent: Option<u64>,
    bytes_received: Option<u64>,
}

async fn poll_container_stats(docker: &Docker, container_name: &str) {
    let mut stream = docker.stats(
        container_name,
        Some(StatsOptions {
            stream: false, // one-shot snapshot per call, not a continuous stream
            ..Default::default()
        }),
    );

    if let Some(Ok(stats)) = stream.next().await {
        // --- CPU % ---
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

        // --- Memory ---
        let mem_usage_mb = stats.memory_stats.usage.unwrap_or(0) as f64 / (1024.0 * 1024.0);

        // --- Network I/O (sum across all interfaces) ---
        let (mut rx_bytes, mut tx_bytes) = (0u64, 0u64);
        if let Some(networks) = &stats.networks {
            for (_iface, net) in networks {
                rx_bytes += net.rx_bytes;
                tx_bytes += net.tx_bytes;
            }
        }

        println!(
            "Container stats -> cpu: {:.2}%, mem: {:.2} MB, net_rx: {} bytes, net_tx: {} bytes",
            cpu_percent, mem_usage_mb, rx_bytes, tx_bytes
        );
    }
}

#[tokio::main]
async fn main() {
    // TODO(B1-05): pull this from TOML config instead of hardcoding
    let broker_host = "localhost";
    let broker_port = 1883;

    let mut mqttoptions = MqttOptions::new("cloud-monitoring-agent", broker_host, broker_port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    client
        .subscribe("$SYS/#", QoS::AtMostOnce)
        .await
        .expect("failed to subscribe to $SYS/#");

    println!("agent starting up, subscribed to $SYS/#");

    // --- NEW: B1-02 Docker stats polling, spawned as its own task ---
    let container_name = "mosquitto"; // TODO(B1-05): configurable
    let docker = Docker::connect_with_local_defaults().expect("failed to connect to Docker socket");
    tokio::spawn(async move {
        loop {
            poll_container_stats(&docker, container_name).await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });
    // --- end new block ---

    let mut metrics = BrokerMetrics::default();

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                let topic = publish.topic.as_str();
                let payload = String::from_utf8_lossy(&publish.payload);

                match topic {
                    "$SYS/broker/clients/connected" => {
                        metrics.clients_connected = payload.trim().parse().ok();
                    }
                    "$SYS/broker/messages/sent" => {
                        metrics.messages_sent = payload.trim().parse().ok();
                    }
                    "$SYS/broker/messages/received" => {
                        metrics.messages_received = payload.trim().parse().ok();
                    }
                    "$SYS/broker/bytes/sent" => {
                        metrics.bytes_sent = payload.trim().parse().ok();
                    }
                    "$SYS/broker/bytes/received" => {
                        metrics.bytes_received = payload.trim().parse().ok();
                    }
                    _ => {} // ignore other $SYS topics for now
                }

                println!("{:?}", metrics);
            }
            Ok(_) => {} // other event types (ConnAck, PubAck, etc.) — ignore for now
            Err(e) => {
                eprintln!("MQTT event loop error: {:?}", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}
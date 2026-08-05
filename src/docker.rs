use bollard::container::StatsOptions;
use bollard::Docker;
use chrono::Utc;
use futures_util::stream::StreamExt;
use std::time::Duration;

use crate::config::BrokerConfig;
use crate::types::SharedState;

pub async fn run_docker_task(
    broker: BrokerConfig,
    state: SharedState,
    docker: Docker,
    scrape_interval_secs: u64,
) {
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

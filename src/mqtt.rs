use chrono::Utc;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;

use crate::config::BrokerConfig;
use crate::types::SharedState;

pub async fn run_mqtt_task(broker: BrokerConfig, state: SharedState, scrape_interval_secs: u64) {
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

use bollard::container::ListContainersOptions;
use bollard::Docker;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::config::BrokerConfig;
use crate::registry::{self, BrokerRuntime};
use crate::types::{BrokerRegistry};

const MONITOR_LABEL: &str = "progressbox.monitor";
const BROKER_ID_LABEL: &str = "progressbox.broker_id";

/// Inspects one labeled container and builds a BrokerConfig from it.
/// Returns None (and logs) if required labels/ports are missing or malformed —
/// a misconfigured container should never crash discovery for everyone else.
fn broker_config_from_container(
    container: &bollard::models::ContainerSummary,
) -> Option<BrokerConfig> {
    let labels = container.labels.as_ref()?;
    let broker_id = labels.get(BROKER_ID_LABEL)?.clone();

    let container_name = container
        .names
        .as_ref()?
        .first()?
        .trim_start_matches('/')
        .to_string();

    // Find the host-side port bound to the container's 1883/tcp.
    let mqtt_port = container
        .ports
        .as_ref()?
        .iter()
        .find(|p| p.private_port == 1883)
        .and_then(|p| p.public_port)?;

    Some(BrokerConfig {
        id: broker_id,
        mqtt_host: "localhost".to_string(), // agent and containers share the host network
        mqtt_port,
        container_name,
    })
}

/// One reconciliation pass: list labeled containers, diff against the
/// registry, spawn new ones, stop vanished ones. Never touches brokers
/// that were registered manually via POST /brokers and don't carry the
/// discovery label — those remain under separate control.
async fn reconcile(docker: &Docker, runtime: &BrokerRuntime, registry: &BrokerRegistry) {
    let mut filters = HashMap::new();
    filters.insert("label".to_string(), vec![format!("{}=true", MONITOR_LABEL)]);

    let containers = match docker
        .list_containers(Some(ListContainersOptions {
            all: false, // only running containers — a stopped one shouldn't stay "online"
            filters,
            ..Default::default()
        }))
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[discovery] failed to list containers: {:?}", e);
            return;
        }
    };

    let discovered: HashMap<String, BrokerConfig> = containers
        .iter()
        .filter_map(broker_config_from_container)
        .map(|b| (b.id.clone(), b))
        .collect();

    let currently_registered: HashSet<String> =
        registry::list_brokers(registry).into_iter().collect();

    // New: discovered but not registered.
    for (id, broker) in &discovered {
        if !currently_registered.contains(id) {
            if registry::is_suppressed(id) {
                // A human explicitly DELETEd this broker and its label is
                // still present. Respect that until the label/container
                // actually disappears — don't just resync it back in.
                println!("[discovery] broker '{}' is suppressed (manually deleted), not re-registering", id);
            } else {
                println!("[discovery] found new broker '{}', registering", id);
                registry::spawn_broker(broker.clone(), runtime, registry);
            }
        }
        // Record every id we currently see labeled, whether or not it was
        // just spawned. This is what makes `was_previously_discovered` mean
        // something — without it the set stays empty forever and vanished
        // brokers are never cleaned up (the bug this fixes).
        mark_discovered(id);
    }

    // Vanished: registered but no longer discovered.
    // Only remove brokers this loop itself would have added — i.e. skip
    // anything that's *not* label-discoverable and was likely added via
    // POST /brokers manually. We approximate this by only ever removing
    // ids that were present in a previous discovered set.
    for id in &currently_registered {
        if !discovered.contains_key(id) && was_previously_discovered(id) {
            println!("[discovery] broker '{}' no longer present, deregistering", id);
            // Not a manual delete — the container/label genuinely vanished —
            // so use the non-suppressing stop. If a broker with this id
            // shows up again later it should be treated as brand new.
            registry::stop_broker_no_suppress(id, runtime.state.clone(), registry);
            // The container is gone — stop tracking it too, so if a broker
            // with the same id is later added manually via POST /brokers,
            // a stray label-container reappearing later is treated as
            // genuinely new rather than silently interacting with old state.
            unmark_discovered(id);
        }
    }

    // Clear suppression once a container's label/existence actually goes
    // away — a *new* container that happens to reuse this id later should
    // be treated as genuinely new, not permanently blocked by an old delete.
    for id in registry::suppressed_ids() {
        if !discovered.contains_key(&id) {
            println!("[discovery] suppressed broker '{}' no longer labeled/present, clearing suppression", id);
            registry::unsuppress(&id);
        }
    }
}

// Tracks ids this task itself has ever spawned, so it never deregisters
// a broker that a human added via POST /brokers.
use std::sync::Mutex;
use std::sync::OnceLock;
fn discovered_ids() -> &'static Mutex<HashSet<String>> {
    static IDS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    IDS.get_or_init(|| Mutex::new(HashSet::new()))
}
fn was_previously_discovered(id: &str) -> bool {
    discovered_ids().lock().unwrap().contains(id)
}
fn mark_discovered(id: &str) {
    discovered_ids().lock().unwrap().insert(id.to_string());
}
fn unmark_discovered(id: &str) {
    discovered_ids().lock().unwrap().remove(id);
}

pub async fn run_discovery_task(docker: Docker, runtime: BrokerRuntime, registry: BrokerRegistry, interval_secs: u64) {
    loop {
        reconcile(&docker, &runtime, &registry).await;
        tokio::time::sleep(Duration::from_secs(interval_secs)).await;
    }
}
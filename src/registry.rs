use bollard::Docker;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use crate::config::BrokerConfig;
use crate::types::{BrokerRegistry, SharedState};
use crate::{docker as docker_task, mqtt};

/// Everything needed to spawn or stop a broker's monitoring tasks.
/// Bundled into one struct so both main() at startup and the HTTP
/// handlers at runtime call spawn_broker with identical context.
#[derive(Clone)]
pub struct BrokerRuntime {
    pub state: SharedState,
    pub docker: Docker,
    pub mqtt_interval: u64,
    pub docker_interval: u64,
}

pub fn new_registry() -> BrokerRegistry {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Spawns the MQTT and Docker tasks for one broker and registers the
/// resulting handles under its id. If a broker with this id is already
/// registered, its old handles are aborted first — this makes the
/// function idempotent, so re-registering the same id safely replaces
/// rather than leaking a duplicate set of tasks.
///
/// Explicit (re)registration — whether via config.toml at startup or a
/// direct POST /brokers call — always wins over a prior manual delete,
/// so this also clears any suppression for the id.
pub fn spawn_broker(broker: BrokerConfig, runtime: &BrokerRuntime, registry: &BrokerRegistry) {
    let broker_id = broker.id.clone();

    // Replace, don't leak — abort any existing tasks for this id first.
    stop_broker_internal(&broker_id, runtime.state.clone(), registry);
    unsuppress(&broker_id);

    let mut handles = Vec::with_capacity(2);

    let state_clone = runtime.state.clone();
    let broker_clone = broker.clone();
    let mqtt_interval = runtime.mqtt_interval;
    handles.push(tokio::spawn(async move {
        mqtt::run_mqtt_task(broker_clone, state_clone, mqtt_interval).await;
    }));

    let state_clone = runtime.state.clone();
    let broker_clone = broker.clone();
    let docker_clone = runtime.docker.clone();
    let docker_interval = runtime.docker_interval;
    handles.push(tokio::spawn(async move {
        docker_task::run_docker_task(broker_clone, state_clone, docker_clone, docker_interval).await;
    }));

    registry.lock().unwrap().insert(broker_id.clone(), handles);
    println!("[registry] broker '{}' registered and tasks spawned", broker_id);
}

/// Aborts the running tasks for a broker and clears its metrics entry so
/// GET /metrics stops showing it immediately rather than serving stale data.
/// Returns true if a broker with this id was actually found and stopped.
///
/// This is the public entry point used by DELETE /brokers/{id} and by
/// discovery's own "vanished" cleanup. It marks the id as suppressed —
/// callers that want a non-suppressing stop (e.g. spawn_broker replacing
/// an existing broker) should use `stop_broker_internal` instead.
pub fn stop_broker(broker_id: &str, state: SharedState, registry: &BrokerRegistry) -> bool {
    let stopped = stop_broker_internal(broker_id, state, registry);
    if stopped {
        suppress(broker_id);
    }
    stopped
}

/// Same as `stop_broker` but does not touch suppression state. Used
/// internally when replacing a broker's tasks (spawn_broker), and publicly
/// by discovery when it deregisters a broker whose container/label
/// genuinely vanished — neither case is a "manual delete" that should
/// block future re-discovery.
pub fn stop_broker_no_suppress(broker_id: &str, state: SharedState, registry: &BrokerRegistry) -> bool {
    stop_broker_internal(broker_id, state, registry)
}

fn stop_broker_internal(broker_id: &str, state: SharedState, registry: &BrokerRegistry) -> bool {
    let handles = registry.lock().unwrap().remove(broker_id);
    match handles {
        Some(handles) => {
            for h in handles {
                h.abort();
            }
            state.remove(broker_id);
            println!("[registry] broker '{}' deregistered and tasks stopped", broker_id);
            true
        }
        None => false,
    }
}

/// Snapshot of currently registered broker ids — used by GET /brokers.
pub fn list_brokers(registry: &BrokerRegistry) -> Vec<String> {
    registry.lock().unwrap().keys().cloned().collect()
}

// ── suppression: ids a human explicitly DELETEd via the API ─────────────────
// Discovery consults this before re-registering a still-labeled container,
// so a manual delete sticks until the label/container actually disappears
// (at which point discovery clears the suppression itself) or someone
// explicitly re-registers the id (POST /brokers, or it reappears in
// config.toml on restart).
fn suppressed() -> &'static Mutex<HashSet<String>> {
    static IDS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    IDS.get_or_init(|| Mutex::new(HashSet::new()))
}

pub fn suppress(id: &str) {
    suppressed().lock().unwrap().insert(id.to_string());
}

pub fn unsuppress(id: &str) {
    suppressed().lock().unwrap().remove(id);
}

pub fn is_suppressed(id: &str) -> bool {
    suppressed().lock().unwrap().contains(id)
}

pub fn suppressed_ids() -> Vec<String> {
    suppressed().lock().unwrap().iter().cloned().collect()
}
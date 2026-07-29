use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub influxdb: InfluxConfig,
    pub intervals: IntervalsConfig,
    pub brokers: Vec<BrokerConfig>,
    #[serde(default)]
    pub alert_rules: Vec<AlertRule>,
}

#[derive(Debug, Deserialize)]
pub struct InfluxConfig {
    pub url: String,
    pub org: String,
    pub bucket: String,
    pub token: String,
}

#[derive(Debug, Deserialize)]
pub struct IntervalsConfig {
    pub mqtt_scrape_secs: u64,
    pub docker_scrape_secs: u64,
    pub influx_write_secs: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BrokerConfig {
    pub id: String,
    pub mqtt_host: String,
    pub mqtt_port: u16,
    pub container_name: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AlertRule {
    pub metric: String,
    pub operator: String,
    pub threshold: f64,
    pub cooldown_secs: u64,
}

// ── B2-01: validation ────────────────────────────────────────────────────────
// Every metric an alert rule is allowed to reference must exist on
// `BrokerMetrics` (main.rs) and be numeric — this list is the single source
// of truth both config validation and the future alert-evaluation task
// (B2-02) should read from.
const VALID_METRICS: &[&str] = &[
    "clients_connected",
    "messages_sent",
    "messages_received",
    "bytes_sent",
    "bytes_received",
    "cpu_percent",
    "mem_usage_mb",
    "net_rx_bytes",
    "net_tx_bytes",
];

const VALID_OPERATORS: &[&str] = &[">", "<", "=="];

impl AlertRule {
    fn validate(&self, index: usize) -> Result<(), String> {
        if !VALID_METRICS.contains(&self.metric.as_str()) {
            return Err(format!(
                "alert_rules[{}]: unknown metric '{}' (valid metrics: {})",
                index,
                self.metric,
                VALID_METRICS.join(", ")
            ));
        }
        if !VALID_OPERATORS.contains(&self.operator.as_str()) {
            return Err(format!(
                "alert_rules[{}]: invalid operator '{}' (valid operators: {})",
                index,
                self.operator,
                VALID_OPERATORS.join(", ")
            ));
        }
        if self.cooldown_secs == 0 {
            return Err(format!(
                "alert_rules[{}]: cooldown_secs must be greater than 0",
                index
            ));
        }
        Ok(())
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Self, String> {
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("failed to read config file '{}': {}", path, e))?;
        let config: Config = toml::from_str(&raw)
            .map_err(|e| format!("invalid config in '{}': {}", path, e))?;

        // ── B2-01: reject unknown metrics or bad operators at load time ──────
        for (i, rule) in config.alert_rules.iter().enumerate() {
            rule.validate(i)?;
        }

        Ok(config)
    }
}
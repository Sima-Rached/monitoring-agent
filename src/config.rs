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

#[derive(Debug, Deserialize)]
pub struct AlertRule {
    pub metric: String,
    pub operator: String,
    pub threshold: f64,
    pub cooldown_secs: u64,
}

impl Config {
    pub fn load(path: &str) -> Result<Self, String> {
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("failed to read config file '{}': {}", path, e))?;

        toml::from_str(&raw)
            .map_err(|e| format!("invalid config in '{}': {}", path, e))
    }
}
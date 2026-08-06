use serde::Deserialize;
use std::fs;

#[derive(Debug)]
pub struct Config {
    pub influxdb: InfluxConfig,
    pub intervals: IntervalsConfig,
    pub brokers: Vec<BrokerConfig>,
    pub email: EmailConfig,
    // alert_rules removed — now live in rules.toml, loaded via RulesConfig::load()
}

#[derive(Debug, Deserialize, Clone)]
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

// ── B2-03: email config ───────────────────────────────────────────────────────
// smtp_host, smtp_port, from, to come from config.toml (safe to commit).
// username and password are injected from env vars at load time — never
// written to disk.
#[derive(Debug, Clone)]
pub struct EmailConfig {
    pub smtp_host: String,
    pub smtp_port: u16,
    pub from: String,
    pub to: Vec<String>,
    pub username: String,   // from SMTP_USERNAME env var
    pub password: String,   // from SMTP_PASSWORD env var
}

// Intermediate struct that serde deserializes into —
// does not include credentials (those come from env).
#[derive(Debug, Deserialize)]
struct EmailConfigRaw {
    smtp_host: String,
    smtp_port: u16,
    from: String,
    to: Vec<String>,
}

// ── Alert rule validation ─────────────────────────────────────────────────────
// Every metric an alert rule is allowed to reference must exist on
// `BrokerMetrics` and be numeric — this list is the single source of truth
// for both config validation and the alert-evaluation task.
pub const VALID_METRICS: &[&str] = &[
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

pub const VALID_OPERATORS: &[&str] = &[">", "<", "=="];

impl AlertRule {
    pub fn validate(&self, index: usize) -> Result<(), String> {
        if !VALID_METRICS.contains(&self.metric.as_str()) {
            return Err(format!(
                "alert_rules[{}]: unknown metric '{}' (valid: {})",
                index,
                self.metric,
                VALID_METRICS.join(", ")
            ));
        }
        if !VALID_OPERATORS.contains(&self.operator.as_str()) {
            return Err(format!(
                "alert_rules[{}]: invalid operator '{}' (valid: {})",
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

// ── RulesConfig ───────────────────────────────────────────────────────────────
// Loaded from a separate file (default: rules.toml, override with RULES_PATH
// env var). Called at startup and again on every POST /reload — the reload
// handler rejects bad files and keeps the old rules active, so a typo in
// rules.toml never silently kills alerting.
#[derive(Debug, Deserialize)]
struct RulesFile {
    #[serde(default)]
    alert_rules: Vec<AlertRule>,
}

pub struct RulesConfig {
    pub alert_rules: Vec<AlertRule>,
}

impl RulesConfig {
    pub fn load(path: &str) -> Result<Self, String> {
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("failed to read rules file '{}': {}", path, e))?;

        let parsed: RulesFile = toml::from_str(&raw)
            .map_err(|e| format!("invalid rules in '{}': {}", path, e))?;

        for (i, rule) in parsed.alert_rules.iter().enumerate() {
            rule.validate(i)?;
        }

        Ok(RulesConfig { alert_rules: parsed.alert_rules })
    }
}

// Intermediate top-level struct for config.toml — alert_rules no longer here.
#[derive(Debug, Deserialize)]
struct ConfigRaw {
    influxdb: InfluxConfig,
    intervals: IntervalsConfig,
    brokers: Vec<BrokerConfig>,
    email: EmailConfigRaw,
}

impl Config {
    pub fn load(path: &str) -> Result<Self, String> {
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("failed to read config file '{}': {}", path, e))?;

        let raw_cfg: ConfigRaw = toml::from_str(&raw)
            .map_err(|e| format!("invalid config in '{}': {}", path, e))?;

        // Read credentials from env — fail fast if missing.
        let username = std::env::var("SMTP_USERNAME")
            .map_err(|_| "missing env var SMTP_USERNAME".to_string())?;
        let password = std::env::var("SMTP_PASSWORD")
            .map_err(|_| "missing env var SMTP_PASSWORD".to_string())?;

        Ok(Config {
            influxdb: raw_cfg.influxdb,
            intervals: raw_cfg.intervals,
            brokers: raw_cfg.brokers,
            email: EmailConfig {
                smtp_host: raw_cfg.email.smtp_host,
                smtp_port: raw_cfg.email.smtp_port,
                from: raw_cfg.email.from,
                to: raw_cfg.email.to,
                username,
                password,
            },
        })
    }
}
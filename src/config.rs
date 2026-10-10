//! Runtime configuration, read once from environment variables and CLI flags.

use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
    pub home: PathBuf,
    pub listen: String,
    pub password: Option<String>,
    pub relay_enabled: bool,
    /// Extra DNS names accepted in the Host header (`PASEO_HOSTNAMES`); a
    /// leading dot matches any subdomain. IPs and localhost are always allowed.
    pub hostnames: Vec<String>,
    pub relay_endpoint: String,
    pub relay_use_tls: bool,
    pub relay_public_endpoint: String,
    pub relay_public_use_tls: bool,
    pub app_base_url: String,
    pub claude_bin: String,
    pub codex_bin: String,
    /// How long an idle `claude` process is kept warm before it is reaped.
    pub claude_idle: Duration,
    /// Hard cap on simultaneously live `claude` processes across all projects.
    pub claude_max_live: usize,
    /// How long the shared `codex app-server` stays up with no active turns.
    pub codex_idle: Duration,
    /// Timelines of agents untouched for this long are dropped from memory.
    pub timeline_unload: Duration,
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

fn env_bool(key: &str) -> Option<bool> {
    env(key).map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

fn env_secs(key: &str, default: u64) -> Duration {
    Duration::from_secs(env(key).and_then(|v| v.parse().ok()).unwrap_or(default))
}

impl Config {
    pub fn load(args: &[String]) -> Config {
        let home = env("PASEO_LITE_HOME").map(PathBuf::from).unwrap_or_else(|| {
            let base = env("HOME").unwrap_or_else(|| ".".into());
            PathBuf::from(base).join(".paseo-lite")
        });
        let mut listen = env("PASEO_LISTEN").unwrap_or_else(|| "127.0.0.1:6767".into());
        let mut relay_enabled = env_bool("PASEO_RELAY_ENABLED").or_else(|| env_bool("PASEO_RELAY")).unwrap_or(true);
        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--listen" if i + 1 < args.len() => {
                    listen = args[i + 1].clone();
                    i += 1;
                }
                "--no-relay" => relay_enabled = false,
                _ => {}
            }
            i += 1;
        }
        let relay_endpoint = env("PASEO_RELAY_ENDPOINT").unwrap_or_else(|| "relay.paseo.sh:443".into());
        let default_tls = relay_endpoint.ends_with(":443");
        let relay_use_tls = env_bool("PASEO_RELAY_USE_TLS").unwrap_or(default_tls);
        let relay_public_endpoint = env("PASEO_RELAY_PUBLIC_ENDPOINT").unwrap_or_else(|| relay_endpoint.clone());
        let relay_public_use_tls = env_bool("PASEO_RELAY_PUBLIC_USE_TLS").unwrap_or(relay_use_tls);
        Config {
            home,
            listen,
            password: env("PASEO_PASSWORD"),
            relay_enabled,
            hostnames: env("PASEO_HOSTNAMES")
                .map(|v| v.split(',').map(|h| h.trim().to_ascii_lowercase()).filter(|h| !h.is_empty()).collect())
                .unwrap_or_default(),
            relay_endpoint,
            relay_use_tls,
            relay_public_endpoint,
            relay_public_use_tls,
            app_base_url: env("PASEO_APP_BASE_URL").unwrap_or_else(|| "https://app.paseo.sh".into()),
            claude_bin: env("PASEO_CLAUDE_BIN").unwrap_or_else(|| "claude".into()),
            codex_bin: env("PASEO_CODEX_BIN").unwrap_or_else(|| "codex".into()),
            claude_idle: env_secs("PASEO_CLAUDE_IDLE_SECS", 90),
            claude_max_live: env("PASEO_CLAUDE_MAX_LIVE").and_then(|v| v.parse().ok()).unwrap_or(3).max(1),
            codex_idle: env_secs("PASEO_CODEX_IDLE_SECS", 300),
            timeline_unload: env_secs("PASEO_TIMELINE_UNLOAD_SECS", 600),
        }
    }
}

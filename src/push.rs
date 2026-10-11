//! Phone notifications, same as upstream: the app registers an Expo push
//! token (`register_push_token`, renewed on a 48h lease) and the daemon posts
//! "Agent finished" / "Agent needs permission" to Expo's push API when no
//! client is actively looking at the app.

use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const LEASE_MS: u64 = 48 * 60 * 60 * 1000;
const EXPO_HOST: &str = "exp.host";
const EXPO_PATH: &str = "/--/api/v2/push/send";
const PREVIEW_LIMIT: usize = 220;
/// A client counts as present (gets an in-app notice instead of a push)
/// when it reported activity this recently.
pub const PRESENCE_WINDOW: Duration = Duration::from_secs(180);

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Push tokens with lease expiry, persisted in `push-tokens.json`.
pub struct TokenStore {
    path: PathBuf,
    tokens: HashMap<String, u64>,
}

impl TokenStore {
    pub fn load(home: &std::path::Path) -> TokenStore {
        let path = home.join("push-tokens.json");
        let tokens = std::fs::read(&path).ok().and_then(|b| serde_json::from_slice::<HashMap<String, u64>>(&b).ok()).unwrap_or_default();
        TokenStore { path, tokens }
    }

    fn save(&self) {
        if let Ok(data) = serde_json::to_vec(&self.tokens) {
            if let Err(e) = crate::util::write_atomic(&self.path, &data, true) {
                crate::warn!("could not save push tokens: {e}");
            }
        }
    }

    pub fn renew(&mut self, token: &str) {
        let token = token.trim();
        if token.is_empty() {
            return;
        }
        let now = now_ms();
        // Skip the disk write while more than half the lease remains.
        if self.tokens.get(token).map(|exp| exp.saturating_sub(now) > LEASE_MS / 2).unwrap_or(false) {
            return;
        }
        self.tokens.insert(token.to_string(), now + LEASE_MS);
        self.save();
    }

    pub fn revoke(&mut self, token: &str) {
        if self.tokens.remove(token.trim()).is_some() {
            self.save();
        }
    }

    pub fn active(&mut self) -> Vec<String> {
        let now = now_ms();
        let before = self.tokens.len();
        self.tokens.retain(|_, exp| *exp > now);
        if self.tokens.len() != before {
            self.save();
        }
        self.tokens.keys().cloned().collect()
    }
}

/// Markdown-ish assistant text -> one short plain line for a notification.
pub fn preview(text: &str) -> Option<String> {
    let plain: String = text
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.starts_with("```") && !l.starts_with("~~~"))
        .map(|l| l.trim_start_matches(|c| c == '#' || c == '>').trim_start())
        .map(|l| l.strip_prefix("- ").or_else(|| l.strip_prefix("* ")).unwrap_or(l))
        .collect::<Vec<_>>()
        .join(" ")
        .replace(['*', '`'], "");
    let plain = plain.split_whitespace().collect::<Vec<_>>().join(" ");
    if plain.is_empty() {
        return None;
    }
    if plain.chars().count() <= PREVIEW_LIMIT {
        return Some(plain);
    }
    let cut: String = plain.chars().take(PREVIEW_LIMIT - 3).collect();
    Some(format!("{}...", cut.trim_end()))
}

/// Upstream's `buildAgentAttentionNotificationPayload`.
pub fn attention_payload(reason: &str, server_id: &str, workspace_id: &str, agent_id: &str, detail: Option<&str>) -> Value {
    let title = match reason {
        "permission" => "Agent needs permission",
        "error" => "Agent needs attention",
        _ => "Agent finished",
    };
    let fallback = match reason {
        "permission" => "Permission requested.",
        "error" => "Encountered an error.",
        _ => "Finished working.",
    };
    let body = if reason == "error" { None } else { detail.and_then(preview) }.unwrap_or_else(|| fallback.to_string());
    json!({ "title": title, "body": body, "data": { "serverId": server_id, "workspaceId": workspace_id, "agentId": agent_id, "reason": reason } })
}

/// Send one notification to every token. Returns tokens Expo reports as
/// dead (DeviceNotRegistered) so they can be dropped.
pub async fn send(tokens: Vec<String>, payload: Value) -> Vec<String> {
    let mut dead = Vec::new();
    for batch in tokens.chunks(100) {
        let messages: Vec<Value> = batch
            .iter()
            .map(|t| json!({ "to": t, "title": payload["title"], "body": payload["body"], "data": payload["data"], "sound": "default" }))
            .collect();
        match post_json(EXPO_HOST, EXPO_PATH, &Value::Array(messages).to_string()).await {
            Ok((200, body)) => {
                let tickets = serde_json::from_str::<Value>(&body).ok().and_then(|v| v.get("data").and_then(|d| d.as_array()).cloned()).unwrap_or_default();
                for (token, ticket) in batch.iter().zip(tickets.iter()) {
                    if ticket.get("status").and_then(|s| s.as_str()) == Some("error") {
                        let code = ticket.pointer("/details/error").and_then(|e| e.as_str()).unwrap_or("");
                        crate::warn!("push to a device failed: {}", ticket.get("message").and_then(|m| m.as_str()).unwrap_or(code));
                        // InvalidCredentials is a project-level problem, not a dead device.
                        if code == "DeviceNotRegistered" {
                            dead.push(token.clone());
                        }
                    }
                }
            }
            Ok((status, body)) => crate::warn!("expo push API returned {status}: {}", crate::util::truncate(&body, 200)),
            Err(e) => crate::warn!("push notification failed: {e}"),
        }
    }
    dead
}

/// Minimal HTTPS POST (HTTP/1.1, `Connection: close`) returning status and body.
async fn post_json(host: &str, path: &str, body: &str) -> Result<(u16, String), String> {
    let work = async {
        let tcp = tokio::net::TcpStream::connect((host, 443)).await.map_err(|e| e.to_string())?;
        let _ = tcp.set_nodelay(true);
        let name = rustls::pki_types::ServerName::try_from(host.to_string()).map_err(|e| e.to_string())?;
        let mut tls = tokio_rustls::TlsConnector::from(crate::relay::tls_config()).connect(name, tcp).await.map_err(|e| e.to_string())?;
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nAccept: application/json\r\nUser-Agent: paseo-lite/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            env!("CARGO_PKG_VERSION"),
            body.len()
        );
        tls.write_all(request.as_bytes()).await.map_err(|e| e.to_string())?;
        let mut raw = Vec::new();
        // Servers may skip TLS close_notify on Connection: close; keep what arrived.
        let _ = (&mut tls).take(256 * 1024).read_to_end(&mut raw).await;
        parse_response(&raw)
    };
    tokio::time::timeout(Duration::from_secs(20), work).await.map_err(|_| "timed out".to_string())?
}

fn parse_response(raw: &[u8]) -> Result<(u16, String), String> {
    let text = String::from_utf8_lossy(raw);
    let (head, body) = text.split_once("\r\n\r\n").ok_or("malformed HTTP response")?;
    let status = head.split_whitespace().nth(1).and_then(|s| s.parse().ok()).ok_or("malformed status line")?;
    let chunked = head.lines().any(|l| l.to_ascii_lowercase().starts_with("transfer-encoding:") && l.to_ascii_lowercase().contains("chunked"));
    Ok((status, if chunked { dechunk(body) } else { body.to_string() }))
}

fn dechunk(mut rest: &str) -> String {
    let mut out = String::new();
    while let Some((size_line, after)) = rest.split_once("\r\n") {
        let Ok(size) = usize::from_str_radix(size_line.split(';').next().unwrap_or("").trim(), 16) else { break };
        let (Some(chunk), Some(next)) = (after.get(..size), after.get(size..)) else { break };
        if size == 0 {
            break;
        }
        out.push_str(chunk);
        rest = next.trim_start_matches("\r\n");
    }
    out
}

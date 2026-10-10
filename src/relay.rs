//! Outbound relay transport, protocol v2 (same as `relay-transport.ts`):
//! - one control socket: `/ws?serverId=..&role=server&v=2`, carrying
//!   `sync` / `connected` / `disconnected` / `ping` JSON messages;
//! - one data socket per client connection (`&connectionId=..`), on which the
//!   client performs the `e2ee_hello` / `e2ee_ready` handshake and then sends
//!   NaCl-box encrypted frames. The relay only ever sees ciphertext.

use crate::config::Config;
use crate::e2ee::{Channel, DaemonIdentity};
use crate::session::{self, Ctx};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;

const PING_INTERVAL: Duration = Duration::from_secs(10);
const STALE_AFTER: Duration = Duration::from_secs(30);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

fn url(cfg: &Config, server_id: &str, connection_id: Option<&str>) -> String {
    let scheme = if cfg.relay_use_tls { "wss" } else { "ws" };
    let enc = |s: &str| s.chars().filter(|c| c.is_ascii_alphanumeric() || "-_.~".contains(*c)).collect::<String>();
    let mut u = format!("{scheme}://{}/ws?serverId={}&role=server&v=2", cfg.relay_endpoint, enc(server_id));
    if let Some(c) = connection_id {
        u.push_str("&connectionId=");
        u.push_str(&enc(c));
    }
    u
}

type DataSockets = Arc<Mutex<HashMap<String, JoinHandle<()>>>>;

pub async fn run(ctx: Arc<Ctx>, cfg: Config, identity: Arc<DaemonIdentity>) {
    let data: DataSockets = Arc::new(Mutex::new(HashMap::new()));
    let mut attempt: u64 = 0;
    loop {
        match control_session(&ctx, &cfg, &identity, &data).await {
            Ok(()) => attempt = 0,
            Err(e) => {
                attempt += 1;
                crate::warn!("relay control disconnected: {e}");
            }
        }
        let delay = Duration::from_secs((attempt.max(1)).min(30));
        tokio::time::sleep(delay).await;
    }
}

async fn control_session(ctx: &Arc<Ctx>, cfg: &Config, identity: &Arc<DaemonIdentity>, data: &DataSockets) -> Result<(), String> {
    let target = url(cfg, &identity.server_id, None);
    let (ws, _) = tokio::time::timeout(Duration::from_secs(10), tokio_tungstenite::connect_async(target.as_str()))
        .await
        .map_err(|_| "connect timeout".to_string())?
        .map_err(|e| e.to_string())?;
    let (mut sink, mut stream) = ws.split();
    let mut last_seen = Instant::now();
    let mut ping = tokio::time::interval(PING_INTERVAL);
    let mut ready = false;
    loop {
        tokio::select! {
            _ = ping.tick() => {
                if last_seen.elapsed() > STALE_AFTER {
                    return Err("control socket stale".into());
                }
                sink.send(Message::Ping(Vec::new())).await.map_err(|e| e.to_string())?;
            }
            frame = stream.next() => {
                let frame = match frame {
                    Some(Ok(f)) => f,
                    Some(Err(e)) => return Err(e.to_string()),
                    None => return Err("closed".into()),
                };
                last_seen = Instant::now();
                let text = match frame {
                    Message::Text(t) => t,
                    Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                    Message::Close(_) => return Err("closed by relay".into()),
                    _ => continue,
                };
                let Ok(msg) = serde_json::from_str::<Value>(&text) else { continue };
                if !ready {
                    ready = true;
                    crate::log!("relay connected ({})", cfg.relay_endpoint);
                }
                match msg.get("type").and_then(|t| t.as_str()) {
                    Some("ping") => {
                        let pong = json!({ "type": "pong", "ts": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0) });
                        sink.send(Message::Text(pong.to_string())).await.map_err(|e| e.to_string())?;
                    }
                    Some("sync") => {
                        let ids: Vec<String> = msg.get("connectionIds").and_then(|v| v.as_array()).map(|a| a.iter().filter_map(|v| v.as_str()).map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()).unwrap_or_default();
                        for id in ids {
                            ensure_data_socket(ctx, cfg, identity, data, id);
                        }
                    }
                    Some("connected") => {
                        if let Some(id) = msg.get("connectionId").and_then(|v| v.as_str()).map(|s| s.trim().to_string()).filter(|s| !s.is_empty()) {
                            ensure_data_socket(ctx, cfg, identity, data, id);
                        }
                    }
                    Some("disconnected") => {
                        if let Some(id) = msg.get("connectionId").and_then(|v| v.as_str()) {
                            if let Some(handle) = data.lock().unwrap().remove(id.trim()) {
                                handle.abort();
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

fn ensure_data_socket(ctx: &Arc<Ctx>, cfg: &Config, identity: &Arc<DaemonIdentity>, data: &DataSockets, connection_id: String) {
    let mut map = data.lock().unwrap();
    map.retain(|_, h| !h.is_finished());
    if map.contains_key(&connection_id) {
        return;
    }
    let (ctx, cfg, identity, data2, id) = (ctx.clone(), cfg.clone(), identity.clone(), data.clone(), connection_id.clone());
    let handle = tokio::spawn(async move {
        if let Err(e) = data_socket(ctx, &cfg, identity, &id).await {
            crate::warn!("relay data socket {id} ended: {e}");
        }
        data2.lock().unwrap().remove(&id);
    });
    map.insert(connection_id, handle);
}

fn parse_handshake(text: &str) -> Option<(String, Option<String>)> {
    if !text.trim_start().starts_with('{') {
        return None;
    }
    let v: Value = serde_json::from_str(text).ok()?;
    let kind = v.get("type")?.as_str()?.to_string();
    let key = v.get("key").and_then(|k| k.as_str()).map(|k| k.to_string());
    (kind == "e2ee_hello" || kind == "e2ee_ready").then_some((kind, key))
}

fn ready_frame(binary_ciphertext: bool) -> Message {
    let ready = if binary_ciphertext {
        json!({ "type": "e2ee_ready", "capabilities": { "binaryCiphertext": true } })
    } else {
        json!({ "type": "e2ee_ready" })
    };
    Message::Text(ready.to_string())
}

async fn data_socket(ctx: Arc<Ctx>, cfg: &Config, identity: Arc<DaemonIdentity>, connection_id: &str) -> Result<(), String> {
    let target = url(cfg, &identity.server_id, Some(connection_id));
    let (ws, _) = tokio::time::timeout(HANDSHAKE_TIMEOUT, tokio_tungstenite::connect_async(target.as_str()))
        .await
        .map_err(|_| "open timeout".to_string())?
        .map_err(|e| e.to_string())?;
    let (mut sink, mut stream) = ws.split();

    // Handshake: first text frame must be the client's e2ee_hello.
    let (channel, peer_key, binary_ciphertext) = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
        while let Some(frame) = stream.next().await {
            let Message::Text(text) = frame.map_err(|e| e.to_string())? else { continue };
            let v: Value = serde_json::from_str(&text).map_err(|_| "invalid hello".to_string())?;
            if v.get("type").and_then(|t| t.as_str()) != Some("e2ee_hello") {
                return Err("invalid hello".to_string());
            }
            let key = v.get("key").and_then(|k| k.as_str()).ok_or("hello without key")?.trim().to_string();
            let binary = v.pointer("/capabilities/binaryCiphertext").and_then(|b| b.as_bool()).unwrap_or(false);
            let channel = Channel::new(&identity.secret, &key)?;
            return Ok((channel, key, binary));
        }
        Err("closed during handshake".to_string())
    })
    .await
    .map_err(|_| "handshake timeout".to_string())??;
    sink.send(ready_frame(binary_ciphertext)).await.map_err(|e| e.to_string())?;

    let (in_tx, in_rx) = tokio::sync::mpsc::channel::<String>(64);
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<Arc<str>>(256);
    let session = tokio::spawn(session::run(ctx, in_rx, out_tx, true));
    let result = loop {
        tokio::select! {
            out = out_rx.recv() => {
                let Some(out) = out else { break Ok(()) };
                // JSON is text plaintext => base64 ciphertext in a text frame.
                if let Err(e) = sink.send(Message::Text(channel.encrypt_text(&out))).await {
                    break Err(e.to_string());
                }
            }
            frame = stream.next() => {
                let frame = match frame {
                    Some(Ok(f)) => f,
                    Some(Err(e)) => break Err(e.to_string()),
                    None => break Ok(()),
                };
                let plaintext = match frame {
                    Message::Text(text) => {
                        if let Some((kind, key)) = parse_handshake(&text) {
                            if kind == "e2ee_hello" {
                                if key.as_deref().map(str::trim) == Some(peer_key.as_str()) {
                                    let _ = sink.send(ready_frame(binary_ciphertext)).await;
                                    continue;
                                }
                                let _ = sink.send(Message::Close(Some(CloseFrame { code: CloseCode::Policy, reason: "E2EE re-handshake key mismatch".into() }))).await;
                                break Err("re-handshake key mismatch".into());
                            }
                            continue;
                        }
                        if text.trim_start().starts_with('{') {
                            break Err("plaintext frame on encrypted channel".into());
                        }
                        channel.decrypt_text(&text)
                    }
                    Message::Binary(bytes) => channel.decrypt(&bytes).and_then(|p| String::from_utf8(p).map_err(|e| e.to_string())),
                    Message::Close(_) => break Ok(()),
                    _ => continue,
                };
                match plaintext {
                    Ok(text) => {
                        if in_tx.send(text).await.is_err() {
                            break Ok(());
                        }
                    }
                    Err(e) => {
                        let _ = sink.send(Message::Close(Some(CloseFrame { code: CloseCode::Error, reason: e.clone().into() }))).await;
                        break Err(e);
                    }
                }
            }
        }
    };
    session.abort();
    result
}

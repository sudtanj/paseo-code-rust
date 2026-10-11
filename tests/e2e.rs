//! End-to-end tests: start the real daemon binary on a free port with a
//! throwaway home, then talk to it over HTTP and WebSocket exactly like the
//! app does. A fake `claude` script stands in for the Claude Code CLI.

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

struct Daemon {
    child: Child,
    port: u16,
    dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn unique_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("paseo-lite-e2e-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A stand-in for `claude -p --input-format stream-json`: answers every user
/// message with one assistant message and a successful result.
fn fake_claude(dir: &std::path::Path) -> PathBuf {
    let path = dir.join("fake-claude.sh");
    let script = r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"type":"user"'*)
      printf '%s\n' '{"type":"assistant","message":{"id":"m1","role":"assistant","content":[{"type":"text","text":"hello from fake claude"}],"usage":{"input_tokens":3,"output_tokens":4}}}'
      printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"usage":{"input_tokens":3,"output_tokens":4},"total_cost_usd":0.0}'
      ;;
  esac
done
"#;
    std::fs::write(&path, script).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    path
}

fn start(tag: &str, password: Option<&str>) -> Daemon {
    let dir = unique_dir(tag);
    let port = free_port();
    let claude = fake_claude(&dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_paseo-lite"));
    cmd.args(["--listen", &format!("127.0.0.1:{port}"), "--no-relay"])
        .env("PASEO_LITE_HOME", dir.join("home"))
        .env("PASEO_CLAUDE_BIN", &claude)
        .env("PASEO_CODEX_BIN", "/nonexistent/codex")
        .env("PASEO_DEFAULT_SYSTEM_PROMPT", "")
        .env("PASEO_RETRY_MAX", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(p) = password {
        cmd.env("PASEO_PASSWORD", p);
    }
    let child = cmd.spawn().expect("start paseo-lite");
    let daemon = Daemon { child, port, dir };
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return daemon;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("daemon did not start listening on {port}");
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(port: u16) -> Ws {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws")).await.expect("ws connect");
    ws
}

async fn send(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string())).await.unwrap();
}

async fn recv(ws: &mut Ws) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(20), ws.next()).await.expect("timed out waiting for a frame") {
            Some(Ok(Message::Text(t))) => return serde_json::from_str(&t).unwrap(),
            Some(Ok(_)) => continue,
            other => panic!("socket closed: {other:?}"),
        }
    }
}

/// Read frames until `pick` returns something.
async fn recv_until<T>(ws: &mut Ws, mut pick: impl FnMut(&Value) -> Option<T>) -> T {
    loop {
        let v = recv(ws).await;
        if let Some(found) = pick(&v) {
            return found;
        }
    }
}

async fn hello(ws: &mut Ws, auth: Option<&str>) -> Value {
    let mut msg = json!({ "type": "hello", "protocolVersion": 1, "clientId": "test", "clientType": "cli" });
    if let Some(p) = auth {
        msg["auth"] = json!({ "password": p });
    }
    send(ws, msg).await;
    recv(ws).await
}

fn session(msg: Value) -> Value {
    json!({ "type": "session", "message": msg })
}

#[test]
fn serves_the_web_ui() {
    let d = start("http", None);
    let mut s = TcpStream::connect(("127.0.0.1", d.port)).unwrap();
    s.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").unwrap();
    let mut body = String::new();
    s.read_to_string(&mut body).unwrap();
    assert!(body.starts_with("HTTP/1.1 200"), "{body}");
    assert!(body.to_ascii_lowercase().contains("<html"));
}

#[tokio::test]
async fn handshake_announces_features() {
    let d = start("hello", None);
    let mut ws = connect(d.port).await;
    let info = hello(&mut ws, None).await;
    let payload = &info["message"]["payload"];
    assert_eq!(payload["status"], "server_info");
    assert_eq!(payload["features"]["projectAdd"], true);
    assert_eq!(payload["features"]["providerRemoval"], true);
}

#[tokio::test]
async fn rejects_incompatible_protocol_and_bad_password() {
    let d = start("auth", Some("secret"));
    let mut ws = connect(d.port).await;
    send(&mut ws, json!({ "type": "hello", "protocolVersion": 99 })).await;
    assert_eq!(recv(&mut ws).await["type"], "hello.rejected");

    let mut ws = connect(d.port).await;
    let r = hello(&mut ws, None).await;
    assert_eq!(r["type"], "hello.rejected");
    assert_eq!(r["reason"], "password_required");

    let mut ws = connect(d.port).await;
    let r = hello(&mut ws, Some("wrong")).await;
    assert_eq!(r["reason"], "incorrect_password");

    let mut ws = connect(d.port).await;
    let r = hello(&mut ws, Some("secret")).await;
    assert_eq!(r["message"]["payload"]["status"], "server_info");
}

#[tokio::test]
async fn unsupported_requests_get_an_rpc_error() {
    let d = start("rpcerr", None);
    let mut ws = connect(d.port).await;
    hello(&mut ws, None).await;
    send(&mut ws, session(json!({ "type": "create_terminal_request", "requestId": "r1" }))).await;
    let err = recv_until(&mut ws, |v| (v["message"]["type"] == "rpc_error").then(|| v["message"]["payload"].clone())).await;
    assert_eq!(err["requestId"], "r1");
}

#[tokio::test]
async fn daemon_config_custom_models_round_trip() {
    let d = start("config", None);
    let mut ws = connect(d.port).await;
    hello(&mut ws, None).await;
    let patch = json!({ "providers": { "claude": { "additionalModels": [{ "id": "my-custom-model", "label": "Mine" }] } } });
    send(&mut ws, session(json!({ "type": "set_daemon_config_request", "requestId": "c1", "config": patch }))).await;
    recv_until(&mut ws, |v| (v["message"]["type"] == "set_daemon_config_response").then_some(())).await;

    send(&mut ws, session(json!({ "type": "list_provider_models_request", "requestId": "c2", "provider": "claude" }))).await;
    let models = recv_until(&mut ws, |v| (v["message"]["type"] == "list_provider_models_response").then(|| v["message"]["payload"]["models"].clone())).await;
    assert!(models.as_array().unwrap().iter().any(|m| m["id"] == "my-custom-model" && m["label"] == "Mine"));

    // Persisted: a second connection sees it in get_daemon_config.
    let mut ws2 = connect(d.port).await;
    hello(&mut ws2, None).await;
    send(&mut ws2, session(json!({ "type": "get_daemon_config_request", "requestId": "c3" }))).await;
    let cfg = recv_until(&mut ws2, |v| (v["message"]["type"] == "get_daemon_config_response").then(|| v["message"]["payload"]["config"].clone())).await;
    assert_eq!(cfg["providers"]["claude"]["additionalModels"][0]["id"], "my-custom-model");
}

#[tokio::test]
async fn project_add_and_list() {
    let d = start("project", None);
    let proj = d.dir.join("myproject");
    std::fs::create_dir_all(&proj).unwrap();
    let mut ws = connect(d.port).await;
    hello(&mut ws, None).await;
    send(&mut ws, session(json!({ "type": "project.add.request", "requestId": "p1", "cwd": proj.to_string_lossy() }))).await;
    let added = recv_until(&mut ws, |v| (v["message"]["type"] == "project.add.response").then(|| v["message"]["payload"].clone())).await;
    assert!(added["error"].is_null(), "{added}");
    send(&mut ws, session(json!({ "type": "project.list.request", "requestId": "p2" }))).await;
    let list = recv_until(&mut ws, |v| (v["message"]["type"] == "project.list.response").then(|| v["message"]["payload"]["projects"].clone())).await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["projectDisplayName"], "myproject");
}

#[tokio::test]
async fn agent_turn_with_fake_claude() {
    let d = start("agent", None);
    let cwd = d.dir.join("work");
    std::fs::create_dir_all(&cwd).unwrap();
    let mut ws = connect(d.port).await;
    hello(&mut ws, None).await;
    send(
        &mut ws,
        session(json!({
            "type": "create_agent_request",
            "requestId": "a1",
            "config": { "provider": "claude", "cwd": cwd.to_string_lossy(), "modeId": "bypassPermissions" },
            "initialPrompt": "say hi",
        })),
    )
    .await;
    let agent_id = recv_until(&mut ws, |v| {
        let p = &v["message"]["payload"];
        (v["message"]["type"] == "status" && p["status"] == "agent_created").then(|| p["agentId"].as_str().unwrap().to_string())
    })
    .await;

    // The turn completes, and the assistant text lands in the timeline.
    recv_until(&mut ws, |v| (v["message"]["type"] == "agent_stream" && v["message"]["payload"]["event"]["type"] == "turn_completed").then_some(())).await;
    send(&mut ws, session(json!({ "type": "fetch_agent_timeline_request", "requestId": "t1", "agentId": agent_id, "direction": "tail", "limit": 50 }))).await;
    let tl = recv_until(&mut ws, |v| (v["message"]["type"] == "fetch_agent_timeline_response").then(|| v["message"]["payload"].clone())).await;
    assert!(tl["error"].is_null(), "{tl}");
    let items: Vec<String> = tl["entries"].as_array().unwrap().iter().map(|e| e["item"]["type"].as_str().unwrap().to_string()).collect();
    assert!(items.contains(&"user_message".to_string()), "{items:?}");
    assert!(items.contains(&"assistant_message".to_string()), "{items:?}");
    let text = tl["entries"].as_array().unwrap().iter().find(|e| e["item"]["type"] == "assistant_message").unwrap()["item"]["text"].as_str().unwrap().to_string();
    assert!(text.contains("hello from fake claude"));

    // History lists the agent, and it can be archived.
    send(&mut ws, session(json!({ "type": "fetch_agent_history_request", "requestId": "h1" }))).await;
    let hist = recv_until(&mut ws, |v| (v["message"]["type"] == "fetch_agent_history_response").then(|| v["message"]["payload"].clone())).await;
    assert_eq!(hist["entries"].as_array().unwrap().len(), 1);
    send(&mut ws, session(json!({ "type": "archive_agent_request", "requestId": "z1", "agentId": agent_id }))).await;
    recv_until(&mut ws, |v| (v["message"]["type"] == "agent_archived").then_some(())).await;
}

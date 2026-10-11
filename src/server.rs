//! Direct transport: HTTP for the embedded web UI, `/ws` for the protocol.

use crate::session::{self, Ctx};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;

const INDEX_HTML: &str = include_str!("../web/index.html");

#[derive(Clone)]
struct AppState {
    ctx: Arc<Ctx>,
    hostnames: Arc<Vec<String>>,
}

pub async fn serve(ctx: Arc<Ctx>, listen: String, hostnames: Vec<String>) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    let state = AppState { ctx, hostnames: Arc::new(hostnames) };
    let app = Router::new().route("/", get(index)).route("/ws", get(ws_upgrade)).with_state(state);
    crate::log!("listening on http://{listen} (web UI) and ws://{listen}/ws");
    // Nagle off: agent stream updates are many small frames.
    axum::serve(listener, app).tcp_nodelay(true).await
}

async fn index() -> Response {
    ([(header::CACHE_CONTROL, "no-cache")], Html(INDEX_HTML)).into_response()
}

/// DNS-rebinding guard, same policy as the Node daemon: IP literals and
/// localhost are always accepted; other names only when listed in
/// `PASEO_HOSTNAMES` (a leading dot matches the domain and its subdomains).
fn host_allowed(headers: &HeaderMap, extra: &[String]) -> bool {
    let Some(host) = headers.get(header::HOST).and_then(|h| h.to_str().ok()) else { return true };
    let host = host.trim().to_ascii_lowercase();
    let name = if let Some(rest) = host.strip_prefix('[') {
        rest.split(']').next().unwrap_or("").to_string()
    } else {
        host.rsplit_once(':').map(|(h, _)| h.to_string()).unwrap_or(host.clone())
    };
    if name == "localhost" || name.ends_with(".localhost") || name.parse::<std::net::IpAddr>().is_ok() {
        return true;
    }
    extra.iter().any(|allowed| match allowed.strip_prefix('.') {
        Some(domain) => name == domain || name.ends_with(allowed.as_str()),
        None => name == *allowed,
    })
}

async fn ws_upgrade(State(state): State<AppState>, headers: HeaderMap, ws: WebSocketUpgrade) -> Response {
    if !host_allowed(&headers, &state.hostnames) {
        return (StatusCode::FORBIDDEN, "Host not allowed").into_response();
    }
    let ctx = state.ctx.clone();
    ws.max_message_size(16 * 1024 * 1024).on_upgrade(move |socket| handle_socket(ctx, socket))
}

async fn handle_socket(ctx: Arc<Ctx>, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    let (in_tx, in_rx) = tokio::sync::mpsc::channel::<String>(64);
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<Arc<str>>(256);
    let session = tokio::spawn(session::run(ctx, in_rx, out_tx, false));
    loop {
        tokio::select! {
            out = out_rx.recv() => {
                let Some(out) = out else { break };
                if sink.send(Message::Text(out.to_string())).await.is_err() {
                    break;
                }
            }
            frame = stream.next() => {
                let text = match frame {
                    Some(Ok(Message::Text(t))) => t,
                    Some(Ok(Message::Binary(b))) => match String::from_utf8(b) {
                        Ok(t) => t,
                        Err(_) => continue,
                    },
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(_)) => continue,
                };
                if in_tx.send(text).await.is_err() {
                    break;
                }
            }
        }
    }
    session.abort();
}

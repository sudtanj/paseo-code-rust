//! One shared `codex app-server` for every Codex agent in every project.
//!
//! Upstream Paseo spawns an app-server per agent (plus throwaway ones for model
//! listing). The app-server already multiplexes threads, each with its own
//! `cwd`, so here a single process serves all agents: notifications are routed
//! by `threadId`. The process starts on first use and exits after
//! `codex_idle` with no running turns; threads are `thread/resume`d lazily
//! after a restart.

use super::{AgentSpec, EventTx, ProviderEvent};
use crate::config::Config;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};

type Pending = oneshot::Sender<Result<Value, String>>;

struct Proc {
    stdin: mpsc::UnboundedSender<String>,
    kill: Option<oneshot::Sender<()>>,
    generation: u64,
}

#[derive(Default)]
struct State {
    proc: Option<Proc>,
    generation: u64,
    next_id: u64,
    pending: HashMap<u64, Pending>,
    thread_agent: HashMap<String, String>,
    loaded: HashSet<String>,
    /// agentId -> (threadId, turnId once known)
    active: HashMap<String, (String, Option<String>)>,
    streamed_items: HashSet<String>,
    open_approvals: usize,
    last_activity: Option<Instant>,
    models: Option<Value>,
}

struct Inner {
    cfg: Config,
    events: EventTx,
    start_lock: tokio::sync::Mutex<()>,
    state: Mutex<State>,
}

#[derive(Clone)]
pub struct Codex {
    inner: Arc<Inner>,
}

fn policies(mode: &str) -> (&'static str, &'static str, Value) {
    match mode {
        "read-only" => ("on-request", "read-only", json!({ "type": "readOnly" })),
        "full-access" => ("never", "danger-full-access", json!({ "type": "dangerFullAccess" })),
        _ => (
            "on-request",
            "workspace-write",
            json!({ "type": "workspaceWrite", "networkAccess": false, "writableRoots": [], "excludeSlashTmp": false, "excludeTmpdirEnvVar": false }),
        ),
    }
}

fn s(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(|x| x.to_string())
}

impl Codex {
    pub fn new(cfg: Config, events: EventTx) -> Codex {
        Codex { inner: Arc::new(Inner { cfg, events, start_lock: tokio::sync::Mutex::new(()), state: Mutex::new(State::default()) }) }
    }

    pub fn is_running(&self) -> bool {
        self.inner.state.lock().unwrap().proc.is_some()
    }

    async fn ensure_started(&self) -> Result<(), String> {
        let _guard = self.inner.start_lock.lock().await;
        if self.is_running() {
            return Ok(());
        }
        self.spawn()?;
        let init = json!({
            "clientInfo": { "name": "paseo_lite", "title": "Paseo Lite", "version": env!("CARGO_PKG_VERSION") },
            "capabilities": { "experimentalApi": true },
        });
        if let Err(e) = self.request("initialize", init, Duration::from_secs(30)).await {
            self.kill();
            return Err(format!("codex initialize failed: {e}"));
        }
        self.notify("initialized", json!({}));
        Ok(())
    }

    fn spawn(&self) -> Result<(), String> {
        let mut cmd = Command::new(&self.inner.cfg.codex_bin);
        cmd.arg("app-server").stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).kill_on_drop(true);
        let mut child = cmd.spawn().map_err(|e| format!("failed to start {}: {e}", self.inner.cfg.codex_bin))?;
        let mut stdin = child.stdin.take().ok_or("no stdin")?;
        let stdout = child.stdout.take().ok_or("no stdout")?;
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let (kill_tx, mut kill_rx) = oneshot::channel::<()>();
        let generation = {
            let mut st = self.inner.state.lock().unwrap();
            st.generation += 1;
            st.loaded.clear();
            st.last_activity = Some(Instant::now());
            st.proc = Some(Proc { stdin: tx, kill: Some(kill_tx), generation: st.generation });
            st.generation
        };
        crate::log!("codex app-server started (shared, generation {generation})");
        tokio::spawn(async move {
            while let Some(line) = rx.recv().await {
                if stdin.write_all(line.as_bytes()).await.is_err() || stdin.write_all(b"\n").await.is_err() || stdin.flush().await.is_err() {
                    break;
                }
            }
        });
        let this = self.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            loop {
                tokio::select! {
                    line = lines.next_line() => match line {
                        Ok(Some(line)) => this.handle_line(&line),
                        _ => break,
                    },
                    _ = &mut kill_rx => {
                        let _ = child.start_kill();
                        break;
                    }
                }
            }
            let _ = child.wait().await;
            this.on_exit(generation);
        });
        Ok(())
    }

    fn on_exit(&self, generation: u64) {
        let (pending, active) = {
            let mut st = self.inner.state.lock().unwrap();
            if st.proc.as_ref().map(|p| p.generation) == Some(generation) {
                st.proc = None;
            }
            st.loaded.clear();
            st.open_approvals = 0;
            (std::mem::take(&mut st.pending), std::mem::take(&mut st.active))
        };
        crate::log!("codex app-server exited (generation {generation})");
        for (_, tx) in pending {
            let _ = tx.send(Err("codex app-server exited".into()));
        }
        for (agent_id, _) in active {
            let _ = self.inner.events.send(ProviderEvent::TurnFailed { agent_id, error: "codex app-server exited".into() });
        }
    }

    fn send_raw(&self, msg: Value) -> bool {
        let st = self.inner.state.lock().unwrap();
        st.proc.as_ref().map(|p| p.stdin.send(msg.to_string()).is_ok()).unwrap_or(false)
    }

    fn notify(&self, method: &str, params: Value) {
        self.send_raw(json!({ "method": method, "params": params }));
    }

    async fn request(&self, method: &str, params: Value, timeout: Duration) -> Result<Value, String> {
        let (tx, rx) = oneshot::channel();
        let id = {
            let mut st = self.inner.state.lock().unwrap();
            st.next_id += 1;
            let id = st.next_id;
            st.pending.insert(id, tx);
            st.last_activity = Some(Instant::now());
            id
        };
        if !self.send_raw(json!({ "id": id, "method": method, "params": params })) {
            self.inner.state.lock().unwrap().pending.remove(&id);
            return Err("codex app-server not running".into());
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("codex request dropped".into()),
            Err(_) => {
                self.inner.state.lock().unwrap().pending.remove(&id);
                Err(format!("codex {method} timed out"))
            }
        }
    }

    /// Start a turn for an agent; thread creation/resume happens on demand.
    pub async fn run_turn(&self, spec: AgentSpec, text: String) -> Result<(), String> {
        self.ensure_started().await?;
        let mode = spec.mode_id.clone().unwrap_or_else(|| "auto".into());
        let (approval, sandbox, sandbox_policy) = policies(&mode);
        let model = spec.model.clone().filter(|m| !m.is_empty() && m != "default");
        let thread_id = match spec.session_id.clone() {
            Some(tid) => {
                let loaded = self.inner.state.lock().unwrap().loaded.contains(&tid);
                if !loaded {
                    self.request("thread/resume", json!({ "threadId": tid }), Duration::from_secs(120)).await?;
                }
                tid
            }
            None => {
                let mut params = json!({ "cwd": spec.cwd, "approvalPolicy": approval, "sandbox": sandbox });
                if let Some(m) = &model {
                    params["model"] = json!(m);
                }
                if let Some(p) = spec.system_prompt.as_deref().filter(|p| !p.is_empty()) {
                    params["developerInstructions"] = json!(p);
                }
                let res = self.request("thread/start", params, Duration::from_secs(120)).await?;
                let tid = res.get("thread").and_then(|t| s(t, "id")).ok_or("thread/start returned no thread id")?;
                let _ = self.inner.events.send(ProviderEvent::SessionStarted { agent_id: spec.id.clone(), session_id: tid.clone() });
                tid
            }
        };
        {
            let mut st = self.inner.state.lock().unwrap();
            st.loaded.insert(thread_id.clone());
            st.thread_agent.insert(thread_id.clone(), spec.id.clone());
            st.active.insert(spec.id.clone(), (thread_id.clone(), None));
        }
        let mut params = json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": text, "text_elements": [] }],
            "cwd": spec.cwd,
            "approvalPolicy": approval,
            "sandboxPolicy": sandbox_policy,
        });
        if let Some(m) = &model {
            params["model"] = json!(m);
        }
        match self.request("turn/start", params, Duration::from_secs(90)).await {
            Ok(res) => {
                if let Some(turn_id) = res.get("turn").and_then(|t| s(t, "id")) {
                    if let Some(entry) = self.inner.state.lock().unwrap().active.get_mut(&spec.id) {
                        entry.1.get_or_insert(turn_id);
                    }
                }
                Ok(())
            }
            Err(e) => {
                self.inner.state.lock().unwrap().active.remove(&spec.id);
                Err(e)
            }
        }
    }

    pub async fn interrupt(&self, agent_id: &str) {
        let target = self.inner.state.lock().unwrap().active.get(agent_id).cloned();
        if let Some((thread_id, Some(turn_id))) = target {
            let _ = self.request("turn/interrupt", json!({ "threadId": thread_id, "turnId": turn_id }), Duration::from_secs(5)).await;
        }
    }

    pub fn forget(&self, agent_id: &str) {
        let mut st = self.inner.state.lock().unwrap();
        st.thread_agent.retain(|_, a| a != agent_id);
        st.active.remove(agent_id);
    }

    pub async fn list_models(&self) -> Result<Value, String> {
        if let Some(cached) = self.inner.state.lock().unwrap().models.clone() {
            return Ok(cached);
        }
        self.ensure_started().await?;
        let res = self.request("model/list", json!({}), Duration::from_secs(30)).await?;
        let list = res.get("data").or_else(|| res.get("models")).and_then(|d| d.as_array()).cloned().unwrap_or_default();
        let models: Vec<Value> = list
            .iter()
            .filter_map(|m| {
                let id = s(m, "model").or_else(|| s(m, "id"))?;
                Some(json!({
                    "provider": "codex",
                    "id": id,
                    "label": s(m, "displayName").unwrap_or_else(|| id.clone()),
                    "description": s(m, "description"),
                    "isDefault": m.get("isDefault").and_then(|v| v.as_bool()).unwrap_or(false),
                }))
            })
            .collect();
        let models = Value::Array(models);
        self.inner.state.lock().unwrap().models = Some(models.clone());
        Ok(models)
    }

    /// The model list from an earlier `list_models`, without starting anything.
    pub fn cached_models(&self) -> Option<Value> {
        self.inner.state.lock().unwrap().models.clone()
    }

    /// Drop the cached model list so the next `list_models` asks the app-server again.
    pub fn clear_models(&self) {
        self.inner.state.lock().unwrap().models = None;
    }

    fn kill(&self) {
        let mut st = self.inner.state.lock().unwrap();
        if let Some(mut p) = st.proc.take() {
            if let Some(k) = p.kill.take() {
                let _ = k.send(());
            }
        }
    }

    /// Stop the shared process when nothing has used it for `codex_idle`.
    pub fn reap(&self) {
        let idle = {
            let st = self.inner.state.lock().unwrap();
            st.proc.is_some()
                && st.active.is_empty()
                && st.pending.is_empty()
                && st.open_approvals == 0
                && st.last_activity.map(|t| t.elapsed() >= self.inner.cfg.codex_idle).unwrap_or(true)
        };
        if idle {
            crate::log!("codex app-server idle; releasing process");
            self.kill();
        }
    }

    pub fn shutdown(&self) {
        self.kill();
    }

    fn agent_for(&self, params: &Value) -> Option<String> {
        let tid = s(params, "threadId").or_else(|| s(params, "conversationId"))?;
        self.inner.state.lock().unwrap().thread_agent.get(&tid).cloned()
    }

    fn emit(&self, ev: ProviderEvent) {
        let _ = self.inner.events.send(ev);
    }

    fn handle_line(&self, line: &str) {
        let Ok(msg) = serde_json::from_str::<Value>(line) else { return };
        self.inner.state.lock().unwrap().last_activity = Some(Instant::now());
        let method = s(&msg, "method");
        match (msg.get("id"), method) {
            (Some(id), None) => {
                let Some(id) = id.as_u64() else { return };
                let tx = self.inner.state.lock().unwrap().pending.remove(&id);
                if let Some(tx) = tx {
                    let result = match msg.get("error") {
                        Some(err) => Err(err.get("message").and_then(|m| m.as_str()).map(|m| m.to_string()).unwrap_or_else(|| err.to_string())),
                        None => Ok(msg.get("result").cloned().unwrap_or(Value::Null)),
                    };
                    let _ = tx.send(result);
                }
            }
            (Some(id), Some(method)) => self.handle_server_request(id.clone(), &method, msg.get("params").cloned().unwrap_or(Value::Null)),
            (None, Some(method)) => self.handle_notification(&method, msg.get("params").unwrap_or(&Value::Null)),
            _ => {}
        }
    }
}

#[path = "codex_events.rs"]
mod events;

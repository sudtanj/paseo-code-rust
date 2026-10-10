//! The manager actor: sole owner of agent state. Sessions and providers talk to
//! it over channels, so there are no locks around agent data and it never
//! awaits while handling a message.

use crate::config::Config;
use crate::providers::claude::{ClaudePool, SendOutcome};
use crate::providers::codex::Codex;
use crate::providers::{AgentSpec, ProviderEvent};
use crate::store::{AgentRecord, ProjectRecord, Row, Store, WorkspaceRecord};
use crate::util::{new_id, now_iso};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot};

pub type Out = mpsc::UnboundedSender<Arc<str>>;

pub const EPOCH: &str = "e1";

pub enum Cmd {
    /// A session-level message (already unwrapped from the `session` envelope).
    Rpc { msg: Value, out: Out },
    /// Register a directory produced by background work (GitHub clone), then
    /// send `response` with `payload.project` filled in.
    RegisterProject { path: String, out: Out, response: Value },
}

pub struct Permission {
    pub request: Value,
    pub reply: Option<oneshot::Sender<Value>>,
}

pub struct Agent {
    pub rec: AgentRecord,
    pub status: &'static str,
    pub active_turn: Option<(String, String)>,
    pub queue: VecDeque<String>,
    pub permissions: Vec<Permission>,
    pub timeline: Option<Vec<Row>>,
    pub dirty_rows: BTreeSet<u64>,
    pub last_touch: Instant,
}

pub struct Manager {
    pub cfg: Config,
    pub server_id: String,
    pub store: Store,
    pub agents: HashMap<String, Agent>,
    pub claude: ClaudePool,
    pub codex: Codex,
    pub bcast: broadcast::Sender<Arc<str>>,
    pub events_tx: mpsc::UnboundedSender<ProviderEvent>,
    pub agents_dirty: bool,
    /// Relay pairing link (None when the relay is disabled).
    pub pairing_url: Option<String>,
    /// Directories added as projects, in insertion order.
    pub projects: Vec<ProjectRecord>,
    /// Workspaces (including archived ones, kept for agent history).
    pub workspaces: Vec<WorkspaceRecord>,
    /// Lets background tasks hand results back to the manager.
    pub cmds_tx: mpsc::UnboundedSender<Cmd>,
    /// Live git diff subscriptions for the Changes panel.
    pub checkout: crate::checkout::Watcher,
}

pub fn wrap(msg: Value) -> Arc<str> {
    Arc::from(json!({ "type": "session", "message": msg }).to_string())
}

pub fn reply(out: &Out, msg: Value) {
    let _ = out.send(wrap(msg));
}

impl Manager {
    pub fn new(
        cfg: Config,
        server_id: String,
        pairing_url: Option<String>,
        bcast: broadcast::Sender<Arc<str>>,
        cmds_tx: mpsc::UnboundedSender<Cmd>,
    ) -> (Manager, mpsc::UnboundedReceiver<ProviderEvent>) {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let store = Store::new(cfg.home.clone());
        let mut agents = HashMap::new();
        for rec in store.load_agents() {
            agents.insert(rec.id.clone(), Agent::new(rec));
        }
        let projects = store.load_projects();
        let workspaces = store.load_workspaces();
        crate::log!("loaded {} agents, {} projects, {} workspaces", agents.len(), projects.len(), workspaces.len());
        let claude = ClaudePool::new(cfg.clone(), events_tx.clone());
        let codex = Codex::new(cfg.clone(), events_tx.clone());
        let mut manager = Manager { cfg, server_id, store, agents, claude, codex, bcast, events_tx, agents_dirty: false, pairing_url, projects, workspaces, cmds_tx, checkout: crate::checkout::Watcher::start() };
        manager.migrate_agent_workspaces();
        (manager, events_rx)
    }

    /// Add a directory to the project registry (idempotent) and return its descriptor.
    pub fn register_project(&mut self, path: &str) -> Value {
        if !self.projects.iter().any(|p| p.root_path == path) {
            self.projects.push(ProjectRecord { root_path: path.to_string(), created_at: now_iso() });
            self.store.save_projects(&self.projects);
            crate::log!("project added: {path}");
        }
        let project = crate::snapshot::project_descriptor(path);
        self.broadcast(json!({ "type": "project.update", "payload": { "kind": "upsert", "project": project } }));
        project
    }

    pub async fn run(mut self, mut cmds: mpsc::UnboundedReceiver<Cmd>, mut events: mpsc::UnboundedReceiver<ProviderEvent>, mut shutdown: oneshot::Receiver<()>) {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                Some(cmd) = cmds.recv() => match cmd {
                    Cmd::Rpc { msg, out } => self.handle_rpc(msg, &out),
                    Cmd::RegisterProject { path, out, mut response } => {
                        response["payload"]["project"] = self.register_project(&path);
                        reply(&out, response);
                    }
                },
                Some(ev) = events.recv() => self.handle_event(ev),
                _ = tick.tick() => self.housekeeping(),
                _ = &mut shutdown => break,
            }
        }
        self.flush_all();
        self.claude.shutdown();
        self.codex.shutdown();
    }

    fn housekeeping(&mut self) {
        self.claude.reap();
        self.codex.reap();
        self.flush_all();
        let unload = self.cfg.timeline_unload;
        for agent in self.agents.values_mut() {
            if agent.timeline.is_some() && agent.status != "running" && agent.dirty_rows.is_empty() && agent.last_touch.elapsed() >= unload {
                agent.timeline = None;
            }
        }
    }

    pub fn flush_all(&mut self) {
        for agent in self.agents.values_mut() {
            agent.flush(&self.store);
        }
        if self.agents_dirty {
            self.agents_dirty = false;
            self.store.save_agents(self.agents.values().map(|a| &a.rec));
        }
    }

    pub fn broadcast(&self, msg: Value) {
        let _ = self.bcast.send(wrap(msg));
    }

    pub fn touch(&mut self, agent_id: &str) {
        if let Some(a) = self.agents.get_mut(agent_id) {
            a.rec.updated_at = now_iso();
            a.last_touch = Instant::now();
        }
        self.agents_dirty = true;
    }

    pub fn publish_agent(&self, agent_id: &str) {
        if let Some(a) = self.agents.get(agent_id) {
            self.broadcast(json!({
                "type": "agent_update",
                "payload": { "kind": "upsert", "agent": a.snapshot(), "project": a.project() },
            }));
            // Workspace status/activity is derived from its agents.
            if let Some(ws) = &a.rec.workspace_id {
                self.publish_workspace(ws);
            }
        }
    }

    fn stream(&self, agent_id: &str, provider: &str, event: Value) {
        let _ = provider;
        self.broadcast(json!({
            "type": "agent_stream",
            "payload": { "agentId": agent_id, "event": event, "timestamp": now_iso() },
        }));
    }

    /// Append a timeline item, merging chunks / tool updates into existing rows.
    pub fn append_item(&mut self, agent_id: &str, item: Value) {
        let item = crate::snapshot::clean(item);
        let store = &self.store;
        let Some(agent) = self.agents.get_mut(agent_id) else { return };
        agent.ensure_timeline(store);
        let seq = agent.rec.next_seq;
        agent.rec.next_seq += 1;
        let ts = now_iso();
        let turn_id = agent.active_turn.as_ref().map(|t| t.0.clone());
        let provider = agent.rec.provider.clone();
        let kind = item.get("type").and_then(|t| t.as_str()).unwrap_or("").to_string();
        let rows = agent.timeline.as_mut().expect("loaded");

        let mut merged_into: Option<usize> = None;
        if kind == "assistant_message" || kind == "reasoning" {
            if let Some(last) = rows.last_mut() {
                if last.item.get("type").and_then(|t| t.as_str()) == Some(kind.as_str()) && last.turn_id == turn_id {
                    let add = item.get("text").and_then(|t| t.as_str()).unwrap_or("");
                    if let Some(Value::String(text)) = last.item.get_mut("text") {
                        text.push_str(add);
                    }
                    let tag = if kind == "assistant_message" { "assistant_merge" } else { "reasoning_merge" };
                    if !last.collapsed.iter().any(|c| c == tag) {
                        last.collapsed.push(tag.into());
                    }
                    merged_into = Some(rows.len() - 1);
                }
            }
        } else if kind == "tool_call" {
            let call_id = item.get("callId").and_then(|c| c.as_str()).unwrap_or("");
            let n = rows.len();
            if let Some(idx) = rows.iter().rev().take(256).position(|r| r.item.get("callId").and_then(|c| c.as_str()) == Some(call_id)).map(|p| n - 1 - p) {
                rows[idx].item = item.clone();
                if !rows[idx].collapsed.iter().any(|c| c == "tool_lifecycle") {
                    rows[idx].collapsed.push("tool_lifecycle".into());
                }
                merged_into = Some(idx);
            }
        }

        match merged_into {
            Some(idx) => {
                let row = &mut rows[idx];
                row.seq_end = seq;
                match row.ranges.last_mut() {
                    Some(r) if r.1 + 1 == seq => r.1 = seq,
                    _ => row.ranges.push((seq, seq)),
                }
                agent.dirty_rows.insert(row.seq_start);
            }
            None => {
                rows.push(Row { seq_start: seq, seq_end: seq, ranges: vec![(seq, seq)], collapsed: vec![], item: item.clone(), turn_id: turn_id.clone(), timestamp: ts.clone() });
                agent.dirty_rows.insert(seq);
            }
        }
        agent.last_touch = Instant::now();
        self.agents_dirty = true;
        let mut event = json!({ "type": "timeline", "provider": provider, "item": item });
        if let Some(t) = turn_id {
            event["turnId"] = json!(t);
        }
        self.broadcast(json!({
            "type": "agent_stream",
            "payload": { "agentId": agent_id, "event": event, "timestamp": ts, "seq": seq, "epoch": EPOCH },
        }));
    }

    fn spec(&self, agent_id: &str) -> Option<AgentSpec> {
        let a = self.agents.get(agent_id)?;
        Some(AgentSpec {
            id: a.rec.id.clone(),
            cwd: a.rec.cwd.clone(),
            model: a.rec.model.clone(),
            mode_id: a.rec.mode_id.clone(),
            session_id: a.rec.session_id.clone(),
            system_prompt: match (self.cfg.default_system_prompt.as_deref(), a.rec.system_prompt.as_deref().filter(|p| !p.trim().is_empty())) {
                (Some(d), Some(own)) => Some(format!("{d}\n\n{own}")),
                (Some(d), None) => Some(d.to_string()),
                (None, own) => own.map(String::from),
            },
        })
    }

    /// Accept a prompt: start now when idle, else queue behind the running turn.
    pub fn submit_prompt(&mut self, agent_id: &str, text: String, message_id: Option<String>) -> Result<(), String> {
        let agent = self.agents.get_mut(agent_id).ok_or("agent not found")?;
        if agent.rec.archived_at.is_some() {
            agent.rec.archived_at = None;
        }
        if agent.status == "running" {
            agent.queue.push_back(text);
            return Ok(());
        }
        self.start_turn(agent_id, text, message_id);
        Ok(())
    }

    fn start_turn(&mut self, agent_id: &str, text: String, message_id: Option<String>) {
        let turn_id = new_id();
        let provider = {
            let Some(agent) = self.agents.get_mut(agent_id) else { return };
            agent.status = "running";
            agent.active_turn = Some((turn_id.clone(), now_iso()));
            agent.rec.last_user_message_at = Some(now_iso());
            agent.rec.last_error = None;
            agent.rec.attention_reason = None;
            if agent.rec.title.is_none() {
                let first_line = text.lines().next().unwrap_or("").trim();
                agent.rec.title = Some(crate::util::truncate(first_line, 80));
            }
            agent.rec.provider.clone()
        };
        let mut user = json!({ "type": "user_message", "text": text, "messageId": message_id.clone().unwrap_or_else(new_id) });
        if let Some(id) = message_id {
            user["clientMessageId"] = json!(id);
        }
        self.append_item(agent_id, user);
        self.stream(agent_id, &provider, json!({ "type": "turn_started", "provider": provider, "turnId": turn_id }));
        self.touch(agent_id);
        self.publish_agent(agent_id);
        self.dispatch(agent_id, text);
    }

    fn dispatch(&mut self, agent_id: &str, text: String) {
        let Some(spec) = self.spec(agent_id) else { return };
        let provider = self.agents[agent_id].rec.provider.clone();
        if provider == "codex" {
            let codex = self.codex.clone();
            let events = self.events_tx.clone();
            let id = agent_id.to_string();
            tokio::spawn(async move {
                if let Err(error) = codex.run_turn(spec, text).await {
                    let _ = events.send(ProviderEvent::TurnFailed { agent_id: id, error });
                }
            });
            return;
        }
        match self.claude.send(spec, text) {
            SendOutcome::Started => {}
            SendOutcome::Queued => {
                let note = json!({ "type": "notification", "level": "info", "message": format!("Waiting for a free Claude slot ({} running)", self.claude.live_count()) });
                self.append_item(agent_id, note);
            }
            SendOutcome::Failed(error) => self.finish_turn(agent_id, Err(error)),
        }
    }

    /// End the active turn. `Ok(None)` completed, `Ok(Some(reason))` canceled, `Err` failed.
    pub fn finish_turn(&mut self, agent_id: &str, outcome: Result<Option<String>, String>) {
        let (provider, turn_id) = {
            let Some(agent) = self.agents.get_mut(agent_id) else { return };
            if agent.status != "running" {
                return;
            }
            let turn_id = agent.active_turn.take().map(|t| t.0);
            // Dropping the reply senders makes providers deny outstanding requests.
            let pending: Vec<String> = agent.permissions.drain(..).filter_map(|p| p.request.get("id").and_then(|i| i.as_str()).map(String::from)).collect();
            agent.status = if outcome.is_err() { "error" } else { "idle" };
            let reason = match &outcome {
                Ok(None) => Some("finished"),
                Err(_) => Some("error"),
                Ok(Some(_)) => None,
            };
            agent.rec.attention_reason = reason.map(String::from);
            agent.rec.attention_at = reason.map(|_| now_iso());
            if let Err(e) = &outcome {
                agent.rec.last_error = Some(e.clone());
            }
            let provider = agent.rec.provider.clone();
            for id in pending {
                self.broadcast(json!({ "type": "agent_permission_resolved", "payload": { "agentId": agent_id, "requestId": id, "resolution": { "behavior": "deny", "message": "turn ended" } } }));
            }
            (provider, turn_id)
        };
        let event = match &outcome {
            Ok(None) => {
                let mut ev = json!({ "type": "turn_completed", "provider": provider, "turnId": turn_id });
                if let Some(u) = self.agents[agent_id].rec.last_usage.clone() {
                    ev["usage"] = crate::snapshot::strip_nulls(u);
                }
                ev
            }
            Ok(Some(reason)) => json!({ "type": "turn_canceled", "provider": provider, "turnId": turn_id, "reason": reason }),
            Err(error) => {
                self.append_item(agent_id, json!({ "type": "error", "message": error }));
                json!({ "type": "turn_failed", "provider": provider, "turnId": turn_id, "error": error })
            }
        };
        self.stream(agent_id, &provider, event);
        if let Some(a) = self.agents.get_mut(agent_id) {
            a.flush(&self.store);
        }
        self.touch(agent_id);
        self.publish_agent(agent_id);
        self.checkout.poke();

        if provider == "claude" {
            for (spec, text) in self.claude.turn_finished(agent_id) {
                let id = spec.id.clone();
                if let SendOutcome::Failed(e) = self.claude.send(spec, text) {
                    self.finish_turn(&id, Err(e));
                }
            }
        }
        let next = self.agents.get_mut(agent_id).and_then(|a| a.queue.pop_front());
        if let Some(text) = next {
            self.start_turn(agent_id, text, None);
        }
    }

    pub fn cancel(&mut self, agent_id: &str) {
        let Some(agent) = self.agents.get_mut(agent_id) else { return };
        agent.queue.clear();
        if agent.status != "running" {
            return;
        }
        if agent.rec.provider == "codex" {
            let codex = self.codex.clone();
            let id = agent_id.to_string();
            tokio::spawn(async move { codex.interrupt(&id).await });
        } else if self.claude.interrupt(agent_id) {
            self.finish_turn(agent_id, Ok(Some("canceled before start".into())));
        }
    }

    fn handle_event(&mut self, ev: ProviderEvent) {
        match ev {
            ProviderEvent::SessionStarted { agent_id, session_id } => {
                if let Some(a) = self.agents.get_mut(&agent_id) {
                    a.rec.session_id = Some(session_id.clone());
                    let provider = a.rec.provider.clone();
                    self.agents_dirty = true;
                    self.stream(&agent_id, &provider, json!({ "type": "thread_started", "provider": provider, "sessionId": session_id }));
                }
            }
            ProviderEvent::TurnStarted { .. } => {}
            ProviderEvent::Timeline { agent_id, item } => {
                if self.agents.get(&agent_id).map(|a| a.status == "running").unwrap_or(false) {
                    self.append_item(&agent_id, item);
                }
            }
            ProviderEvent::Permission { agent_id, request, reply } => {
                let request = crate::snapshot::clean(request);
                let Some(a) = self.agents.get_mut(&agent_id) else { return };
                a.permissions.push(Permission { request: request.clone(), reply: Some(reply) });
                a.rec.attention_reason = Some("permission".into());
                a.rec.attention_at = Some(now_iso());
                let provider = a.rec.provider.clone();
                self.broadcast(json!({ "type": "agent_permission_request", "payload": { "agentId": agent_id, "request": request } }));
                self.stream(&agent_id, &provider, json!({ "type": "permission_requested", "provider": provider, "request": request }));
                self.publish_agent(&agent_id);
            }
            ProviderEvent::Usage { agent_id, usage } => {
                if let Some(a) = self.agents.get_mut(&agent_id) {
                    a.rec.last_usage = Some(usage);
                }
            }
            ProviderEvent::TurnCompleted { agent_id } => self.finish_turn(&agent_id, Ok(None)),
            ProviderEvent::TurnCanceled { agent_id, reason } => self.finish_turn(&agent_id, Ok(Some(reason))),
            ProviderEvent::TurnFailed { agent_id, error } => self.finish_turn(&agent_id, Err(error)),
            ProviderEvent::ProcessExited { agent_id, generation } => {
                let (current, ready) = self.claude.process_exited(&agent_id, generation);
                if current && self.agents.get(&agent_id).map(|a| a.status == "running").unwrap_or(false) && !self.claude.is_waiting(&agent_id) {
                    self.finish_turn(&agent_id, Err("claude exited unexpectedly".into()));
                }
                for (spec, text) in ready {
                    let id = spec.id.clone();
                    if let SendOutcome::Failed(e) = self.claude.send(spec, text) {
                        self.finish_turn(&id, Err(e));
                    }
                }
            }
        }
    }

    pub fn respond_permission(&mut self, agent_id: &str, request_id: &str, response: Value) -> bool {
        let Some(a) = self.agents.get_mut(agent_id) else { return false };
        let Some(pos) = a.permissions.iter().position(|p| p.request.get("id").and_then(|i| i.as_str()) == Some(request_id)) else { return false };
        let mut perm = a.permissions.remove(pos);
        if let Some(tx) = perm.reply.take() {
            let _ = tx.send(response.clone());
        }
        if a.permissions.is_empty() && a.rec.attention_reason.as_deref() == Some("permission") {
            a.rec.attention_reason = None;
        }
        let provider = a.rec.provider.clone();
        self.broadcast(json!({ "type": "agent_permission_resolved", "payload": { "agentId": agent_id, "requestId": request_id, "resolution": response } }));
        self.stream(agent_id, &provider, json!({ "type": "permission_resolved", "provider": provider, "requestId": request_id, "resolution": response }));
        self.publish_agent(agent_id);
        true
    }

    /// Release every runtime resource held for an agent (process slot, thread route).
    pub fn release_runtime(&mut self, agent_id: &str) {
        if let Some(a) = self.agents.get_mut(agent_id) {
            a.queue.clear();
            a.permissions.clear();
            a.status = "idle";
            a.active_turn = None;
        }
        self.claude.stop(agent_id);
        self.codex.forget(agent_id);
    }
}

impl Agent {
    pub fn new(rec: AgentRecord) -> Agent {
        Agent { rec, status: "idle", active_turn: None, queue: VecDeque::new(), permissions: Vec::new(), timeline: None, dirty_rows: BTreeSet::new(), last_touch: Instant::now() }
    }

    pub fn ensure_timeline(&mut self, store: &Store) {
        if self.timeline.is_none() {
            let rows = store.load_timeline(&self.rec.id);
            if let Some(last) = rows.last() {
                self.rec.next_seq = self.rec.next_seq.max(last.seq_end + 1);
            }
            self.timeline = Some(rows);
        }
        self.last_touch = Instant::now();
    }

    pub fn flush(&mut self, store: &Store) {
        if self.dirty_rows.is_empty() {
            return;
        }
        let Some(rows) = &self.timeline else { return };
        let dirty = std::mem::take(&mut self.dirty_rows);
        let out: Vec<&Row> = rows.iter().filter(|r| dirty.contains(&r.seq_start)).collect();
        store.append_rows(&self.rec.id, &out);
    }
}

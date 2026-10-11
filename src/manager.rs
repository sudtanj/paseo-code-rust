//! The manager actor: sole owner of agent state. Sessions and providers talk to
//! it over channels, so there are no locks around agent data and it never
//! awaits while handling a message.

use crate::config::Config;
use crate::providers::claude::{ClaudePool, SendOutcome};
use crate::providers::codex::Codex;
use crate::providers::{binary_available, AgentSpec, ProviderEvent};
use crate::store::{AgentRecord, ProjectRecord, Row, Store, WorkspaceRecord};
use crate::util::{new_id, now_iso};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot};

pub type Out = mpsc::UnboundedSender<Arc<str>>;

pub const EPOCH: &str = "e1";

/// Minimum gap between attempts to (re)start a keep-warm process, so a
/// broken binary or a vanished session can't spin in a restart loop.
const WARM_RETRY: Duration = Duration::from_secs(60);

/// Prompt text that compacts the conversation (native in Claude Code; mapped
/// to `thread/compact/start` for Codex).
const COMPACT_COMMAND: &str = "/compact";

/// The CLI that serves a provider.
fn cli_of(provider: &str) -> &'static str {
    if provider == "codex" { "codex" } else { "claude" }
}

fn cli_label(cli: &str) -> &'static str {
    if cli == "codex" { "Codex" } else { "Claude Code" }
}

pub enum Cmd {
    /// A session-level message (already unwrapped from the `session` envelope).
    Rpc { msg: Value, out: Out },
    /// Register a directory produced by background work (GitHub clone), then
    /// send `response` with `payload.project` filled in.
    RegisterProject { path: String, out: Out, response: Value },
    /// Push tokens Expo reported as dead.
    RevokePushTokens(Vec<String>),
}

/// What a connected client last reported via `client_heartbeat`.
pub struct Presence {
    pub out: Out,
    pub focused_agent: Option<String>,
    pub app_visible: bool,
    pub active_at: Instant,
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
    /// The active turn is a daemon-started compaction, not a user prompt.
    pub compacting: bool,
    /// The user prompt of the running turn (resent when a retry has nothing to continue).
    pub turn_text: String,
    /// Automatic retries used by the running turn.
    pub attempts: u32,
    /// The turn produced output (assistant text, reasoning or a tool call).
    pub produced: bool,
    /// Last sign of life from the provider, for the stall watchdog.
    pub last_event: Instant,
    /// A retry is scheduled: when to re-dispatch and with which prompt.
    pub retry_at: Option<Instant>,
    pub retry_text: String,
}

pub struct Manager {
    pub cfg: Config,
    pub server_id: String,
    pub store: Store,
    pub agents: HashMap<String, Agent>,
    pub claude: ClaudePool,
    pub codex: Codex,
    /// The one CLI allowed to have a process ("claude" / "codex"). Switching
    /// stops the other CLI first, so the two never run side by side.
    pub active_cli: Option<&'static str>,
    /// Agents whose turn currently holds a slot on the active CLI.
    pub running: HashSet<String>,
    /// Turns (agentId, prompt) waiting for the CLI, across all projects and
    /// both providers, strictly first come first served.
    pub cli_queue: VecDeque<(String, String)>,
    claude_warm_at: Option<Instant>,
    codex_warm_at: Option<Instant>,
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
    /// When the daemon started (`daemon.get_status`).
    pub started_at: String,
    /// Pending `wait_for_finish_request`s.
    pub waiters: Vec<Waiter>,
    /// Expo push tokens registered by phones.
    pub push_tokens: crate::push::TokenStore,
    /// Presence per connected client, for in-app vs push notifications.
    pub clients: Vec<Presence>,
    /// App-editable settings (custom models, appended system prompt).
    pub daemon_config: crate::daemon_config::DaemonConfig,
}

/// A client blocked in `wait_for_finish_request` until the agent stops
/// running, needs permission, or the deadline passes.
pub struct Waiter {
    pub agent_id: String,
    pub request_id: String,
    pub out: Out,
    pub deadline: Option<Instant>,
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
        let push_tokens = crate::push::TokenStore::load(&cfg.home);
        let daemon_config = crate::daemon_config::DaemonConfig::load(&cfg.home);
        for p in &projects {
            crate::snapshot::set_project_name(&p.root_path, p.custom_name.as_deref());
        }
        crate::log!("loaded {} agents, {} projects, {} workspaces", agents.len(), projects.len(), workspaces.len());
        let claude = ClaudePool::new(cfg.clone(), events_tx.clone());
        let codex = Codex::new(cfg.clone(), events_tx.clone());
        let mut manager = Manager { cfg, server_id, store, agents, claude, codex, active_cli: None, running: HashSet::new(), cli_queue: VecDeque::new(), claude_warm_at: None, codex_warm_at: None, bcast, events_tx, agents_dirty: false, pairing_url, projects, workspaces, cmds_tx, checkout: crate::checkout::Watcher::start(), started_at: now_iso(), waiters: Vec::new(), push_tokens, clients: Vec::new(), daemon_config };
        manager.migrate_agent_workspaces();
        (manager, events_rx)
    }

    /// Add a directory to the project registry (idempotent) and return its
    /// descriptor. `owned`: paseo-lite created the directory itself.
    pub fn register_project(&mut self, path: &str, owned: bool) -> Value {
        match self.projects.iter_mut().find(|p| p.root_path == path) {
            Some(p) => {
                if owned && !p.owned {
                    p.owned = true;
                    self.store.save_projects(&self.projects);
                }
            }
            None => {
                self.projects.push(ProjectRecord { root_path: path.to_string(), created_at: now_iso(), custom_name: None, owned });
                self.store.save_projects(&self.projects);
                crate::log!("project added: {path}");
            }
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
                        response["payload"]["project"] = self.register_project(&path, true);
                        reply(&out, response);
                    }
                    Cmd::RevokePushTokens(tokens) => {
                        for t in tokens {
                            self.push_tokens.revoke(&t);
                        }
                    }
                },
                Some(ev) = events.recv() => self.handle_event(ev),
                _ = tick.tick() => self.housekeeping(),
                _ = &mut shutdown => break,
            }
            self.resolve_waiters();
        }
        self.flush_all();
        self.claude.shutdown();
        self.codex.shutdown();
    }

    fn housekeeping(&mut self) {
        self.claude.reap();
        self.codex.reap();
        self.keep_warm();
        self.run_retries_and_watchdog();
        self.flush_all();
        let unload = self.cfg.timeline_unload;
        for agent in self.agents.values_mut() {
            if agent.timeline.is_some() && agent.status != "running" && agent.dirty_rows.is_empty() && agent.last_touch.elapsed() >= unload {
                agent.timeline = None;
            }
        }
    }

    /// Keep the active CLI ready so prompts skip the cold start. Only the
    /// active CLI is ever warmed; the other one stays fully stopped.
    /// Runs every tick; the first tick fires right after startup.
    fn keep_warm(&mut self) {
        if self.active_cli.is_none() {
            // Startup: the CLI of the most recently used agent becomes active.
            self.active_cli = self
                .agents
                .values()
                .filter(|a| a.rec.archived_at.is_none() && a.rec.last_user_message_at.is_some())
                .max_by(|a, b| a.rec.last_user_message_at.cmp(&b.rec.last_user_message_at))
                .map(|a| cli_of(&a.rec.provider));
            if let Some(cli) = self.active_cli {
                crate::log!("active CLI: {cli} (last used)");
                if cli == "claude" {
                    self.codex.set_allowed(false);
                    self.codex.shutdown();
                }
            }
        }
        if self.active_cli.is_none() && self.codex.is_running() {
            // Started for model listing before any agent ran: it is the warm
            // CLI now, so a later Claude turn switches (and stops it) properly.
            self.active_cli = Some("codex");
        }
        let due = |t: Option<Instant>| t.map(|t| t.elapsed() >= WARM_RETRY).unwrap_or(true);
        match self.active_cli {
            Some("codex") => {
                if self.cfg.codex_keep_warm && !self.codex.is_running() && due(self.codex_warm_at) && binary_available(&self.cfg.codex_bin) {
                    self.codex_warm_at = Some(Instant::now());
                    let codex = self.codex.clone();
                    tokio::spawn(async move {
                        if let Err(e) = codex.warm_up().await {
                            crate::warn!("codex warm-up failed: {e}");
                        }
                    });
                }
            }
            Some("claude") => {
                if self.cfg.claude_keep_warm && self.claude.live_count() == 0 && !self.codex.is_running() && self.running.is_empty() && self.cli_queue.is_empty() && due(self.claude_warm_at) && binary_available(&self.cfg.claude_bin) {
                    // The process is tied to one session and cwd, so warm the Claude
                    // agent most likely to get the next prompt: the last one used.
                    let candidate = self
                        .agents
                        .values()
                        .filter(|a| a.rec.provider == "claude" && a.status != "running" && a.rec.archived_at.is_none() && a.rec.session_id.is_some())
                        .filter(|a| std::path::Path::new(&a.rec.cwd).is_dir())
                        .max_by(|a, b| a.rec.last_user_message_at.as_ref().unwrap_or(&a.rec.updated_at).cmp(b.rec.last_user_message_at.as_ref().unwrap_or(&b.rec.updated_at)))
                        .map(|a| a.rec.id.clone());
                    if let Some(spec) = candidate.and_then(|id| self.spec(&id)) {
                        self.claude_warm_at = Some(Instant::now());
                        if let Err(e) = self.claude.prewarm(&spec) {
                            crate::warn!("claude prewarm failed: {e}");
                        }
                    }
                }
            }
            _ => {}
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
            // Built-in default, then the app's appendSystemPrompt, then the agent's own.
            system_prompt: {
                let parts: Vec<String> = [self.cfg.default_system_prompt.clone(), self.daemon_config.append_system_prompt(), a.rec.system_prompt.clone()]
                    .into_iter()
                    .flatten()
                    .filter(|p| !p.trim().is_empty())
                    .collect();
                (!parts.is_empty()).then(|| parts.join("\n\n"))
            },
            thinking: a.rec.thinking_option_id.clone(),
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
            agent.turn_text = text.clone();
            agent.attempts = 0;
            agent.produced = false;
            agent.retry_at = None;
            agent.last_event = Instant::now();
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

    /// Queue a turn for the shared CLI and start whatever can run now.
    fn dispatch(&mut self, agent_id: &str, text: String) {
        self.cli_queue.push_back((agent_id.to_string(), text));
        self.pump();
        if let Some(pos) = self.cli_queue.iter().position(|(id, _)| id == agent_id) {
            let note = json!({ "type": "notification", "level": "info", "message": format!("Queued: waiting for the agent CLI ({} ahead)", self.running.len() + pos) });
            self.append_item(agent_id, note);
        }
    }

    /// Start queued turns in strict FIFO order. Only one CLI is active at a
    /// time: a turn for the other CLI waits until the active CLI's running
    /// turns end, then the active CLI is shut down and the other one cold
    /// starts and stays warm until the next switch.
    fn pump(&mut self) {
        while let Some((front, _)) = self.cli_queue.front() {
            let id = front.clone();
            let Some(provider) = self.agents.get(&id).filter(|a| a.status == "running").map(|a| a.rec.provider.clone()) else {
                self.cli_queue.pop_front();
                continue;
            };
            let cli = cli_of(&provider);
            if self.active_cli != Some(cli) {
                if !self.running.is_empty() {
                    break;
                }
                self.switch_cli(cli, &id);
            }
            let cap = if cli == "codex" { self.cfg.codex_max_active } else { self.cfg.claude_max_live };
            if self.running.len() >= cap {
                break;
            }
            let Some((id, text)) = self.cli_queue.pop_front() else { break };
            let Some(spec) = self.spec(&id) else { continue };
            self.running.insert(id.clone());
            if cli == "codex" {
                self.run_codex(spec, text);
            } else if let SendOutcome::Failed(error) = self.claude.send(spec, text) {
                self.finish_turn(&id, Err(error));
            }
        }
    }

    /// Whether side work (model listing) may start the Codex app-server:
    /// never while Claude Code is the active CLI.
    pub fn codex_may_start(&self) -> bool {
        self.active_cli != Some("claude")
    }

    /// Stop the active CLI's processes and hand the machine to `cli`.
    fn switch_cli(&mut self, cli: &'static str, for_agent: &str) {
        // Always stop the other CLI, even with no active one recorded yet.
        if cli == "codex" {
            self.claude.shutdown();
            self.codex.set_allowed(true);
        } else {
            // Block first so an in-flight warm-up can't restart it.
            self.codex.set_allowed(false);
            self.codex.shutdown();
        }
        if let Some(old) = self.active_cli {
            crate::log!("switching CLI: {old} -> {cli}");
            let note = json!({ "type": "notification", "level": "info", "message": format!("Switching from {} to {} (starting {})", cli_label(old), cli_label(cli), cli_label(cli)) });
            self.append_item(for_agent, note);
        }
        self.active_cli = Some(cli);
        self.claude_warm_at = None;
        self.codex_warm_at = None;
    }

    fn run_codex(&mut self, spec: AgentSpec, text: String) {
        let codex = self.codex.clone();
        let events = self.events_tx.clone();
        let id = spec.id.clone();
        tokio::spawn(async move {
            // `/compact` (typed, or queued by auto-compaction) maps to Codex's
            // native thread compaction; Claude handles the command itself.
            let result = if text.trim() == COMPACT_COMMAND { codex.compact(spec).await } else { codex.run_turn(spec, text).await };
            if let Err(error) = result {
                let _ = events.send(ProviderEvent::TurnFailed { agent_id: id, error });
            }
        });
    }

    /// True when the agent's last turn left its context window at or past
    /// `auto_compact_pct`.
    fn needs_compaction(&self, agent_id: &str) -> bool {
        let pct = self.cfg.auto_compact_pct as u64;
        let Some(a) = self.agents.get(agent_id) else { return false };
        if pct == 0 || a.rec.session_id.is_none() {
            return false;
        }
        let Some(usage) = &a.rec.last_usage else { return false };
        let get = |k: &str| usage.get(k).and_then(|v| v.as_u64()).filter(|n| *n > 0);
        match (get("contextWindowUsedTokens"), get("contextWindowMaxTokens")) {
            (Some(used), Some(max)) => used * 100 >= max * pct,
            _ => false,
        }
    }

    /// Run `/compact` as its own turn, ahead of the agent's queued prompts,
    /// so the next prompt is sent with a summarised (smaller) context.
    fn start_compaction(&mut self, agent_id: &str) {
        let provider = {
            let Some(agent) = self.agents.get_mut(agent_id) else { return };
            let turn_id = new_id();
            agent.status = "running";
            agent.active_turn = Some((turn_id, now_iso()));
            agent.compacting = true;
            agent.last_event = Instant::now();
            agent.retry_at = None;
            // Wait for fresh numbers before considering another compaction.
            if let Some(Value::Object(u)) = agent.rec.last_usage.as_mut() {
                u.remove("contextWindowUsedTokens");
            }
            agent.rec.provider.clone()
        };
        crate::log!("auto-compacting context for agent {agent_id}");
        let turn_id = self.agents[agent_id].active_turn.as_ref().map(|t| t.0.clone());
        let note = json!({ "type": "notification", "level": "info", "message": format!("Context is over {}% full; compacting to save tokens", self.cfg.auto_compact_pct) });
        self.append_item(agent_id, note);
        self.stream(agent_id, &provider, json!({ "type": "turn_started", "provider": provider, "turnId": turn_id }));
        self.touch(agent_id);
        self.publish_agent(agent_id);
        self.dispatch(agent_id, COMPACT_COMMAND.to_string());
    }

    /// End the active turn. `Ok(None)` completed, `Ok(Some(reason))` canceled, `Err` failed.
    pub fn finish_turn(&mut self, agent_id: &str, outcome: Result<Option<String>, String>) {
        let compacting = match self.agents.get_mut(agent_id) {
            // A scheduled retry owns the turn; late events from the dead attempt are ignored.
            Some(a) if a.status == "running" && a.retry_at.is_none() => std::mem::take(&mut a.compacting),
            _ => return,
        };
        // A failed auto-compaction is not the user's failure: note it and
        // carry on with the uncompacted context (the CLI's own limit still applies).
        let outcome = match outcome {
            Err(e) if compacting => {
                crate::warn!("auto-compaction failed for agent {agent_id}: {e}");
                self.append_item(agent_id, json!({ "type": "notification", "level": "warning", "message": format!("Auto-compaction failed: {e}") }));
                Ok(None)
            }
            other => other,
        };
        if !compacting {
            if let Some(reason) = self.retry_reason(agent_id, &outcome) {
                if self.schedule_retry(agent_id, &reason) {
                    return;
                }
            }
        }
        let (provider, turn_id) = {
            let Some(agent) = self.agents.get_mut(agent_id) else { return };
            let turn_id = agent.active_turn.take().map(|t| t.0);
            // Dropping the reply senders makes providers deny outstanding requests.
            let pending: Vec<String> = agent.permissions.drain(..).filter_map(|p| p.request.get("id").and_then(|i| i.as_str()).map(String::from)).collect();
            agent.status = if outcome.is_err() { "error" } else { "idle" };
            let reason = match &outcome {
                Ok(None) => Some("finished"),
                Err(_) => Some("error"),
                Ok(Some(_)) => None,
            };
            // The user's turn already raised its attention; the compaction after it doesn't.
            if !compacting {
                agent.rec.attention_reason = reason.map(String::from);
                agent.rec.attention_at = reason.map(|_| now_iso());
            }
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
        if !compacting {
            match &outcome {
                Ok(None) => self.notify_attention(agent_id, "finished"),
                Err(_) => self.notify_attention(agent_id, "error"),
                Ok(Some(_)) => {}
            }
        }

        // Free the CLI slot (or the queue entry of a turn that never started).
        self.running.remove(agent_id);
        self.cli_queue.retain(|(id, _)| id != agent_id);
        if provider == "claude" {
            for (spec, text) in self.claude.turn_finished(agent_id) {
                let id = spec.id.clone();
                if let SendOutcome::Failed(e) = self.claude.send(spec, text) {
                    self.finish_turn(&id, Err(e));
                }
            }
        }
        self.pump();
        if !compacting && matches!(outcome, Ok(None)) && self.needs_compaction(agent_id) {
            // Queued prompts for this agent run after the compaction finishes.
            self.start_compaction(agent_id);
            return;
        }
        let next = self.agents.get_mut(agent_id).and_then(|a| a.queue.pop_front());
        if let Some(text) = next {
            self.start_turn(agent_id, text, None);
        }
    }

    /// Why a finished turn should be retried automatically, if it should:
    /// a transient error, no output at all, or a stop in the middle of a
    /// tool call. Auth/quota errors and user cancels are never retried.
    fn retry_reason(&self, agent_id: &str, outcome: &Result<Option<String>, String>) -> Option<String> {
        if self.cfg.retry_max == 0 {
            return None;
        }
        let a = self.agents.get(agent_id)?;
        match outcome {
            Err(e) => retryable_error(e).then(|| e.clone()),
            Ok(None) if !a.produced => Some("the model returned no response".into()),
            Ok(None) => {
                let last = a.timeline.as_ref().and_then(|t| t.last())?;
                let kind = last.item.get("type").and_then(|t| t.as_str());
                let status = last.item.get("status").and_then(|t| t.as_str());
                (kind == Some("tool_call") && matches!(status, Some("canceled") | Some("running"))).then(|| "the model stopped in the middle of a tool call".into())
            }
            Ok(Some(_)) => None,
        }
    }

    /// Stop the failed attempt and re-dispatch after a backoff. False when the
    /// retries are used up (the failure is then reported as usual).
    fn schedule_retry(&mut self, agent_id: &str, reason: &str) -> bool {
        let (max, base) = (self.cfg.retry_max, self.cfg.retry_base);
        let Some(a) = self.agents.get_mut(agent_id) else { return false };
        if a.attempts >= max {
            return false;
        }
        a.attempts += 1;
        let attempt = a.attempts;
        let delay = (base * 2u32.saturating_pow(attempt - 1)).min(Duration::from_secs(60));
        a.retry_at = Some(Instant::now() + delay);
        // Output already streamed stays in the conversation: ask the model to
        // pick up from there. With nothing produced, resend the prompt.
        a.retry_text = if a.produced { CONTINUE_PROMPT.to_string() } else { a.turn_text.clone() };
        // Dropping the reply senders makes providers deny outstanding requests.
        a.permissions.clear();
        let provider = a.rec.provider.clone();
        crate::warn!("agent {agent_id}: {reason}; retry {attempt}/{max} in {}s", delay.as_secs());
        // A clean slate: a fresh CLI process resumes the session; Codex's turn is interrupted.
        self.running.remove(agent_id);
        self.cli_queue.retain(|(id, _)| id != agent_id);
        if provider == "codex" {
            self.codex.abandon(agent_id);
        } else {
            self.claude.stop(agent_id);
        }
        let note = json!({ "type": "notification", "level": "warning", "message": format!("Something went wrong ({}). Retrying automatically in {}s (attempt {attempt} of {max})", crate::util::truncate(reason, 160), delay.as_secs()) });
        self.append_item(agent_id, note);
        self.publish_agent(agent_id);
        self.pump();
        true
    }

    /// Housekeeping: re-dispatch due retries and unstick silent turns.
    fn run_retries_and_watchdog(&mut self) {
        let now = Instant::now();
        let due: Vec<(String, String)> = self.agents.values().filter(|a| a.retry_at.map(|t| now >= t).unwrap_or(false)).map(|a| (a.rec.id.clone(), a.retry_text.clone())).collect();
        for (id, text) in due {
            if let Some(a) = self.agents.get_mut(&id) {
                a.retry_at = None;
                a.last_event = Instant::now();
            }
            self.dispatch(&id, text);
        }
        let limit = self.cfg.stall_timeout;
        if limit.is_zero() || self.cfg.retry_max == 0 {
            return;
        }
        let stalled: Vec<String> = self
            .agents
            .values()
            .filter(|a| a.status == "running" && a.retry_at.is_none() && self.running.contains(&a.rec.id) && a.permissions.is_empty() && a.last_event.elapsed() >= limit)
            // A long-running tool call is quiet by nature.
            .filter(|a| !a.timeline.as_ref().and_then(|t| t.last()).map(|r| r.item.get("type").and_then(|t| t.as_str()) == Some("tool_call") && r.item.get("status").and_then(|s| s.as_str()) == Some("running")).unwrap_or(false))
            .map(|a| a.rec.id.clone())
            .collect();
        for id in stalled {
            crate::warn!("agent {id}: no output for {}s; treating the turn as stuck", limit.as_secs());
            self.finish_turn(&id, Err(format!("no output for {} minutes", (limit.as_secs() / 60).max(1))));
        }
    }

    pub fn cancel(&mut self, agent_id: &str) {
        let Some(agent) = self.agents.get_mut(agent_id) else { return };
        agent.queue.clear();
        if agent.status != "running" {
            return;
        }
        if agent.retry_at.take().is_some() {
            // Canceling while a retry is pending: the turn is simply over.
            self.finish_turn(agent_id, Ok(Some("canceled".into())));
            return;
        }
        let is_codex = agent.rec.provider == "codex";
        if self.cli_queue.iter().any(|(id, _)| id == agent_id) {
            self.finish_turn(agent_id, Ok(Some("canceled before start".into())));
            return;
        }
        if is_codex {
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
                if let Some(a) = self.agents.get_mut(&agent_id).filter(|a| a.status == "running" && a.retry_at.is_none()) {
                    a.last_event = Instant::now();
                    if matches!(item.get("type").and_then(|t| t.as_str()), Some("assistant_message" | "reasoning" | "tool_call")) {
                        a.produced = true;
                    }
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
                self.notify_attention(&agent_id, "permission");
            }
            ProviderEvent::Usage { agent_id, usage } => {
                if let Some(a) = self.agents.get_mut(&agent_id) {
                    let mut usage = usage;
                    // Claude doesn't report its window size; derive it from the model.
                    if a.rec.provider == "claude" && usage.get("contextWindowMaxTokens").map(|v| v.is_null()).unwrap_or(true) {
                        // From the model catalog; unknown or default model -> the configured window.
                        let model = a.rec.model.as_deref().unwrap_or("");
                        let window = crate::providers::claude_context_window(model).unwrap_or(if model.to_ascii_lowercase().contains("[1m]") { 1_000_000 } else { self.cfg.claude_context_window });
                        usage["contextWindowMaxTokens"] = json!(window);
                    }
                    a.rec.last_usage = Some(usage);
                }
            }
            ProviderEvent::TurnCompleted { agent_id } => self.finish_turn(&agent_id, Ok(None)),
            ProviderEvent::TurnCanceled { agent_id, reason } => self.finish_turn(&agent_id, Ok(Some(reason))),
            ProviderEvent::TurnFailed { agent_id, error } => self.finish_turn(&agent_id, Err(error)),
            ProviderEvent::ProcessExited { agent_id, generation } => {
                let (current, ready) = self.claude.process_exited(&agent_id, generation);
                let queued = self.claude.is_waiting(&agent_id) || self.cli_queue.iter().any(|(id, _)| id == &agent_id);
                let failed = current && self.agents.get(&agent_id).map(|a| a.status == "running").unwrap_or(false) && !queued;
                // Start the prompts that were waiting on this slot before
                // finish_turn drains the pool again, so they keep their order.
                for (spec, text) in ready {
                    let id = spec.id.clone();
                    if let SendOutcome::Failed(e) = self.claude.send(spec, text) {
                        self.finish_turn(&id, Err(e));
                    }
                }
                if failed {
                    self.finish_turn(&agent_id, Err("claude exited unexpectedly".into()));
                }
            }
        }
    }

    /// Record a `client_heartbeat` (focus, visibility, last activity).
    pub fn client_heartbeat(&mut self, msg: &Value, out: &Out) {
        self.clients.retain(|c| !c.out.is_closed());
        // Age of the client's last activity, from its own timestamp; never in the future.
        let age = msg
            .get("lastActivityAt")
            .and_then(|v| v.as_str())
            .and_then(|t| humantime::parse_rfc3339_weak(t).ok())
            .and_then(|t| std::time::SystemTime::now().duration_since(t).ok())
            .unwrap_or_default();
        let presence = Presence {
            out: out.clone(),
            focused_agent: msg.get("focusedAgentId").and_then(|v| v.as_str()).map(String::from),
            app_visible: msg.get("appVisible").and_then(|v| v.as_bool()).unwrap_or(false),
            active_at: Instant::now().checked_sub(age).unwrap_or_else(Instant::now),
        };
        match self.clients.iter_mut().find(|c| c.out.same_channel(out)) {
            Some(c) => *c = presence,
            None => self.clients.push(presence),
        }
    }

    /// Tell the user an agent needs them, like upstream: nothing if a present
    /// client is looking at that agent; an in-app notice to the most recently
    /// active present client; otherwise a phone push (not for errors).
    pub fn notify_attention(&mut self, agent_id: &str, reason: &'static str) {
        self.clients.retain(|c| !c.out.is_closed());
        // A backgrounded app (phone locked) is not present: it should get a push.
        let present: Vec<&Presence> = self.clients.iter().filter(|c| c.app_visible && c.active_at.elapsed() <= crate::push::PRESENCE_WINDOW).collect();
        if present.iter().any(|c| c.app_visible && c.focused_agent.as_deref() == Some(agent_id)) {
            return;
        }
        let recipient = present.iter().max_by_key(|c| c.active_at).map(|c| c.out.clone());
        // The workspace the app groups this agent under (same id as in its snapshot).
        let Some(workspace) = self.agents.get(agent_id).map(|a| a.rec.workspace_id.clone().unwrap_or_else(|| crate::snapshot::workspace_id(&a.rec.cwd))) else { return };
        let detail = match reason {
            "finished" => self.last_assistant_message(agent_id),
            "permission" => self.agents.get(agent_id).and_then(|a| a.permissions.last()).map(|p| {
                let title = p.request.get("title").and_then(|t| t.as_str()).unwrap_or("").trim().to_string();
                let desc = p.request.get("description").and_then(|t| t.as_str()).unwrap_or("").trim().to_string();
                match (title.is_empty(), desc.is_empty() || desc == title) {
                    (false, false) => format!("{title} - {desc}"),
                    (false, true) => title,
                    (true, false) => desc,
                    (true, true) => p.request.get("name").and_then(|n| n.as_str()).unwrap_or("tool").to_string(),
                }
            }),
            _ => None,
        };
        let payload = crate::push::attention_payload(reason, &self.server_id, &workspace, agent_id, detail.as_deref());
        match recipient {
            Some(out) => reply(&out, json!({ "type": "agent_attention_required", "payload": { "agentId": agent_id, "reason": reason, "timestamp": now_iso(), "shouldNotify": true, "notification": payload } })),
            None if reason != "error" => {
                let tokens = self.push_tokens.active();
                if tokens.is_empty() {
                    return;
                }
                crate::log!("push notification ({reason}) for agent {agent_id} to {} device(s)", tokens.len());
                let cmds = self.cmds_tx.clone();
                tokio::spawn(async move {
                    let dead = crate::push::send(tokens, payload).await;
                    if !dead.is_empty() {
                        let _ = cmds.send(Cmd::RevokePushTokens(dead));
                    }
                });
            }
            None => {}
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
            a.compacting = false;
            a.retry_at = None;
        }
        self.claude.stop(agent_id);
        self.cli_queue.retain(|(id, _)| id != agent_id);
        // Interrupt the turn in the shared app-server too, so the freed slot
        // doesn't overlap with an orphaned turn.
        self.codex.abandon(agent_id);
        if self.running.remove(agent_id) {
            self.pump();
        }
    }
}

impl Agent {
    pub fn new(rec: AgentRecord) -> Agent {
        Agent { rec, status: "idle", active_turn: None, queue: VecDeque::new(), permissions: Vec::new(), timeline: None, dirty_rows: BTreeSet::new(), last_touch: Instant::now(), compacting: false, turn_text: String::new(), attempts: 0, produced: false, last_event: Instant::now(), retry_at: None, retry_text: String::new() }
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

/// Sent when an interrupted turn is retried after it had already produced output.
const CONTINUE_PROMPT: &str = "Your previous response was interrupted unexpectedly. Continue from where you left off and finish the task.";

/// Transient failures are retried; auth, quota and setup problems won't fix
/// themselves, so those surface immediately.
fn retryable_error(error: &str) -> bool {
    const PERMANENT: [&str; 12] = [
        "not logged in", "log in", "login", "authentication", "unauthorized", "invalid api key", "credit balance", "usage limit", "quota", "failed to start", "no such file", "not found",
    ];
    let e = error.to_ascii_lowercase();
    !PERMANENT.iter().any(|p| e.contains(p))
}

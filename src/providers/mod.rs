//! Provider adapters. Both report into the manager through one event channel,
//! so the manager is the single owner of agent state.

pub mod claude;
pub mod claude_stream;
pub mod codex;
pub mod tools;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

pub type EventTx = mpsc::UnboundedSender<ProviderEvent>;

/// What a provider needs to run a turn for an agent.
#[derive(Clone, Debug)]
pub struct AgentSpec {
    pub id: String,
    pub cwd: String,
    pub model: Option<String>,
    pub mode_id: Option<String>,
    pub session_id: Option<String>,
    pub system_prompt: Option<String>,
}

pub enum ProviderEvent {
    SessionStarted { agent_id: String, session_id: String },
    TurnStarted { agent_id: String, turn_id: Option<String> },
    /// A timeline item. `assistant_message` / `reasoning` items are chunks the
    /// manager merges; `tool_call` items with a known `callId` replace that row.
    Timeline { agent_id: String, item: Value },
    Permission { agent_id: String, request: Value, reply: oneshot::Sender<Value> },
    Usage { agent_id: String, usage: Value },
    TurnCompleted { agent_id: String },
    TurnFailed { agent_id: String, error: String },
    TurnCanceled { agent_id: String, reason: String },
    /// Process-level exit (Claude) so the pool can free the slot.
    ProcessExited { agent_id: String, generation: u64 },
}

pub fn claude_modes() -> Value {
    serde_json::json!([
        { "id": "default", "label": "Always ask", "description": "Prompt for every tool that needs permission" },
        { "id": "acceptEdits", "label": "Accept edits", "description": "Auto-approve file edits" },
        { "id": "plan", "label": "Plan", "description": "Read-only planning" },
        { "id": "bypassPermissions", "label": "Bypass", "description": "Never ask (dangerous)" }
    ])
}

pub fn codex_modes() -> Value {
    serde_json::json!([
        { "id": "read-only", "label": "Read only", "description": "Read-only sandbox, ask before changes" },
        { "id": "auto", "label": "Auto", "description": "Workspace-write sandbox, ask when needed" },
        { "id": "full-access", "label": "Full access", "description": "No sandbox, never ask (dangerous)" }
    ])
}

pub fn default_mode(provider: &str) -> &'static str {
    if provider == "codex" { "auto" } else { "default" }
}

pub fn modes_for(provider: &str) -> Value {
    if provider == "codex" { codex_modes() } else { claude_modes() }
}

pub fn claude_models() -> Value {
    serde_json::json!([
        { "provider": "claude", "id": "default", "label": "Default", "isDefault": true },
        { "provider": "claude", "id": "opus", "label": "Opus" },
        { "provider": "claude", "id": "sonnet", "label": "Sonnet" },
        { "provider": "claude", "id": "haiku", "label": "Haiku" }
    ])
}

pub fn is_supported(provider: &str) -> bool {
    provider == "claude" || provider == "codex"
}

/// Locate an executable on PATH without spawning anything.
pub fn binary_available(bin: &str) -> bool {
    let p = std::path::Path::new(bin);
    if p.components().count() > 1 {
        return p.is_file();
    }
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(bin).is_file()))
        .unwrap_or(false)
}

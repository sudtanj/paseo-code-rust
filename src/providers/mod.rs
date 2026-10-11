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
    /// Reasoning effort; None = provider default.
    pub thinking: Option<String>,
}

pub enum ProviderEvent {
    SessionStarted { agent_id: String, session_id: String },
    TurnStarted { agent_id: String, turn_id: Option<String> },
    /// A timeline item. `assistant_message` / `reasoning` items are chunks the
    /// manager merges; `tool_call` items with a known `callId` replace that row.
    Timeline { agent_id: String, item: Value },
    Permission { agent_id: String, request: Value, reply: oneshot::Sender<Value> },
    Usage { agent_id: String, usage: Value },
    /// Running token/cost totals for the usage ledger.
    Spend { agent_id: String, sample: crate::usage::Sample },
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

/// Reasoning-effort levels passed to `claude --effort`.
pub fn claude_thinking_options() -> Value {
    serde_json::json!([
        { "id": "low", "label": "Low" },
        { "id": "medium", "label": "Medium" },
        { "id": "high", "label": "High" },
        { "id": "xhigh", "label": "Extra high" },
        { "id": "max", "label": "Max" }
    ])
}

/// Claude Code model catalog, ported from upstream's curated manifest
/// (providers/claude/model-manifest.ts). Upstream verifies it with a probe
/// process; paseo-lite skips the probe to avoid spawning `claude` just to
/// list models. Mythos 5.1 is omitted because upstream only shows it after
/// discovery confirms access.
pub fn claude_models() -> Value {
    const MODELS: &[(&str, &str, &str, u64)] = &[
        ("claude-opus-5-5", "Opus 5.5", "Opus 5.5 · Latest release", 1_000_000),
        ("claude-opus-5", "Opus 5", "Opus 5 · Previous release", 1_000_000),
        ("claude-fable-5-1", "Fable 5.1", "Fable 5.1 · Most powerful model", 1_000_000),
        ("claude-fable-5", "Fable 5", "Fable 5 · Previous release", 1_000_000),
        ("claude-opus-4-8[1m]", "Opus 4.8 1M", "Opus 4.8 with 1M context window", 1_000_000),
        ("claude-opus-4-8", "Opus 4.8", "Opus 4.8 · Previous release", 200_000),
        ("claude-sonnet-5-5", "Sonnet 5.5", "Sonnet 5.5 · Best for everyday tasks", 1_000_000),
        ("claude-sonnet-5", "Sonnet 5", "Sonnet 5 · Previous release", 200_000),
        ("claude-sonnet-5[1m]", "Sonnet 5 1M", "Sonnet 5 with 1M context window", 1_000_000),
        ("claude-opus-4-7[1m]", "Opus 4.7 1M", "Opus 4.7 with 1M context window", 1_000_000),
        ("claude-opus-4-7", "Opus 4.7", "Opus 4.7 · Previous release", 200_000),
        ("claude-opus-4-6[1m]", "Opus 4.6 1M", "Opus 4.6 with 1M context window", 1_000_000),
        ("claude-opus-4-6", "Opus 4.6", "Opus 4.6 · Most capable for complex work", 200_000),
        ("claude-sonnet-4-6[1m]", "Sonnet 4.6 1M", "Sonnet 4.6 with 1M context window", 1_000_000),
        ("claude-sonnet-4-6", "Sonnet 4.6", "Sonnet 4.6 · Best for everyday tasks", 200_000),
        ("claude-haiku-5-5", "Haiku 5.5", "Haiku 5.5 · Fastest for quick answers", 1_000_000),
        ("claude-haiku-4-5", "Haiku 4.5", "Haiku 4.5 · Previous release", 200_000),
    ];
    let thinking = claude_thinking_options();
    Value::Array(
        MODELS
            .iter()
            .map(|(id, label, description, context)| {
                let mut m = serde_json::json!({ "provider": "claude", "id": id, "label": label, "description": description, "contextWindowMaxTokens": context });
                if *id == "claude-opus-5-5" {
                    m["isDefault"] = serde_json::json!(true);
                }
                // Haiku models are offered without effort levels.
                if !id.contains("haiku") {
                    m["thinkingOptions"] = thinking.clone();
                }
                m
            })
            .collect(),
    )
}

/// Context window of a catalog model (None for unknown ids / the default).
pub fn claude_context_window(model: &str) -> Option<u64> {
    claude_models().as_array()?.iter().find(|m| m["id"].as_str() == Some(model)).and_then(|m| m["contextWindowMaxTokens"].as_u64())
}

pub fn is_supported(provider: &str) -> bool {
    provider == "claude" || provider == "codex"
}

/// Run an agent CLI below the daemon: lower CPU and I/O priority and a higher
/// OOM score. Children (tool commands, builds, test runs) inherit all three,
/// so a heavy agent task slows itself down instead of starving the daemon.
pub fn deprioritize(cmd: &mut tokio::process::Command, cfg: &crate::config::Config) {
    #[cfg(unix)]
    {
        let nice = cfg.agent_nice;
        let low_io = cfg.agent_low_io;
        let oom = (cfg.agent_oom_score_adj > 0).then(|| cfg.agent_oom_score_adj.to_string().into_bytes());
        // SAFETY: the closure runs between fork and exec, so it only makes
        // async-signal-safe libc calls and never allocates (`oom` is built
        // beforehand). Failures are ignored: priority is best effort.
        unsafe {
            cmd.pre_exec(move || {
                if nice > 0 {
                    libc::setpriority(libc::PRIO_PROCESS, 0, nice);
                }
                #[cfg(target_os = "linux")]
                {
                    if low_io {
                        // IOPRIO_WHO_PROCESS, self, best-effort class (2) at level 7 (lowest).
                        libc::syscall(libc::SYS_ioprio_set, 1, 0, (2 << 13) | 7);
                    }
                    if let Some(value) = &oom {
                        let fd = libc::open(c"/proc/self/oom_score_adj".as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC);
                        if fd >= 0 {
                            libc::write(fd, value.as_ptr().cast(), value.len());
                            libc::close(fd);
                        }
                    }
                }
                #[cfg(not(target_os = "linux"))]
                let _ = (low_io, &oom);
                Ok(())
            });
        }
    }
    #[cfg(not(unix))]
    let _ = (cmd, cfg);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_known_providers_are_supported() {
        assert!(is_supported("claude") && is_supported("codex"));
        assert!(!is_supported("gemini"));
    }

    #[test]
    fn default_modes() {
        assert_eq!(default_mode("codex"), "auto");
        assert_eq!(default_mode("claude"), "default");
    }

    #[test]
    fn claude_catalog_has_one_default_and_context_windows() {
        let models = claude_models();
        let defaults = models.as_array().unwrap().iter().filter(|m| m["isDefault"] == true).count();
        assert_eq!(defaults, 1);
        assert_eq!(claude_context_window("claude-opus-4-6"), Some(200_000));
        assert_eq!(claude_context_window("claude-opus-4-6[1m]"), Some(1_000_000));
        assert_eq!(claude_context_window("unknown"), None);
    }

    #[test]
    fn haiku_has_no_effort_levels() {
        let models = claude_models();
        let haiku = models.as_array().unwrap().iter().find(|m| m["id"] == "claude-haiku-4-5").unwrap();
        assert!(haiku.get("thinkingOptions").is_none());
    }

    #[test]
    fn missing_binary_is_unavailable() {
        assert!(!binary_available("definitely-not-a-real-binary-xyz"));
        assert!(binary_available("/bin/sh") || cfg!(windows));
    }
}

//! File-backed persistence.
//!
//! - `agents.json`: every agent record, rewritten atomically on change (small).
//! - `projects.json`: directories added as projects (with or without agents).
//! - `workspaces.json`: workspaces (a project directory agents are grouped under).
//! - `timelines/<agentId>.jsonl`: append-only rows. A later line with the same
//!   `seqStart` supersedes an earlier one, so in-place row updates (merged
//!   assistant chunks, tool-call status changes) are appends, never rewrites.
//!   Files are compacted on load when they carry more than 2x dead lines.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentRecord {
    pub id: String,
    pub provider: String,
    pub cwd: String,
    /// Workspace this agent belongs to (assigned on load for older records).
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub mode_id: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub last_user_message_at: Option<String>,
    /// Claude session uuid or Codex thread id.
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub archived_at: Option<String>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub last_usage: Option<Value>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub next_seq: u64,
    #[serde(default)]
    pub attention_reason: Option<String>,
    #[serde(default)]
    pub attention_at: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Row {
    pub seq_start: u64,
    pub seq_end: u64,
    #[serde(default)]
    pub ranges: Vec<(u64, u64)>,
    #[serde(default)]
    pub collapsed: Vec<String>,
    pub item: Value,
    #[serde(default)]
    pub turn_id: Option<String>,
    pub timestamp: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectRecord {
    pub root_path: String,
    pub created_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceRecord {
    pub id: String,
    /// Project id (the project root path, as everywhere else in paseo-lite).
    pub project_id: String,
    pub project_root: String,
    /// Directory agents in this workspace run in.
    pub directory: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub pinned_at: Option<String>,
    pub created_at: String,
    #[serde(default)]
    pub archived_at: Option<String>,
}

pub struct Store {
    root: PathBuf,
}

impl Store {
    pub fn new(root: PathBuf) -> Store {
        let _ = std::fs::create_dir_all(root.join("timelines"));
        Store { root }
    }

    pub fn load_agents(&self) -> Vec<AgentRecord> {
        let path = self.root.join("agents.json");
        match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
                crate::warn!("agents.json unreadable ({e}); starting empty");
                Vec::new()
            }),
            Err(_) => Vec::new(),
        }
    }

    pub fn save_agents<'a>(&self, agents: impl Iterator<Item = &'a AgentRecord>) {
        let list: Vec<&AgentRecord> = agents.collect();
        match serde_json::to_vec(&list) {
            Ok(bytes) => {
                if let Err(e) = crate::util::write_atomic(&self.root.join("agents.json"), &bytes, true) {
                    crate::warn!("failed to save agents: {e}");
                }
            }
            Err(e) => crate::warn!("failed to encode agents: {e}"),
        }
    }

    pub fn load_workspaces(&self) -> Vec<WorkspaceRecord> {
        match std::fs::read(self.root.join("workspaces.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
                crate::warn!("workspaces.json unreadable ({e}); starting empty");
                Vec::new()
            }),
            Err(_) => Vec::new(),
        }
    }

    pub fn save_workspaces(&self, workspaces: &[WorkspaceRecord]) {
        match serde_json::to_vec(workspaces) {
            Ok(bytes) => {
                if let Err(e) = crate::util::write_atomic(&self.root.join("workspaces.json"), &bytes, true) {
                    crate::warn!("failed to save workspaces: {e}");
                }
            }
            Err(e) => crate::warn!("failed to encode workspaces: {e}"),
        }
    }

    pub fn load_projects(&self) -> Vec<ProjectRecord> {
        match std::fs::read(self.root.join("projects.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
                crate::warn!("projects.json unreadable ({e}); starting empty");
                Vec::new()
            }),
            Err(_) => Vec::new(),
        }
    }

    pub fn save_projects(&self, projects: &[ProjectRecord]) {
        match serde_json::to_vec(projects) {
            Ok(bytes) => {
                if let Err(e) = crate::util::write_atomic(&self.root.join("projects.json"), &bytes, true) {
                    crate::warn!("failed to save projects: {e}");
                }
            }
            Err(e) => crate::warn!("failed to encode projects: {e}"),
        }
    }

    fn timeline_path(&self, agent_id: &str) -> PathBuf {
        // Agent ids are uuids we minted; reject anything path-like regardless.
        let safe: String = agent_id.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '-').collect();
        self.root.join("timelines").join(format!("{safe}.jsonl"))
    }

    pub fn load_timeline(&self, agent_id: &str) -> Vec<Row> {
        let path = self.timeline_path(agent_id);
        let Ok(file) = std::fs::File::open(&path) else { return Vec::new() };
        let mut rows: BTreeMap<u64, Row> = BTreeMap::new();
        let mut lines = 0usize;
        for line in std::io::BufReader::new(file).lines() {
            let Ok(line) = line else { break };
            lines += 1;
            if let Ok(row) = serde_json::from_str::<Row>(&line) {
                rows.insert(row.seq_start, row);
            }
        }
        let rows: Vec<Row> = rows.into_values().collect();
        if lines > 64 && lines > rows.len() * 2 {
            self.rewrite_timeline(agent_id, &rows);
        }
        rows
    }

    fn rewrite_timeline(&self, agent_id: &str, rows: &[Row]) {
        let mut buf = Vec::new();
        for row in rows {
            if let Ok(line) = serde_json::to_string(row) {
                buf.extend_from_slice(line.as_bytes());
                buf.push(b'\n');
            }
        }
        let _ = crate::util::write_atomic(&self.timeline_path(agent_id), &buf, true);
    }

    pub fn append_rows(&self, agent_id: &str, rows: &[&Row]) {
        if rows.is_empty() {
            return;
        }
        let path = self.timeline_path(agent_id);
        let mut opts = std::fs::OpenOptions::new();
        opts.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut buf = Vec::new();
        for row in rows {
            if let Ok(line) = serde_json::to_string(row) {
                buf.extend_from_slice(line.as_bytes());
                buf.push(b'\n');
            }
        }
        match opts.open(&path) {
            Ok(mut f) => {
                if let Err(e) = f.write_all(&buf) {
                    crate::warn!("timeline append failed for {agent_id}: {e}");
                }
            }
            Err(e) => crate::warn!("timeline open failed for {agent_id}: {e}"),
        }
    }

    pub fn delete_timeline(&self, agent_id: &str) {
        let _ = std::fs::remove_file(self.timeline_path(agent_id));
    }
}

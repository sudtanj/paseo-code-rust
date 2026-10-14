//! Wire projections in Paseo's shapes (`AgentSnapshotPayload`, project placement,
//! workspace descriptors, timeline entries).

use crate::manager::{Agent, EPOCH};
use crate::providers::{default_mode, modes_for};
use crate::store::Row;
use crate::util::basename;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Stable opaque workspace id derived from the directory (FNV-1a).
pub fn workspace_id(cwd: &str) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in cwd.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("ws_{h:016x}")
}

pub fn checkout(cwd: &str) -> Value {
    match crate::checkout::probe(cwd) {
        // Git variant of ProjectCheckoutLite; remoteUrl stays null to avoid a git spawn per snapshot.
        Some((root, branch)) => json!({ "cwd": cwd, "isGit": true, "currentBranch": branch, "remoteUrl": null, "worktreeRoot": root.to_string_lossy(), "isPaseoOwnedWorktree": false, "mainRepoRoot": null }),
        None => json!({ "cwd": cwd, "isGit": false, "currentBranch": null, "remoteUrl": null, "worktreeRoot": null, "isPaseoOwnedWorktree": false, "mainRepoRoot": null }),
    }
}

fn project_kind(cwd: &str) -> &'static str {
    if crate::checkout::probe(cwd).is_some() { "git" } else { "directory" }
}

/// User-chosen project names (`project.rename`), keyed by project root. The
/// manager owns persistence (`projects.json`); this is the read side for the
/// free-standing descriptor functions below. (Workspace titles live on the
/// workspace record.)
fn project_names() -> std::sync::MutexGuard<'static, HashMap<String, String>> {
    static NAMES: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    NAMES.get_or_init(Default::default).lock().unwrap()
}

pub fn set_project_name(path: &str, name: Option<&str>) {
    let mut names = project_names();
    match name.map(str::trim).filter(|n| !n.is_empty()) {
        Some(n) => names.insert(path.to_string(), n.to_string()),
        None => names.remove(path),
    };
}

fn project_custom_name(cwd: &str) -> Option<String> {
    project_names().get(cwd).cloned()
}

pub fn project_for(cwd: &str) -> Value {
    let project_name = project_custom_name(cwd).unwrap_or_else(|| basename(cwd));
    json!({ "projectKey": cwd, "projectName": project_name, "workspaceName": basename(cwd), "checkout": checkout(cwd) })
}

/// `WorkspaceProjectDescriptorPayload`; ids match `workspace_descriptor`.
pub fn project_descriptor(cwd: &str) -> Value {
    let custom = project_custom_name(cwd);
    json!({
        "projectId": cwd,
        "projectKey": cwd,
        "projectDisplayName": custom.clone().unwrap_or_else(|| basename(cwd)),
        "projectCustomName": custom,
        "projectRootPath": cwd,
        "projectKind": project_kind(cwd),
    })
}

impl Agent {
    pub fn project(&self) -> Value {
        project_for(&self.rec.cwd)
    }

    pub fn snapshot(&self) -> Value {
        let r = &self.rec;
        let mode = r.mode_id.clone().unwrap_or_else(|| default_mode(&r.provider).to_string());
        let persistence = r.session_id.as_ref().map(|sid| json!({ "provider": r.provider, "sessionId": sid }));
        let pending: Vec<Value> = self.permissions.iter().map(|p| p.request.clone()).collect();
        let mut snap = json!({
            "id": r.id,
            "provider": r.provider,
            "cwd": r.cwd,
            "workspaceId": r.workspace_id.clone().unwrap_or_else(|| workspace_id(&r.cwd)),
            "model": r.model,
            "thinkingOptionId": r.thinking_option_id,
            "createdAt": r.created_at,
            "updatedAt": r.updated_at,
            "lastUserMessageAt": r.last_user_message_at,
            "status": self.status,
            "activeTurn": self.active_turn.as_ref().map(|(id, at)| json!({ "turnId": id, "startedAt": at })),
            "capabilities": {
                "supportsStreaming": true,
                "supportsSessionPersistence": true,
                "supportsDynamicModes": true,
                "supportsMcpServers": false,
                "supportsReasoningStream": true,
                "supportsToolInvocations": true,
                "supportsRewindConversation": false,
                "supportsRewindFiles": false,
                "supportsRewindBoth": false,
            },
            "currentModeId": mode,
            "availableModes": modes_for(&r.provider),
            "pendingPermissions": pending,
            "persistence": persistence,
            "runtimeInfo": { "provider": r.provider, "sessionId": r.session_id, "model": r.model, "modeId": mode },
            "title": r.title,
            "labels": r.labels,
            "requiresAttention": r.attention_reason.is_some(),
            "attentionReason": r.attention_reason,
            "attentionTimestamp": r.attention_at,
            "archivedAt": r.archived_at,
        });
        if let Some(u) = &r.last_usage {
            snap["lastUsage"] = strip_nulls(u.clone());
        }
        if let Some(e) = &r.last_error {
            snap["lastError"] = json!(e);
        }
        snap
    }
}

/// Zod `.optional()` number fields reject `null`; drop them instead.
pub fn strip_nulls(v: Value) -> Value {
    match v {
        Value::Object(map) => Value::Object(map.into_iter().filter(|(_, v)| !v.is_null()).collect()),
        other => other,
    }
}

/// Recursively drop `null` members (Zod `.optional()` rejects null), except
/// members whose schema requires an explicit null: a tool call's `error`, and
/// `input`/`output` of an `unknown` tool detail. Elsewhere (e.g. a shell
/// detail's `output`) a null fails validation of the whole response.
pub fn clean(v: Value) -> Value {
    match v {
        Value::Object(map) => {
            let unknown_detail = map.get("type").and_then(|t| t.as_str()) == Some("unknown");
            Value::Object(
                map.into_iter()
                    .filter(|(k, v)| !v.is_null() || k == "error" || (unknown_detail && (k == "input" || k == "output")))
                    .map(|(k, v)| (k, clean(v)))
                    .collect(),
            )
        }
        Value::Array(items) => Value::Array(items.into_iter().map(clean).collect()),
        other => other,
    }
}

pub fn timeline_entry(provider: &str, row: &Row) -> Value {
    let ranges: Vec<Value> = row.ranges.iter().map(|(a, b)| json!({ "startSeq": a, "endSeq": b })).collect();
    let mut entry = json!({
        "provider": provider,
        // Rows saved by older versions may hold nulls the app rejects.
        "item": clean(row.item.clone()),
        "timestamp": row.timestamp,
        "seqStart": row.seq_start,
        "seqEnd": row.seq_end,
        "sourceSeqRanges": ranges,
        "collapsed": row.collapsed,
    });
    if let Some(t) = &row.turn_id {
        entry["turnId"] = json!(t);
    }
    entry
}

pub fn cursor(seq: u64) -> Value {
    json!({ "epoch": EPOCH, "seq": seq })
}

pub fn workspace_descriptor(ws: &crate::store::WorkspaceRecord, agents: &[&Agent]) -> Value {
    let dir = ws.directory.as_str();
    let root = ws.project_root.as_str();
    let running = agents.iter().any(|a| a.status == "running");
    let needs_input = agents.iter().any(|a| !a.permissions.is_empty());
    let failed = agents.iter().any(|a| a.status == "error");
    let attention = agents.iter().any(|a| a.rec.attention_reason.is_some());
    let status = if needs_input { "needs_input" } else if running { "running" } else if failed { "failed" } else if attention { "attention" } else { "done" };
    let activity = agents.iter().map(|a| a.rec.updated_at.clone()).max().unwrap_or_else(|| ws.created_at.clone());
    let probe = crate::checkout::probe(dir);
    // Untitled workspaces are named after their conversation (the first
    // agent's title, taken from its first message), not the branch, which
    // is usually the same `main` for every workspace in a project.
    let topic = agents
        .iter()
        .filter(|a| a.rec.title.as_deref().is_some_and(|t| !t.trim().is_empty()))
        .min_by(|a, b| a.rec.created_at.cmp(&b.rec.created_at))
        .and_then(|a| a.rec.title.clone());
    let name = ws.title.clone().or(topic).or_else(|| probe.as_ref().and_then(|(_, b)| b.clone())).unwrap_or_else(|| basename(dir));
    let custom = project_custom_name(root);
    json!({
        "id": ws.id,
        "projectId": ws.project_id,
        "projectDisplayName": custom.clone().unwrap_or_else(|| basename(root)),
        "projectCustomName": custom,
        "projectRootPath": root,
        "workspaceDirectory": dir,
        "projectKind": project_kind(root),
        "workspaceKind": if probe.is_some() { "local_checkout" } else { "directory" },
        "name": name,
        "title": ws.title,
        "pinnedAt": ws.pinned_at,
        "archivingAt": null,
        "status": status,
        "statusEnteredAt": null,
        "activityAt": activity,
        "scripts": [],
        "gitRuntime": probe.as_ref().map(|(_, branch)| json!({ "currentBranch": branch, "isPaseoOwnedWorktree": false })),
        "githubRuntime": null,
        "project": project_for(root),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_drops_nulls_but_keeps_tool_error() {
        let v = json!({ "type": "tool_call", "error": null, "extra": null, "detail": { "type": "shell", "command": "ls", "output": null } });
        let c = clean(v);
        assert!(c.get("extra").is_none());
        assert!(c["error"].is_null() && c.get("error").is_some());
        // A shell detail's null output fails upstream validation, so it must go.
        assert!(c["detail"].get("output").is_none());
    }

    #[test]
    fn clean_keeps_unknown_detail_input_output() {
        let v = json!({ "detail": { "type": "unknown", "input": null, "output": null } });
        let c = clean(v);
        assert!(c["detail"].get("input").is_some());
        assert!(c["detail"].get("output").is_some());
    }

    #[test]
    fn clean_recurses_into_arrays() {
        let c = clean(json!([{ "a": null, "b": 1 }]));
        assert_eq!(c, json!([{ "b": 1 }]));
    }

    #[test]
    fn strip_nulls_is_shallow() {
        let c = strip_nulls(json!({ "a": null, "b": { "c": null } }));
        assert_eq!(c, json!({ "b": { "c": null } }));
    }

    #[test]
    fn workspace_id_is_stable_and_distinct() {
        assert_eq!(workspace_id("/a"), workspace_id("/a"));
        assert_ne!(workspace_id("/a"), workspace_id("/b"));
        assert!(workspace_id("/a").starts_with("ws_"));
    }

    #[test]
    fn project_names_override_display() {
        set_project_name("/tmp/snapshot-test-project", Some("  Custom  "));
        assert_eq!(project_custom_name("/tmp/snapshot-test-project").as_deref(), Some("Custom"));
        set_project_name("/tmp/snapshot-test-project", None);
        assert!(project_custom_name("/tmp/snapshot-test-project").is_none());
    }
}

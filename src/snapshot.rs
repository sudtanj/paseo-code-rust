//! Wire projections in Paseo's shapes (`AgentSnapshotPayload`, project placement,
//! workspace descriptors, timeline entries).

use crate::manager::{Agent, EPOCH};
use crate::providers::{default_mode, modes_for};
use crate::store::Row;
use crate::util::basename;
use serde_json::{json, Value};

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
    json!({ "cwd": cwd, "isGit": false, "currentBranch": null, "remoteUrl": null, "worktreeRoot": null, "isPaseoOwnedWorktree": false, "mainRepoRoot": null })
}

pub fn project_for(cwd: &str) -> Value {
    json!({ "projectKey": cwd, "projectName": basename(cwd), "workspaceName": basename(cwd), "checkout": checkout(cwd) })
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
            "workspaceId": workspace_id(&r.cwd),
            "model": r.model,
            "thinkingOptionId": null,
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
/// members whose schema requires an explicit null.
pub fn clean(v: Value) -> Value {
    const KEEP_NULL: [&str; 3] = ["error", "input", "output"];
    match v {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter(|(k, v)| !v.is_null() || KEEP_NULL.contains(&k.as_str()))
                .map(|(k, v)| (k, clean(v)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(clean).collect()),
        other => other,
    }
}

pub fn timeline_entry(provider: &str, row: &Row) -> Value {
    let ranges: Vec<Value> = row.ranges.iter().map(|(a, b)| json!({ "startSeq": a, "endSeq": b })).collect();
    let mut entry = json!({
        "provider": provider,
        "item": row.item,
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

pub fn workspace_descriptor(cwd: &str, agents: &[&Agent]) -> Value {
    let running = agents.iter().any(|a| a.status == "running");
    let needs_input = agents.iter().any(|a| !a.permissions.is_empty());
    let failed = agents.iter().any(|a| a.status == "error");
    let status = if needs_input { "needs_input" } else if running { "running" } else if failed { "failed" } else { "done" };
    let activity = agents.iter().map(|a| a.rec.updated_at.clone()).max();
    json!({
        "id": workspace_id(cwd),
        "projectId": cwd,
        "projectDisplayName": basename(cwd),
        "projectRootPath": cwd,
        "workspaceDirectory": cwd,
        "projectKind": "directory",
        "workspaceKind": "directory",
        "name": basename(cwd),
        "archivingAt": null,
        "status": status,
        "statusEnteredAt": null,
        "activityAt": activity,
        "scripts": [],
        "gitRuntime": null,
        "githubRuntime": null,
        "project": project_for(cwd),
    })
}

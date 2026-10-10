//! Session RPC handlers for the Paseo core protocol subset.

use crate::manager::{reply, Agent, Manager, Out};
use crate::providers::{binary_available, claude_models, is_supported, modes_for};
use crate::snapshot::{cursor, timeline_entry, workspace_descriptor};
use crate::store::AgentRecord;
use crate::util::{new_id, now_iso};
use serde_json::{json, Value};
use std::collections::BTreeMap;

fn s(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(|x| x.to_string())
}

fn expand_home(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}{rest}");
        }
    }
    path.to_string()
}

fn normalize_dir(path: &str) -> Result<String, String> {
    let expanded = expand_home(path.trim());
    let canon = std::fs::canonicalize(&expanded).map_err(|e| format!("{expanded}: {e}"))?;
    if !canon.is_dir() {
        return Err(format!("{expanded} is not a directory"));
    }
    Ok(canon.to_string_lossy().into_owned())
}

impl Manager {
    fn resolve(&self, key: &str) -> Option<String> {
        if self.agents.contains_key(key) {
            return Some(key.to_string());
        }
        let by_prefix: Vec<&String> = self.agents.keys().filter(|id| id.starts_with(key)).collect();
        if by_prefix.len() == 1 && key.len() >= 4 {
            return Some(by_prefix[0].clone());
        }
        self.agents.values().find(|a| a.rec.title.as_deref() == Some(key)).map(|a| a.rec.id.clone())
    }

    fn directory_entries(&self, include_archived: bool) -> Vec<Value> {
        let mut list: Vec<&Agent> = self.agents.values().filter(|a| include_archived || a.rec.archived_at.is_none()).collect();
        list.sort_by(|a, b| b.rec.updated_at.cmp(&a.rec.updated_at));
        list.iter().map(|a| json!({ "agent": a.snapshot(), "project": a.project() })).collect()
    }

    pub fn handle_rpc(&mut self, msg: Value, out: &Out) {
        let kind = s(&msg, "type").unwrap_or_default();
        let request_id = s(&msg, "requestId");
        let rid = request_id.clone().unwrap_or_default();
        let agent_key = s(&msg, "agentId").unwrap_or_default();
        match kind.as_str() {
            "fetch_agents_request" | "fetch_agent_history_request" => {
                let include_archived = kind == "fetch_agent_history_request" || msg.pointer("/filter/includeArchived").and_then(|v| v.as_bool()).unwrap_or(false);
                let entries = self.directory_entries(include_archived);
                let response_type = kind.replace("_request", "_response");
                let mut payload = json!({ "requestId": rid, "entries": entries, "pageInfo": { "nextCursor": null, "prevCursor": null, "hasMore": false } });
                if msg.get("subscribe").map(|v| !v.is_null()).unwrap_or(false) {
                    // Every session already receives all agent updates.
                    payload["subscriptionId"] = json!(msg.pointer("/subscribe/subscriptionId").and_then(|v| v.as_str()).map(String::from).unwrap_or_else(new_id));
                }
                reply(out, json!({ "type": response_type, "payload": payload }));
            }
            "fetch_agent_request" => {
                let found = self.resolve(&agent_key).and_then(|id| self.agents.get(&id));
                let payload = match found {
                    Some(a) => json!({ "requestId": rid, "agent": a.snapshot(), "project": a.project(), "error": null }),
                    None => json!({ "requestId": rid, "agent": null, "error": "agent not found" }),
                };
                reply(out, json!({ "type": "fetch_agent_response", "payload": payload }));
            }
            "create_agent_request" => self.create_agent(&msg, &rid, out),
            "send_agent_message_request" | "send_agent_message" => {
                let text = s(&msg, "text").unwrap_or_default();
                let result = match self.resolve(&agent_key) {
                    Some(id) if !text.trim().is_empty() => self.submit_prompt(&id, text, s(&msg, "messageId")).map(|_| id),
                    Some(_) => Err("empty message".into()),
                    None => Err("agent not found".into()),
                };
                if kind == "send_agent_message_request" {
                    let (accepted, error) = match &result {
                        Ok(_) => (true, Value::Null),
                        Err(e) => (false, json!(e)),
                    };
                    let agent_id = result.clone().unwrap_or(agent_key);
                    reply(out, json!({ "type": "send_agent_message_response", "payload": { "requestId": rid, "agentId": agent_id, "accepted": accepted, "error": error } }));
                }
            }
            "cancel_agent_request" => {
                let id = self.resolve(&agent_key);
                if let Some(id) = &id {
                    self.cancel(id);
                }
                if request_id.is_some() {
                    let agent = id.as_ref().and_then(|i| self.agents.get(i)).map(|a| a.snapshot());
                    let error = if id.is_none() { json!("agent not found") } else { Value::Null };
                    reply(out, json!({ "type": "cancel_agent_response", "payload": { "requestId": rid, "agentId": agent_key, "agent": agent, "error": error } }));
                }
            }
            "agent_permission_response" => {
                if let Some(id) = self.resolve(&agent_key) {
                    let response = msg.get("response").cloned().unwrap_or_else(|| json!({ "behavior": "deny" }));
                    self.respond_permission(&id, &rid, response);
                }
            }
            "archive_agent_request" => {
                let Some(id) = self.resolve(&agent_key) else {
                    return rpc_error(out, &rid, &kind, "agent not found");
                };
                self.release_runtime(&id);
                let now = now_iso();
                if let Some(a) = self.agents.get_mut(&id) {
                    a.rec.archived_at = Some(now.clone());
                    a.rec.attention_reason = None;
                }
                self.touch(&id);
                self.publish_agent(&id);
                reply(out, json!({ "type": "agent_archived", "payload": { "agentId": id, "archivedAt": now, "requestId": rid } }));
            }
            "delete_agent_request" => {
                let Some(id) = self.resolve(&agent_key) else {
                    return rpc_error(out, &rid, &kind, "agent not found");
                };
                self.release_runtime(&id);
                self.agents.remove(&id);
                self.store.delete_timeline(&id);
                self.agents_dirty = true;
                self.broadcast(json!({ "type": "agent_update", "payload": { "kind": "remove", "agentId": id } }));
                self.broadcast(json!({ "type": "agent_deleted", "payload": { "agentId": id, "requestId": rid } }));
            }
            "clear_agent_attention" => {
                let ids: Vec<String> = match msg.get("agentId") {
                    Some(Value::Array(list)) => list.iter().filter_map(|v| v.as_str().map(String::from)).collect(),
                    Some(Value::String(one)) => vec![one.clone()],
                    _ => vec![],
                };
                for key in ids {
                    if let Some(id) = self.resolve(&key) {
                        if let Some(a) = self.agents.get_mut(&id) {
                            a.rec.attention_reason = None;
                            a.rec.attention_at = None;
                        }
                        self.agents_dirty = true;
                        self.publish_agent(&id);
                    }
                }
                if request_id.is_some() {
                    reply(out, json!({ "type": "clear_agent_attention_response", "payload": { "requestId": rid, "agentId": msg.get("agentId").cloned().unwrap_or(Value::Null), "ok": true } }));
                }
            }
            "fetch_agent_timeline_request" => self.fetch_timeline(&msg, &agent_key, &rid, out),
            "set_agent_mode_request" | "set_agent_model_request" => {
                let response_type = kind.replace("_request", "_response");
                let Some(id) = self.resolve(&agent_key) else {
                    return reply(out, json!({ "type": response_type, "payload": { "requestId": rid, "agentId": agent_key, "accepted": false, "error": "agent not found" } }));
                };
                if kind == "set_agent_mode_request" {
                    let mode = s(&msg, "modeId").unwrap_or_default();
                    let provider = self.agents[&id].rec.provider.clone();
                    if provider == "claude" {
                        self.claude.set_mode(&id, &mode);
                    }
                    if let Some(a) = self.agents.get_mut(&id) {
                        a.rec.mode_id = Some(mode);
                    }
                } else {
                    let model = s(&msg, "modelId");
                    if let Some(a) = self.agents.get_mut(&id) {
                        a.rec.model = model;
                    }
                    self.claude.restart_on_next_prompt(&id);
                }
                self.touch(&id);
                self.publish_agent(&id);
                reply(out, json!({ "type": response_type, "payload": { "requestId": rid, "agentId": id, "accepted": true, "error": null } }));
            }
            "list_provider_models_request" => {
                let provider = s(&msg, "provider").unwrap_or_default();
                let out = out.clone();
                if provider == "codex" {
                    let codex = self.codex.clone();
                    tokio::spawn(async move {
                        let (models, error) = match codex.list_models().await {
                            Ok(m) => (m, Value::Null),
                            Err(e) => (json!([]), json!(e)),
                        };
                        reply(&out, json!({ "type": "list_provider_models_response", "payload": { "provider": "codex", "models": crate::snapshot::clean(models), "error": error, "fetchedAt": now_iso(), "requestId": rid } }));
                    });
                } else {
                    reply(&out, json!({ "type": "list_provider_models_response", "payload": { "provider": provider, "models": claude_models(), "error": null, "fetchedAt": now_iso(), "requestId": rid } }));
                }
            }
            "list_provider_modes_request" => {
                let provider = s(&msg, "provider").unwrap_or_default();
                reply(out, json!({ "type": "list_provider_modes_response", "payload": { "provider": provider, "modes": modes_for(&provider), "error": null, "fetchedAt": now_iso(), "requestId": rid } }));
            }
            "list_available_providers_request" => {
                let providers = json!([
                    { "provider": "claude", "available": binary_available(&self.cfg.claude_bin) },
                    { "provider": "codex", "available": binary_available(&self.cfg.codex_bin) },
                ]);
                reply(out, json!({ "type": "list_available_providers_response", "payload": { "providers": providers, "error": null, "fetchedAt": now_iso(), "requestId": rid } }));
            }
            "get_providers_snapshot_request" => {
                let now = now_iso();
                let entry = |provider: &str, label: &str, bin: &str, models: Value| {
                    let ok = binary_available(bin);
                    json!({
                        "provider": provider,
                        "status": if ok { "ready" } else { "unavailable" },
                        "enabled": true,
                        "source": "builtin",
                        "label": label,
                        "models": models,
                        "modes": modes_for(provider),
                        "defaultModeId": crate::providers::default_mode(provider),
                        "fetchedAt": now,
                    })
                };
                let codex_models = json!([{ "provider": "codex", "id": "default", "label": "Default", "isDefault": true }]);
                let entries = json!([
                    entry("claude", "Claude Code", &self.cfg.claude_bin, claude_models()),
                    entry("codex", "Codex", &self.cfg.codex_bin, codex_models),
                ]);
                let mut payload = json!({ "entries": entries, "generatedAt": now, "requestId": rid });
                if let Some(cwd) = s(&msg, "cwd") {
                    payload["cwd"] = json!(cwd);
                }
                reply(out, json!({ "type": "get_providers_snapshot_response", "payload": payload }));
            }
            "fetch_workspaces_request" => {
                let mut by_cwd: BTreeMap<String, Vec<&Agent>> = BTreeMap::new();
                for a in self.agents.values().filter(|a| a.rec.archived_at.is_none()) {
                    by_cwd.entry(a.rec.cwd.clone()).or_default().push(a);
                }
                let entries: Vec<Value> = by_cwd.iter().map(|(cwd, agents)| workspace_descriptor(cwd, agents)).collect();
                reply(out, json!({ "type": "fetch_workspaces_response", "payload": { "requestId": rid, "entries": entries, "emptyProjects": [], "pageInfo": { "nextCursor": null, "prevCursor": null, "hasMore": false } } }));
            }
            "directory_suggestions_request" => {
                let query = s(&msg, "query").unwrap_or_default();
                let limit = msg.get("limit").and_then(|v| v.as_u64()).unwrap_or(30) as usize;
                let dirs = suggest_dirs(&query, limit);
                let entries: Vec<Value> = dirs.iter().map(|d| json!({ "path": d, "kind": "directory" })).collect();
                reply(out, json!({ "type": "directory_suggestions_response", "payload": { "directories": dirs, "entries": entries, "error": null, "requestId": rid } }));
            }
            "daemon.get_pairing_offer.request" => {
                let (url, qr) = match &self.pairing_url {
                    Some(u) => {
                        let svg = qrcode::QrCode::new(u.as_bytes()).ok().map(|c| c.render::<qrcode::render::svg::Color>().min_dimensions(240, 240).quiet_zone(true).build());
                        (u.clone(), svg)
                    }
                    None => (String::new(), None),
                };
                reply(out, json!({ "type": "daemon.get_pairing_offer.response", "payload": { "requestId": rid, "url": url, "qr": qr, "relayEnabled": self.pairing_url.is_some() } }));
            }
            "client_heartbeat" | "register_push_token" | "abort_request" => {}
            "ping" => {
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
                let mut payload = json!({ "requestId": rid, "serverReceivedAt": now, "serverSentAt": now });
                if let Some(sent) = msg.get("clientSentAt") {
                    payload["clientSentAt"] = sent.clone();
                }
                reply(out, json!({ "type": "pong", "payload": payload }));
            }
            _ => {
                if request_id.is_some() {
                    rpc_error(out, &rid, &kind, "not supported by paseo-lite");
                }
            }
        }
    }

    fn create_agent(&mut self, msg: &Value, rid: &str, out: &Out) {
        let config = msg.get("config").cloned().unwrap_or(Value::Null);
        let provider = s(&config, "provider").unwrap_or_default();
        let fail = |error: String| reply(out, json!({ "type": "status", "payload": { "status": "agent_create_failed", "requestId": rid, "error": error } }));
        if !is_supported(&provider) {
            return fail(format!("provider '{provider}' is not supported (claude, codex)"));
        }
        let cwd = match normalize_dir(&s(&config, "cwd").unwrap_or_default()) {
            Ok(c) => c,
            Err(e) => return fail(e),
        };
        let now = now_iso();
        let labels: BTreeMap<String, String> = msg
            .get("labels")
            .and_then(|l| l.as_object())
            .map(|m| m.iter().filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string()))).collect())
            .unwrap_or_default();
        let rec = AgentRecord {
            id: new_id(),
            provider,
            cwd,
            model: s(&config, "model").filter(|m| !m.is_empty()),
            mode_id: s(&config, "modeId"),
            title: s(&config, "title"),
            created_at: now.clone(),
            updated_at: now,
            last_user_message_at: None,
            session_id: None,
            archived_at: None,
            last_error: None,
            last_usage: None,
            system_prompt: s(&config, "systemPrompt"),
            labels,
            next_seq: 0,
            attention_reason: None,
            attention_at: None,
        };
        let id = rec.id.clone();
        let mut agent = Agent::new(rec);
        agent.timeline = Some(Vec::new());
        self.agents.insert(id.clone(), agent);
        self.agents_dirty = true;
        crate::log!("agent {id} created ({} in {})", self.agents[&id].rec.provider, self.agents[&id].rec.cwd);
        let snapshot = self.agents[&id].snapshot();
        reply(out, json!({ "type": "status", "payload": { "status": "agent_created", "agentId": id, "requestId": rid, "agent": snapshot } }));
        self.publish_agent(&id);
        if let Some(prompt) = s(msg, "initialPrompt").filter(|p| !p.trim().is_empty()) {
            let _ = self.submit_prompt(&id, prompt, s(msg, "clientMessageId"));
        }
    }

    fn fetch_timeline(&mut self, msg: &Value, agent_key: &str, rid: &str, out: &Out) {
        let direction = s(msg, "direction").unwrap_or_else(|| "tail".into());
        let projection = s(msg, "projection").unwrap_or_else(|| "projected".into());
        let limit = msg.get("limit").and_then(|v| v.as_u64()).unwrap_or(200) as usize;
        let cursor_seq = msg.pointer("/cursor/seq").and_then(|v| v.as_u64());
        let Some(id) = self.resolve(agent_key) else {
            return reply(out, json!({ "type": "fetch_agent_timeline_response", "payload": {
                "requestId": rid, "agentId": agent_key, "agent": null, "direction": direction, "projection": projection,
                "epoch": crate::manager::EPOCH, "reset": false, "staleCursor": false, "gap": false,
                "window": { "minSeq": 0, "maxSeq": 0, "nextSeq": 0 }, "startCursor": null, "endCursor": null,
                "hasOlder": false, "hasNewer": false, "entries": [], "error": "agent not found" } }));
        };
        let store = &self.store;
        let agent = self.agents.get_mut(&id).expect("resolved");
        agent.ensure_timeline(store);
        let rows = agent.timeline.as_ref().expect("loaded");
        let n = rows.len();
        let (start, end) = match (direction.as_str(), cursor_seq) {
            ("before", Some(c)) => {
                let end = rows.iter().position(|r| r.seq_start >= c).unwrap_or(n);
                (if limit == 0 { 0 } else { end.saturating_sub(limit) }, end)
            }
            ("after", Some(c)) => {
                let start = rows.iter().position(|r| r.seq_start > c).unwrap_or(n);
                (start, if limit == 0 { n } else { (start + limit).min(n) })
            }
            _ => (if limit == 0 { 0 } else { n.saturating_sub(limit) }, n),
        };
        let provider = agent.rec.provider.clone();
        let entries: Vec<Value> = rows[start..end].iter().map(|r| timeline_entry(&provider, r)).collect();
        let min_seq = rows.first().map(|r| r.seq_start).unwrap_or(0);
        let max_seq = rows.last().map(|r| r.seq_end).unwrap_or(0);
        let start_cursor = rows.get(start).filter(|_| start < end).map(|r| cursor(r.seq_start));
        let end_cursor = if end > start { Some(cursor(rows[end - 1].seq_end)) } else { None };
        let payload = json!({
            "requestId": rid,
            "agentId": id,
            "agent": agent.snapshot(),
            "direction": direction,
            "projection": projection,
            "epoch": crate::manager::EPOCH,
            "reset": false,
            "staleCursor": false,
            "gap": false,
            "window": { "minSeq": min_seq, "maxSeq": max_seq, "nextSeq": agent.rec.next_seq },
            "startCursor": start_cursor,
            "endCursor": end_cursor,
            "hasOlder": start > 0,
            "hasNewer": end < n,
            "entries": entries,
            "error": null,
        });
        reply(out, json!({ "type": "fetch_agent_timeline_response", "payload": payload }));
    }
}

pub fn rpc_error(out: &Out, request_id: &str, request_type: &str, error: &str) {
    reply(out, json!({ "type": "rpc_error", "payload": { "requestId": request_id, "requestType": request_type, "error": error, "code": "unsupported" } }));
}

/// Directory completion: `~/pro` -> children of `~` starting with `pro`.
pub fn suggest_dirs(query: &str, limit: usize) -> Vec<String> {
    let q = expand_home(if query.trim().is_empty() { "~/" } else { query.trim() });
    let (dir, prefix) = match q.rfind('/') {
        Some(i) => (q[..=i].to_string(), q[i + 1..].to_lowercase()),
        None => (expand_home("~/"), q.to_lowercase()),
    };
    let Ok(read) = std::fs::read_dir(&dir) else { return vec![] };
    let mut out: Vec<String> = read
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|name| (!name.starts_with('.') || prefix.starts_with('.')) && name.to_lowercase().starts_with(&prefix))
        .map(|name| format!("{}{}", dir, name))
        .collect();
    out.sort();
    out.truncate(limit.max(1));
    out
}

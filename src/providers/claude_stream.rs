//! Parse Claude Code `--output-format stream-json` lines into provider events.

use super::tools;
use super::{EventTx, ProviderEvent};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub struct StreamState {
    pub agent_id: String,
    pub events: EventTx,
    pub stdin: mpsc::UnboundedSender<String>,
    pub interrupted: Arc<AtomicBool>,
    pub session_id: Option<String>,
    /// tool_use_id -> (name, input), so results can rebuild the detail.
    tools: HashMap<String, (String, Value)>,
    /// Message ids whose text/thinking already arrived as stream deltas.
    streamed: HashSet<String>,
    current_message: Option<String>,
    /// Context window occupancy after the latest API call.
    context_tokens: Option<u64>,
}

fn s(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(|x| x.to_string())
}

impl StreamState {
    pub fn new(agent_id: String, events: EventTx, stdin: mpsc::UnboundedSender<String>, interrupted: Arc<AtomicBool>, session_id: Option<String>) -> Self {
        StreamState { agent_id, events, stdin, interrupted, session_id, tools: HashMap::new(), streamed: HashSet::new(), current_message: None, context_tokens: None }
    }

    fn emit(&self, ev: ProviderEvent) {
        let _ = self.events.send(ev);
    }

    fn timeline(&self, item: Value) {
        self.emit(ProviderEvent::Timeline { agent_id: self.agent_id.clone(), item });
    }

    pub fn handle_line(&mut self, line: &str) {
        let Ok(msg) = serde_json::from_str::<Value>(line) else { return };
        let is_sidechain = msg.get("parent_tool_use_id").map(|v| !v.is_null()).unwrap_or(false);
        match msg.get("type").and_then(|t| t.as_str()).unwrap_or("") {
            "system" if s(&msg, "subtype").as_deref() == Some("compact_boundary") => {
                let meta = msg.get("compact_metadata").or_else(|| msg.get("compactMetadata")).unwrap_or(&Value::Null);
                let pick = |a: &str, b: &str| meta.get(a).or_else(|| meta.get(b)).and_then(|v| v.as_u64());
                let post = pick("post_tokens", "postTokens");
                self.context_tokens = post;
                let trigger = if s(meta, "trigger").as_deref() == Some("manual") { "manual" } else { "auto" };
                let mut item = json!({ "type": "compaction", "status": "completed", "trigger": trigger });
                if let Some(pre) = pick("pre_tokens", "preTokens") {
                    item["preTokens"] = json!(pre);
                }
                self.timeline(item);
            }
            "system" => {
                if s(&msg, "subtype").as_deref() == Some("init") {
                    if let Some(sid) = s(&msg, "session_id") {
                        if self.session_id.as_deref() != Some(sid.as_str()) {
                            self.session_id = Some(sid.clone());
                            self.emit(ProviderEvent::SessionStarted { agent_id: self.agent_id.clone(), session_id: sid });
                        }
                    }
                }
            }
            "stream_event" if !is_sidechain => self.handle_stream_event(msg.get("event").unwrap_or(&Value::Null)),
            "assistant" if !is_sidechain => self.handle_assistant(&msg),
            "user" if !is_sidechain => self.handle_tool_results(&msg),
            "result" => self.handle_result(&msg),
            "control_request" => self.handle_control_request(&msg),
            _ => {}
        }
    }

    fn handle_stream_event(&mut self, ev: &Value) {
        match ev.get("type").and_then(|t| t.as_str()).unwrap_or("") {
            "message_start" => {
                self.current_message = ev.get("message").and_then(|m| s(m, "id"));
            }
            "content_block_delta" => {
                let delta = ev.get("delta").unwrap_or(&Value::Null);
                let (kind, text) = match delta.get("type").and_then(|t| t.as_str()) {
                    Some("text_delta") => ("assistant_message", s(delta, "text")),
                    Some("thinking_delta") => ("reasoning", s(delta, "thinking")),
                    _ => return,
                };
                if let Some(text) = text.filter(|t| !t.is_empty()) {
                    if let Some(id) = &self.current_message {
                        self.streamed.insert(id.clone());
                    }
                    self.timeline(json!({ "type": kind, "text": text }));
                }
            }
            _ => {}
        }
    }

    fn handle_assistant(&mut self, msg: &Value) {
        let message = msg.get("message").unwrap_or(&Value::Null);
        // Each API call reports the full prompt it was sent, so the latest one
        // is how much of the context window the conversation now occupies.
        if let Some(u) = message.get("usage") {
            let n = |k: &str| u.get(k).and_then(|v| v.as_u64()).unwrap_or(0);
            let used = n("input_tokens") + n("cache_creation_input_tokens") + n("cache_read_input_tokens") + n("output_tokens");
            if used > 0 {
                self.context_tokens = Some(used);
            }
        }
        let streamed = s(message, "id").map(|id| self.streamed.contains(&id)).unwrap_or(false);
        let Some(blocks) = message.get("content").and_then(|c| c.as_array()) else { return };
        for block in blocks {
            match block.get("type").and_then(|t| t.as_str()).unwrap_or("") {
                "text" if !streamed => {
                    if let Some(text) = s(block, "text").filter(|t| !t.is_empty()) {
                        self.timeline(json!({ "type": "assistant_message", "text": text }));
                    }
                }
                "thinking" if !streamed => {
                    if let Some(text) = s(block, "thinking").filter(|t| !t.is_empty()) {
                        self.timeline(json!({ "type": "reasoning", "text": text }));
                    }
                }
                "tool_use" | "server_tool_use" | "mcp_tool_use" => {
                    let id = s(block, "id").unwrap_or_else(crate::util::new_id);
                    let name = s(block, "name").unwrap_or_else(|| "tool".into());
                    let input = block.get("input").cloned().unwrap_or(Value::Null);
                    if name == "TodoWrite" {
                        if let Some(todo) = tools::claude_todo(&input) {
                            self.timeline(todo);
                        }
                        continue;
                    }
                    let detail = tools::claude_detail(&name, &input, None);
                    self.timeline(tools::tool_call(&id, &name, detail, "running", None));
                    self.tools.insert(id, (name, input));
                }
                _ => {}
            }
        }
    }

    fn handle_tool_results(&mut self, msg: &Value) {
        let Some(blocks) = msg.get("message").and_then(|m| m.get("content")).and_then(|c| c.as_array()) else { return };
        for block in blocks {
            if !block.get("type").and_then(|t| t.as_str()).map(|t| t.ends_with("tool_result")).unwrap_or(false) {
                continue;
            }
            let Some(id) = s(block, "tool_use_id") else { continue };
            let Some((name, input)) = self.tools.remove(&id) else { continue };
            let output = match block.get("content") {
                Some(Value::String(text)) => text.clone(),
                Some(Value::Array(parts)) => parts.iter().filter_map(|p| s(p, "text")).collect::<Vec<_>>().join("\n"),
                _ => String::new(),
            };
            let failed = block.get("is_error").and_then(|v| v.as_bool()).unwrap_or(false);
            let detail = tools::claude_detail(&name, &input, Some(&output));
            let (status, error) = if failed { ("failed", Some(output)) } else { ("completed", None) };
            self.timeline(tools::tool_call(&id, &name, detail, status, error));
        }
    }

    fn handle_result(&mut self, msg: &Value) {
        let agent_id = self.agent_id.clone();
        if let Some(u) = msg.get("usage") {
            let usage = json!({
                "inputTokens": u.get("input_tokens"),
                "cachedInputTokens": u.get("cache_read_input_tokens"),
                "outputTokens": u.get("output_tokens"),
                "totalCostUsd": msg.get("total_cost_usd"),
                "contextWindowUsedTokens": self.context_tokens,
            });
            self.emit(ProviderEvent::Usage { agent_id: agent_id.clone(), usage });
        }
        // Drop any tool rows still running; their results will never arrive.
        for (id, (name, input)) in self.tools.drain() {
            let detail = tools::claude_detail(&name, &input, None);
            let _ = self.events.send(ProviderEvent::Timeline { agent_id: agent_id.clone(), item: tools::tool_call(&id, &name, detail, "canceled", None) });
        }
        self.streamed.clear();
        let ok = s(msg, "subtype").as_deref() == Some("success") && !msg.get("is_error").and_then(|v| v.as_bool()).unwrap_or(false);
        if self.interrupted.swap(false, Ordering::SeqCst) {
            self.emit(ProviderEvent::TurnCanceled { agent_id, reason: "interrupted".into() });
        } else if ok {
            self.emit(ProviderEvent::TurnCompleted { agent_id });
        } else {
            let error = msg
                .get("errors")
                .and_then(|e| e.as_array())
                .map(|e| e.iter().filter_map(|x| x.as_str()).collect::<Vec<_>>().join("; "))
                .filter(|e| !e.is_empty())
                .or_else(|| s(msg, "result"))
                .unwrap_or_else(|| s(msg, "subtype").unwrap_or_else(|| "turn failed".into()));
            self.emit(ProviderEvent::TurnFailed { agent_id, error });
        }
    }

    fn handle_control_request(&mut self, msg: &Value) {
        let Some(request_id) = s(msg, "request_id") else { return };
        let req = msg.get("request").cloned().unwrap_or(Value::Null);
        if s(&req, "subtype").as_deref() != Some("can_use_tool") {
            // Unknown host callbacks (hooks, MCP) are answered with an error so the CLI never stalls.
            let resp = json!({ "type": "control_response", "response": { "subtype": "error", "request_id": request_id, "error": "unsupported" } });
            let _ = self.stdin.send(resp.to_string());
            return;
        }
        let name = s(&req, "tool_name").unwrap_or_else(|| "tool".into());
        let input = req.get("input").cloned().unwrap_or_else(|| json!({}));
        let kind = match name.as_str() {
            "ExitPlanMode" => "plan",
            "AskUserQuestion" => "question",
            _ => "tool",
        };
        let request = json!({
            "id": format!("perm-{}", crate::util::new_id()),
            "provider": "claude",
            "name": name,
            "kind": kind,
            "title": name,
            "input": input.clone(),
            "detail": tools::claude_detail(&name, &input, None),
        });
        let (tx, rx) = oneshot::channel::<Value>();
        self.emit(ProviderEvent::Permission { agent_id: self.agent_id.clone(), request, reply: tx });
        let stdin = self.stdin.clone();
        tokio::spawn(async move {
            // A dropped sender (agent canceled/archived) counts as a deny.
            let answer = rx.await.unwrap_or_else(|_| json!({ "behavior": "deny", "message": "Permission request canceled" }));
            let inner = if answer.get("behavior").and_then(|b| b.as_str()) == Some("allow") {
                json!({ "behavior": "allow", "updatedInput": answer.get("updatedInput").cloned().unwrap_or(input) })
            } else {
                json!({
                    "behavior": "deny",
                    "message": answer.get("message").and_then(|m| m.as_str()).unwrap_or("The user denied this tool call"),
                    "interrupt": answer.get("interrupt").and_then(|i| i.as_bool()).unwrap_or(false),
                })
            };
            let resp = json!({ "type": "control_response", "response": { "subtype": "success", "request_id": request_id, "response": inner } });
            let _ = stdin.send(resp.to_string());
        });
    }
}

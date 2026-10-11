//! Bytes sent to app clients, by message type, for diagnostics: shows which
//! messages cost the most data (relay and direct connections alike; before
//! the relay's encryption overhead).

use std::collections::HashMap;
use std::sync::Mutex;

static SENT: Mutex<Option<HashMap<String, (u64, u64)>>> = Mutex::new(None);

/// Count one outbound message.
pub fn count(msg: &str) {
    let kind = message_type(msg);
    let mut guard = SENT.lock().unwrap();
    let entry = guard.get_or_insert_with(HashMap::new).entry(kind.to_string()).or_default();
    entry.0 += 1;
    entry.1 += msg.len() as u64;
}

/// The inner type of a `session` envelope (`agent_stream`, …), else the
/// top-level type. Read from the text without parsing it: keys are written
/// sorted, so an object's `type` is its last key.
fn message_type(msg: &str) -> &str {
    let inner = msg.strip_suffix(r#","type":"session"}"#).unwrap_or(msg);
    let Some(i) = inner.rfind(r#""type":""#) else { return "other" };
    let start = i + r#""type":""#.len();
    inner[start..].find('"').map(|e| &inner[start..start + e]).unwrap_or("other")
}

/// Totals since start, biggest first: (type, messages, bytes).
pub fn snapshot() -> Vec<(String, u64, u64)> {
    let guard = SENT.lock().unwrap();
    let mut out: Vec<(String, u64, u64)> = guard.iter().flatten().map(|(k, (n, b))| (k.clone(), *n, *b)).collect();
    out.sort_by(|a, b| b.2.cmp(&a.2));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn types_are_read_from_the_envelope() {
        let wrapped = crate::manager::wrap(serde_json::json!({ "type": "agent_stream", "payload": { "event": { "type": "timeline" } } }));
        assert_eq!(message_type(&wrapped), "agent_stream");
        assert_eq!(message_type(r#"{"type":"pong"}"#), "pong");
        assert_eq!(message_type("garbage"), "other");
        count(r#"{"type":"egress_test"}"#);
        assert!(snapshot().iter().any(|(k, n, b)| k == "egress_test" && *n == 1 && *b == 22));
    }
}

//! The app-editable daemon config (`get_daemon_config` / `set_daemon_config`),
//! persisted in `daemon-config.json`. paseo-lite acts on:
//! - `providers.<id>.additionalModels`: custom models added to that
//!   provider's model list (snapshot, model picker),
//! - `providers.<id>.enabled`: shown as the provider's enabled flag,
//! - `appendSystemPrompt`: added to every agent's instructions.
//! Everything else is stored and echoed back so the settings screen
//! round-trips, but paseo-lite's own settings come from environment variables.

use crate::config::Config;
use serde_json::{json, Map, Value};
use std::path::PathBuf;

pub struct DaemonConfig {
    path: PathBuf,
    stored: Map<String, Value>,
}

/// Per-provider overrides applied to model lists (cheap to clone into tasks).
#[derive(Clone, Default)]
pub struct ProviderOverrides {
    stored: Map<String, Value>,
}

impl ProviderOverrides {
    fn provider(&self, id: &str) -> Option<&Value> {
        self.stored.get(id)
    }

    pub fn enabled(&self, id: &str) -> bool {
        self.provider(id).and_then(|p| p.get("enabled")).and_then(|v| v.as_bool()).unwrap_or(true)
    }

    /// Built-in models plus the user's `additionalModels`. A custom model with
    /// the same id replaces the built-in one; a custom default wins.
    pub fn merge_models(&self, id: &str, builtin: Value) -> Value {
        let mut models: Vec<Value> = builtin.as_array().cloned().unwrap_or_default();
        let extra: Vec<Value> = self
            .provider(id)
            .and_then(|p| p.get("additionalModels"))
            .and_then(|m| m.as_array())
            .map(|list| {
                list.iter()
                    .filter_map(|m| {
                        let model_id = m.get("id").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty())?;
                        let label = m.get("label").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty()).unwrap_or(model_id);
                        let mut out = json!({ "provider": id, "id": model_id, "label": label });
                        if let Some(d) = m.get("description").and_then(|v| v.as_str()) {
                            out["description"] = json!(d);
                        }
                        if m.get("isDefault").and_then(|v| v.as_bool()).unwrap_or(false) {
                            out["isDefault"] = json!(true);
                        }
                        Some(out)
                    })
                    .collect()
            })
            .unwrap_or_default();
        if extra.iter().any(|m| m.get("isDefault").is_some()) {
            for m in models.iter_mut() {
                if let Some(obj) = m.as_object_mut() {
                    obj.remove("isDefault");
                }
            }
        }
        for m in extra {
            match models.iter_mut().find(|b| b["id"] == m["id"]) {
                // Keep built-in extras (thinking options, context window) under the custom label.
                Some(b) => {
                    if let (Some(b), Some(m)) = (b.as_object_mut(), m.as_object()) {
                        for (k, v) in m {
                            b.insert(k.clone(), v.clone());
                        }
                    }
                }
                None => models.push(m),
            }
        }
        Value::Array(models)
    }
}

impl DaemonConfig {
    pub fn load(home: &std::path::Path) -> DaemonConfig {
        let path = home.join("daemon-config.json");
        let stored = std::fs::read(&path).ok().and_then(|b| serde_json::from_slice::<Value>(&b).ok()).and_then(|v| v.as_object().cloned()).unwrap_or_default();
        DaemonConfig { path, stored }
    }

    fn save(&self) {
        match serde_json::to_vec_pretty(&self.stored) {
            Ok(bytes) => {
                if let Err(e) = crate::util::write_atomic(&self.path, &bytes, true) {
                    crate::warn!("failed to save daemon config: {e}");
                }
            }
            Err(e) => crate::warn!("failed to encode daemon config: {e}"),
        }
    }

    pub fn overrides(&self) -> ProviderOverrides {
        ProviderOverrides { stored: self.stored.get("providers").and_then(|p| p.as_object()).cloned().unwrap_or_default() }
    }

    pub fn append_system_prompt(&self) -> Option<String> {
        self.stored.get("appendSystemPrompt").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty()).map(String::from)
    }

    /// The full `MutableDaemonConfig` the app expects (stored values over defaults).
    pub fn to_json(&self, cfg: &Config) -> Value {
        let mut out = self.stored.clone();
        out.insert("mcp".into(), json!({ "enabled": false, "injectIntoAgents": false }));
        out.insert("hostnames".into(), json!(cfg.hostnames));
        out.insert("app".into(), json!({ "baseUrl": cfg.app_base_url }));
        out.insert("relay".into(), json!({ "enabled": cfg.relay_enabled }));
        out.entry("providers").or_insert_with(|| json!({}));
        out.entry("appendSystemPrompt").or_insert_with(|| json!(""));
        Value::Object(out)
    }

    /// Apply a `MutableDaemonConfigPatch`. Provider entries merge field by
    /// field; `removeProviders` deletes entries; other keys replace.
    pub fn apply_patch(&mut self, patch: &Value) {
        let Some(patch) = patch.as_object() else { return };
        for (key, value) in patch {
            match key.as_str() {
                "providers" => {
                    let providers = self.stored.entry("providers").or_insert_with(|| json!({}));
                    if !providers.is_object() {
                        *providers = json!({});
                    }
                    let providers = providers.as_object_mut().expect("object");
                    for (id, fields) in value.as_object().into_iter().flatten() {
                        let entry = providers.entry(id.clone()).or_insert_with(|| json!({}));
                        if !entry.is_object() {
                            *entry = json!({});
                        }
                        let entry = entry.as_object_mut().expect("object");
                        for (k, v) in fields.as_object().into_iter().flatten() {
                            if v.is_null() {
                                entry.remove(k);
                            } else {
                                entry.insert(k.clone(), v.clone());
                            }
                        }
                    }
                }
                "removeProviders" => {
                    if let Some(providers) = self.stored.get_mut("providers").and_then(|p| p.as_object_mut()) {
                        for id in value.as_array().into_iter().flatten().filter_map(|v| v.as_str()) {
                            providers.remove(id);
                        }
                    }
                }
                // Environment-controlled in paseo-lite; not stored.
                "mcp" | "hostnames" | "app" | "relay" => {}
                _ => {
                    self.stored.insert(key.clone(), value.clone());
                }
            }
        }
        self.save();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> DaemonConfig {
        let dir = std::env::temp_dir().join(format!("paseo-lite-cfg-{}", crate::util::new_id()));
        std::fs::create_dir_all(&dir).unwrap();
        DaemonConfig::load(&dir)
    }

    #[test]
    fn additional_models_are_merged() {
        let mut c = config();
        c.apply_patch(&json!({ "providers": { "claude": { "additionalModels": [{ "id": "my-model", "label": "My Model" }] } } }));
        let merged = c.overrides().merge_models("claude", json!([{ "provider": "claude", "id": "opus", "label": "Opus" }]));
        let ids: Vec<&str> = merged.as_array().unwrap().iter().map(|m| m["id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["opus", "my-model"]);
    }

    #[test]
    fn custom_default_wins_and_same_id_replaces_label() {
        let mut c = config();
        c.apply_patch(&json!({ "providers": { "codex": { "additionalModels": [
            { "id": "gpt", "label": "Renamed", "isDefault": true } ] } } }));
        let merged = c.overrides().merge_models("codex", json!([
            { "provider": "codex", "id": "gpt", "label": "GPT", "isDefault": true, "thinkingOptions": [] },
            { "provider": "codex", "id": "other", "label": "Other", "isDefault": true } ]));
        let arr = merged.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["label"], "Renamed");
        assert!(arr[0].get("thinkingOptions").is_some(), "built-in extras are kept");
        assert!(arr[1].get("isDefault").is_none(), "only the custom default remains");
    }

    #[test]
    fn provider_patches_merge_and_remove() {
        let mut c = config();
        c.apply_patch(&json!({ "providers": { "claude": { "enabled": false } } }));
        assert!(!c.overrides().enabled("claude"));
        assert!(c.overrides().enabled("codex"));
        c.apply_patch(&json!({ "providers": { "claude": { "enabled": null } } }));
        assert!(c.overrides().enabled("claude"));
        c.apply_patch(&json!({ "providers": { "claude": { "enabled": false } } }));
        c.apply_patch(&json!({ "removeProviders": ["claude"] }));
        assert!(c.overrides().enabled("claude"));
    }

    #[test]
    fn append_system_prompt_round_trip_and_persistence() {
        let dir = std::env::temp_dir().join(format!("paseo-lite-cfg-{}", crate::util::new_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut c = DaemonConfig::load(&dir);
        assert!(c.append_system_prompt().is_none());
        c.apply_patch(&json!({ "appendSystemPrompt": "  be brief  " }));
        assert_eq!(c.append_system_prompt().as_deref(), Some("be brief"));
        let reloaded = DaemonConfig::load(&dir);
        assert_eq!(reloaded.append_system_prompt().as_deref(), Some("be brief"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn environment_controlled_keys_are_not_stored() {
        let mut c = config();
        c.apply_patch(&json!({ "relay": { "enabled": false }, "hostnames": ["x"] }));
        assert!(c.stored.get("relay").is_none() && c.stored.get("hostnames").is_none());
    }
}

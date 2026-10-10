//! Workspaces: a named place inside a project where agents run. The app's
//! new-workspace screen creates one (`workspace.create`) before its first
//! agent, and the sidebar lists them via `fetch_workspaces` + `workspace_update`.
//! Only local (directory) workspaces are supported; git worktree isolation is
//! not, so the app keeps its default "local" isolation.

use crate::manager::Manager;
use crate::snapshot::{project_descriptor, workspace_descriptor};
use crate::store::{ProjectRecord, WorkspaceRecord};
use crate::util::now_iso;
use serde_json::{json, Value};

fn new_workspace_id() -> String {
    format!("wks_{}", &uuid::Uuid::new_v4().simple().to_string()[..16])
}

impl Manager {
    /// Project root a directory belongs to: the longest registered project
    /// containing it, else the directory itself (registered quietly).
    fn project_root_for(&mut self, dir: &str) -> String {
        let found = self
            .projects
            .iter()
            .filter(|p| dir == p.root_path || dir.starts_with(&format!("{}/", p.root_path.trim_end_matches('/'))))
            .max_by_key(|p| p.root_path.len())
            .map(|p| p.root_path.clone());
        found.unwrap_or_else(|| {
            self.projects.push(ProjectRecord { root_path: dir.to_string(), created_at: now_iso() });
            self.store.save_projects(&self.projects);
            dir.to_string()
        })
    }

    pub fn create_workspace(&mut self, project_root: &str, directory: &str) -> String {
        let id = new_workspace_id();
        self.workspaces.push(WorkspaceRecord {
            id: id.clone(),
            project_id: project_root.to_string(),
            project_root: project_root.to_string(),
            directory: directory.to_string(),
            title: None,
            pinned_at: None,
            created_at: now_iso(),
            archived_at: None,
        });
        self.store.save_workspaces(&self.workspaces);
        id
    }

    /// Workspace for agents created without one (older clients, the built-in UI).
    pub fn find_or_create_workspace(&mut self, dir: &str) -> String {
        if let Some(ws) = self.workspaces.iter().find(|w| w.archived_at.is_none() && w.directory == dir) {
            return ws.id.clone();
        }
        let root = self.project_root_for(dir);
        self.create_workspace(&root, dir)
    }

    /// Give every pre-workspace agent a workspace (one per directory).
    pub fn migrate_agent_workspaces(&mut self) {
        let orphans: Vec<(String, String)> = self
            .agents
            .values()
            .filter(|a| a.rec.workspace_id.as_ref().map(|id| !self.workspaces.iter().any(|w| &w.id == id)).unwrap_or(true))
            .map(|a| (a.rec.id.clone(), a.rec.cwd.clone()))
            .collect();
        for (agent_id, cwd) in &orphans {
            let ws = self.find_or_create_workspace(cwd);
            if let Some(a) = self.agents.get_mut(agent_id) {
                a.rec.workspace_id = Some(ws);
            }
        }
        if !orphans.is_empty() {
            crate::log!("assigned workspaces to {} older agents", orphans.len());
            self.agents_dirty = true;
        }
    }

    pub fn workspace_json(&self, id: &str) -> Option<Value> {
        let ws = self.workspaces.iter().find(|w| w.id == id)?;
        let agents: Vec<&crate::manager::Agent> = self
            .agents
            .values()
            .filter(|a| a.rec.workspace_id.as_deref() == Some(id) && a.rec.archived_at.is_none())
            .collect();
        Some(workspace_descriptor(ws, &agents))
    }

    pub fn publish_workspace(&self, id: &str) {
        let Some(ws) = self.workspaces.iter().find(|w| w.id == id) else { return };
        if ws.archived_at.is_some() {
            return;
        }
        if let Some(desc) = self.workspace_json(id) {
            self.broadcast(json!({ "type": "workspace_update", "payload": { "kind": "upsert", "workspace": desc } }));
        }
    }

    /// Active workspaces, most recently active first.
    pub fn workspace_entries(&self, project_filter: Option<&str>) -> Vec<Value> {
        let mut list: Vec<Value> = self
            .workspaces
            .iter()
            .filter(|w| w.archived_at.is_none() && project_filter.map(|p| p == w.project_id).unwrap_or(true))
            .filter_map(|w| self.workspace_json(&w.id))
            .collect();
        let key = |v: &Value| v["activityAt"].as_str().or(v["createdAt"].as_str()).unwrap_or("").to_string();
        list.sort_by(|a, b| key(b).cmp(&key(a)));
        list
    }

    /// Registered projects with no active workspace.
    pub fn empty_projects(&self) -> Vec<Value> {
        self.projects
            .iter()
            .filter(|p| !self.workspaces.iter().any(|w| w.archived_at.is_none() && w.project_root == p.root_path))
            .map(|p| project_descriptor(&p.root_path))
            .collect()
    }

    /// Archive a workspace and its agents. Returns the archive timestamp.
    pub fn archive_workspace(&mut self, id: &str) -> Result<String, String> {
        let Some(idx) = self.workspaces.iter().position(|w| w.id == id) else {
            return Err("workspace not found".into());
        };
        let now = now_iso();
        let agent_ids: Vec<String> = self.agents.values().filter(|a| a.rec.workspace_id.as_deref() == Some(id) && a.rec.archived_at.is_none()).map(|a| a.rec.id.clone()).collect();
        for agent_id in &agent_ids {
            self.release_runtime(agent_id);
            if let Some(a) = self.agents.get_mut(agent_id) {
                a.rec.archived_at = Some(now.clone());
                a.rec.attention_reason = None;
            }
            self.touch(agent_id);
            self.broadcast(json!({ "type": "agent_update", "payload": { "kind": "upsert", "agent": self.agents[agent_id].snapshot(), "project": self.agents[agent_id].project() } }));
        }
        self.workspaces[idx].archived_at = Some(now.clone());
        let root = self.workspaces[idx].project_root.clone();
        self.store.save_workspaces(&self.workspaces);
        let mut payload = json!({ "kind": "remove", "id": id });
        if !self.workspaces.iter().any(|w| w.archived_at.is_none() && w.project_root == root) {
            payload["emptyProject"] = project_descriptor(&root);
        }
        self.broadcast(json!({ "type": "workspace_update", "payload": payload }));
        Ok(now)
    }

    /// Apply `f` to a workspace, persist, publish. Returns the updated record.
    pub fn update_workspace(&mut self, id: &str, f: impl FnOnce(&mut WorkspaceRecord)) -> Option<WorkspaceRecord> {
        let ws = self.workspaces.iter_mut().find(|w| w.id == id && w.archived_at.is_none())?;
        f(ws);
        let updated = ws.clone();
        self.store.save_workspaces(&self.workspaces);
        self.publish_workspace(id);
        Some(updated)
    }
}

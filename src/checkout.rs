//! Git checkout status and diffs for the app's Changes panel
//! (`checkout_status`, `checkout.diff.get`, `subscribe_checkout_diff`).
//!
//! Semantics follow the Node daemon: "uncommitted" = working tree vs HEAD plus
//! untracked files; "base" = merge-base(default branch, HEAD)..HEAD. Live
//! subscriptions are refreshed on a slow timer and whenever an agent turn
//! ends, and only pushed when the diff actually changed.

use crate::manager::{wrap, Out};
use crate::projects::run;
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

const GIT_TIMEOUT: Duration = Duration::from_secs(30);
const TOTAL_DIFF_MAX_BYTES: usize = 2 * 1024 * 1024;
const FILE_MAX_LINES: usize = 5000;
const UNTRACKED_MAX_FILES: usize = 200;
const UNTRACKED_MAX_BYTES: u64 = 256 * 1024;
const EMPTY_TREE: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";
/// Slow fallback poll; turn completions trigger an immediate refresh.
const REFRESH_EVERY: Duration = Duration::from_secs(20);

fn expand(cwd: &str) -> PathBuf {
    let p = match (cwd.strip_prefix('~'), std::env::var("HOME")) {
        (Some(rest), Ok(home)) if rest.is_empty() || rest.starts_with('/') => format!("{home}{rest}"),
        _ => cwd.to_string(),
    };
    PathBuf::from(p)
}

async fn git(cwd: &Path, args: &[&str]) -> Result<String, String> {
    run("git", args, Some(cwd), GIT_TIMEOUT).await
}

async fn git_ok(cwd: &Path, args: &[&str]) -> Option<String> {
    git(cwd, args).await.ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

fn checkout_error(message: impl Into<String>, code: &str) -> Value {
    json!({ "code": code, "message": message.into() })
}

/// Cheap filesystem-only probe used by project/workspace descriptors (no git spawn).
pub fn probe(cwd: &str) -> Option<(PathBuf, Option<String>)> {
    let mut dir = Some(Path::new(cwd));
    while let Some(d) = dir {
        let dot = d.join(".git");
        if dot.exists() {
            let branch = std::fs::read_to_string(dot.join("HEAD"))
                .ok()
                .and_then(|h| h.trim().strip_prefix("ref: refs/heads/").map(String::from));
            return Some((d.to_path_buf(), branch));
        }
        dir = d.parent();
    }
    None
}

/// Default branch name ("main"), as upstream resolves it.
async fn default_branch(cwd: &Path) -> Option<String> {
    if let Some(r) = git_ok(cwd, &["symbolic-ref", "--quiet", "refs/remotes/origin/HEAD"]).await {
        return r.strip_prefix("refs/remotes/origin/").map(String::from);
    }
    for name in ["main", "master"] {
        if git(cwd, &["show-ref", "--verify", "--quiet", &format!("refs/heads/{name}")]).await.is_ok()
            || git(cwd, &["show-ref", "--verify", "--quiet", &format!("refs/remotes/origin/{name}")]).await.is_ok()
        {
            return Some(name.to_string());
        }
    }
    None
}

/// `checkout_status_response` payload.
pub async fn status(cwd: &str, request_id: &str) -> Value {
    let dir = expand(cwd);
    let not_git = |error: Value| {
        json!({
            "cwd": cwd, "error": error, "requestId": request_id, "isGit": false, "isPaseoOwnedWorktree": false,
            "repoRoot": null, "currentBranch": null, "isDirty": null, "baseRef": null, "aheadBehind": null,
            "aheadOfOrigin": null, "behindOfOrigin": null, "hasRemote": false, "remoteUrl": null,
        })
    };
    if !dir.is_dir() {
        return not_git(checkout_error(format!("Directory not found: {cwd}"), "UNKNOWN"));
    }
    let Some(repo_root) = git_ok(&dir, &["rev-parse", "--show-toplevel"]).await else {
        return not_git(Value::Null);
    };
    let branch = git_ok(&dir, &["symbolic-ref", "--quiet", "--short", "HEAD"]).await;
    let dirty = git(&dir, &["status", "--porcelain", "--untracked-files=normal"]).await.map(|s| !s.trim().is_empty()).unwrap_or(false);
    let upstream = git_ok(&dir, &["rev-parse", "--symbolic-full-name", "@{upstream}"]).await;
    let (ahead_origin, behind_origin) = match &upstream {
        Some(_) => match git_ok(&dir, &["rev-list", "--left-right", "--count", "HEAD...@{upstream}"]).await {
            Some(counts) => {
                let mut it = counts.split_whitespace().filter_map(|n| n.parse::<u64>().ok());
                (it.next().map(Value::from).unwrap_or(Value::Null), it.next().map(Value::from).unwrap_or(Value::Null))
            }
            None => (Value::Null, Value::Null),
        },
        None => (Value::Null, Value::Null),
    };
    let base = default_branch(&dir).await;
    let ahead_behind = match &base {
        Some(b) => {
            let base_ref = best_base_ref(&dir, b).await;
            git_ok(&dir, &["rev-list", "--left-right", "--count", &format!("HEAD...{base_ref}")]).await.and_then(|c| {
                let v: Vec<u64> = c.split_whitespace().filter_map(|n| n.parse().ok()).collect();
                (v.len() == 2).then(|| json!({ "ahead": v[0], "behind": v[1] }))
            })
        }
        None => None,
    };
    let remotes = git_ok(&dir, &["remote"]).await;
    let remote_url = git_ok(&dir, &["remote", "get-url", "origin"]).await;
    json!({
        "cwd": cwd, "error": null, "requestId": request_id, "upstreamRef": upstream,
        "isGit": true, "isPaseoOwnedWorktree": false, "repoRoot": repo_root, "mainRepoRoot": null,
        "currentBranch": branch, "isDirty": dirty, "baseRef": base, "aheadBehind": ahead_behind,
        "aheadOfOrigin": ahead_origin, "behindOfOrigin": behind_origin,
        "hasRemote": remotes.is_some(), "remoteUrl": remote_url,
    })
}

/// Prefer `origin/<name>` when it exists (what upstream compares against).
async fn best_base_ref(cwd: &Path, name: &str) -> String {
    let origin = format!("origin/{name}");
    if git(cwd, &["rev-parse", "--verify", "--quiet", &origin]).await.is_ok() { origin } else { name.to_string() }
}

/// Parse `git diff` text into `ParsedDiffFile`s (mirrors diff-highlighter.ts).
pub fn parse_diff(text: &str) -> Vec<Value> {
    let mut files = Vec::new();
    for section in text.split("\ndiff --git ").map(|s| s.strip_prefix("diff --git ").unwrap_or(s)) {
        if section.trim().is_empty() {
            continue;
        }
        let lines: Vec<&str> = section.split('\n').collect();
        let header = lines[0];
        let mut path = header
            .split_once(" b/")
            .map(|(_, b)| b.to_string())
            .unwrap_or_else(|| header.to_string());
        let mut old_path: Option<String> = header.strip_prefix("a/").and_then(|a| a.split_once(" b/")).map(|(a, _)| a.to_string());
        let mut is_new = false;
        let mut is_deleted = false;
        let mut binary = false;
        let mut hunks: Vec<Value> = Vec::new();
        let mut cur: Option<(Vec<u64>, Vec<Value>)> = None;
        let (mut adds, mut dels, mut count) = (0u64, 0u64, 0usize);
        for line in &lines[1..] {
            if line.starts_with("new file mode") || *line == "--- /dev/null" {
                is_new = true;
            } else if line.starts_with("deleted file mode") || *line == "+++ /dev/null" {
                is_deleted = true;
            } else if line.starts_with("Binary files ") {
                binary = true;
            } else if let Some(p) = line.strip_prefix("+++ b/") {
                path = p.to_string();
            } else if let Some(p) = line.strip_prefix("--- a/") {
                old_path = Some(p.to_string());
            }
            if line.starts_with("index ") || line.starts_with("--- ") || line.starts_with("+++ ") || line.starts_with("new file mode") || line.starts_with("deleted file mode") {
                continue;
            }
            if let Some(h) = parse_hunk_header(line) {
                if let Some((n, l)) = cur.take() {
                    hunks.push(hunk(n, l));
                }
                cur = Some((h, vec![json!({ "type": "header", "content": line.split(" @@").next().map(|s| format!("{s} @@")).unwrap_or_else(|| line.to_string()) })]));
                continue;
            }
            let Some((_, out)) = cur.as_mut() else { continue };
            count += 1;
            let (kind, content) = match line.chars().next() {
                Some('+') => {
                    adds += 1;
                    ("add", &line[1..])
                }
                Some('-') => {
                    dels += 1;
                    ("remove", &line[1..])
                }
                Some(' ') => ("context", &line[1..]),
                Some('\\') | None => continue,
                _ => ("context", *line),
            };
            if count <= FILE_MAX_LINES {
                out.push(json!({ "type": kind, "content": content }));
            }
        }
        if let Some((n, l)) = cur.take() {
            hunks.push(hunk(n, l));
        }
        let status = if binary { "binary" } else if count > FILE_MAX_LINES { "too_large" } else { "ok" };
        if status != "ok" {
            hunks.clear();
        }
        let mut file = json!({ "path": path, "isNew": is_new, "isDeleted": is_deleted, "additions": adds, "deletions": dels, "hunks": hunks, "status": status });
        if let Some(old) = old_path.filter(|o| *o != file["path"].as_str().unwrap_or("")) {
            file["oldPath"] = json!(old);
        }
        files.push(file);
    }
    files
}

fn parse_hunk_header(line: &str) -> Option<Vec<u64>> {
    let body = line.strip_prefix("@@ -")?;
    let (ranges, _) = body.split_once(" @@")?;
    let (old, new) = ranges.split_once(" +")?;
    let pair = |r: &str| -> Option<(u64, u64)> {
        let mut it = r.splitn(2, ',');
        let start = it.next()?.parse().ok()?;
        let count = it.next().map(|c| c.parse().ok()).unwrap_or(Some(1))?;
        Some((start, count))
    };
    let (os, oc) = pair(old)?;
    let (ns, nc) = pair(new)?;
    Some(vec![os, oc, ns, nc])
}

fn hunk(n: Vec<u64>, lines: Vec<Value>) -> Value {
    json!({ "oldStart": n[0], "oldCount": n[1], "newStart": n[2], "newCount": n[3], "lines": lines })
}

fn untracked_file(root: &Path, rel: &str) -> Value {
    let path = root.join(rel);
    let base = |status: &str, adds: u64, hunks: Vec<Value>| json!({ "path": rel, "isNew": true, "isDeleted": false, "additions": adds, "deletions": 0, "hunks": hunks, "status": status });
    let Ok(meta) = std::fs::metadata(&path) else { return base("ok", 0, vec![]) };
    if meta.len() > UNTRACKED_MAX_BYTES {
        return base("too_large", 0, vec![]);
    }
    let Ok(bytes) = std::fs::read(&path) else { return base("ok", 0, vec![]) };
    if bytes[..bytes.len().min(8192)].contains(&0) {
        return base("binary", 0, vec![]);
    }
    let text = String::from_utf8_lossy(&bytes);
    let body: Vec<&str> = text.lines().collect();
    if body.len() > FILE_MAX_LINES {
        return base("too_large", body.len() as u64, vec![]);
    }
    let n = body.len() as u64;
    let mut lines = vec![json!({ "type": "header", "content": format!("@@ -0,0 +1,{n} @@") })];
    lines.extend(body.iter().map(|l| json!({ "type": "add", "content": l })));
    base("ok", n, vec![hunk(vec![0, 0, 1, n], lines)])
}

/// `(files, error, diffTooLarge)` for a compare spec.
pub async fn diff(cwd: &str, compare: &Value) -> (Vec<Value>, Value, bool) {
    let dir = expand(cwd);
    let Some(root) = git_ok(&dir, &["rev-parse", "--show-toplevel"]).await.map(PathBuf::from) else {
        return (vec![], checkout_error("Not a git repository", "NOT_GIT_REPO"), false);
    };
    let mode = compare.get("mode").and_then(|m| m.as_str()).unwrap_or("uncommitted");
    let ignore_ws = compare.get("ignoreWhitespace").and_then(|v| v.as_bool()).unwrap_or(false);
    let (base, target, untracked) = if mode == "base" {
        let requested = compare.get("baseRef").and_then(|v| v.as_str()).map(String::from);
        let Some(name) = (match requested {
            Some(r) => Some(r),
            None => default_branch(&root).await,
        }) else {
            return (vec![], Value::Null, false);
        };
        let best = best_base_ref(&root, &name).await;
        let merge_base = git_ok(&root, &["merge-base", &best, "HEAD"]).await.unwrap_or(best);
        (merge_base, Some("HEAD".to_string()), false)
    } else {
        let head = if git(&root, &["rev-parse", "--verify", "--quiet", "HEAD"]).await.is_ok() { "HEAD" } else { EMPTY_TREE };
        (head.to_string(), None, true)
    };
    let mut args = vec!["-c", "core.quotepath=off", "diff", "--no-color", "--no-ext-diff", "-M"];
    if ignore_ws {
        args.push("-w");
    }
    args.push(&base);
    if let Some(t) = &target {
        args.push(t);
    }
    let text = match git(&root, &args).await {
        Ok(t) => t,
        Err(e) => return (vec![], checkout_error(e, "UNKNOWN"), false),
    };
    if text.len() > TOTAL_DIFF_MAX_BYTES {
        return (vec![], Value::Null, true);
    }
    let mut files = parse_diff(&text);
    if untracked {
        if let Ok(list) = git(&root, &["ls-files", "--others", "--exclude-standard", "-z"]).await {
            for rel in list.split('\0').filter(|p| !p.is_empty()).take(UNTRACKED_MAX_FILES) {
                files.push(untracked_file(&root, rel));
            }
        }
    }
    files.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
    (files, Value::Null, false)
}

struct Sub {
    cwd: String,
    compare: Value,
    out: Out,
    last: u64,
}

fn fingerprint(files: &[Value], error: &Value, too_large: bool) -> u64 {
    let mut h = DefaultHasher::new();
    serde_json::to_string(files).unwrap_or_default().hash(&mut h);
    error.to_string().hash(&mut h);
    too_large.hash(&mut h);
    h.finish()
}

/// Live diff subscriptions, shared between the manager (subscribe/unsubscribe,
/// poke on turn end) and one background refresh task.
#[derive(Clone)]
pub struct Watcher {
    subs: Arc<Mutex<HashMap<String, Sub>>>,
    wake: Arc<Notify>,
}

impl Watcher {
    pub fn start() -> Watcher {
        let w = Watcher { subs: Arc::new(Mutex::new(HashMap::new())), wake: Arc::new(Notify::new()) };
        let bg = w.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(REFRESH_EVERY) => {}
                    _ = bg.wake.notified() => {
                        // Let the agent's last writes land before diffing.
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
                bg.refresh().await;
            }
        });
        w
    }

    pub fn subscribe(&self, id: String, cwd: String, compare: Value, out: Out, last: u64) {
        self.subs.lock().unwrap().insert(id, Sub { cwd, compare, out, last });
    }

    pub fn unsubscribe(&self, id: &str) {
        self.subs.lock().unwrap().remove(id);
    }

    /// Something may have changed on disk (an agent turn ended).
    pub fn poke(&self) {
        if !self.subs.lock().unwrap().is_empty() {
            self.wake.notify_one();
        }
    }

    async fn refresh(&self) {
        // Group by (cwd, compare) so N subscribers to one checkout cost one diff.
        let targets: Vec<(String, Value)> = {
            let mut subs = self.subs.lock().unwrap();
            subs.retain(|_, s| !s.out.is_closed());
            let mut seen: Vec<(String, Value)> = Vec::new();
            for s in subs.values() {
                if !seen.iter().any(|(c, k)| *c == s.cwd && *k == s.compare) {
                    seen.push((s.cwd.clone(), s.compare.clone()));
                }
            }
            seen
        };
        for (cwd, compare) in targets {
            let (files, error, too_large) = diff(&cwd, &compare).await;
            let fp = fingerprint(&files, &error, too_large);
            let mut subs = self.subs.lock().unwrap();
            for (id, sub) in subs.iter_mut().filter(|(_, s)| s.cwd == cwd && s.compare == compare && s.last != fp) {
                sub.last = fp;
                let _ = sub.out.send(wrap(json!({ "type": "checkout_diff_update", "payload": {
                    "subscriptionId": id, "cwd": cwd, "files": files, "error": error, "diffTooLarge": too_large,
                } })));
            }
        }
    }
}

/// Subscribe: compute the first diff, reply, then register for live updates.
pub async fn subscribe(watcher: Watcher, out: Out, request_id: String, subscription_id: String, cwd: String, compare: Value) {
    let (files, error, too_large) = diff(&cwd, &compare).await;
    let fp = fingerprint(&files, &error, too_large);
    let _ = out.send(wrap(json!({ "type": "subscribe_checkout_diff_response", "payload": {
        "requestId": request_id, "subscriptionId": subscription_id, "cwd": cwd,
        "files": files, "error": error, "diffTooLarge": too_large,
    } })));
    watcher.subscribe(subscription_id, cwd, compare, out, fp);
}

/// `checkout.diff.get.response` payload.
pub async fn get(cwd: &str, compare: &Value, request_id: &str) -> Value {
    let (files, error, too_large) = diff(cwd, compare).await;
    json!({ "requestId": request_id, "cwd": cwd, "files": files, "error": error, "diffTooLarge": too_large })
}

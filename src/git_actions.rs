//! Git actions from the app's Changes panel (commit, push, pull, branches,
//! stash, merge, commit history, discard, pull requests via `gh`), so routine
//! git work doesn't cost an agent turn. Shapes follow upstream's
//! checkout-session.ts. Every request names its directory in `cwd`; work runs
//! in a background task, and mutations refresh the diff subscriptions and push
//! a fresh `checkout_status_update`.

use crate::checkout::{best_base_ref, checkout_error, default_branch, expand, git, git_ok, parse_diff, status, Watcher};
use crate::manager::{reply, wrap, Out};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

const LONG: Duration = Duration::from_secs(120);

/// What a git action needs from the manager, cheap to move into a task.
#[derive(Clone)]
pub struct GitCtx {
    pub watcher: Watcher,
    pub bcast: broadcast::Sender<Arc<str>>,
}

fn s(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(|x| x.to_string())
}

/// Git with network or long work (push, pull, merge): never prompts, 120s cap.
async fn git_long(dir: &Path, args: &[&str]) -> Result<String, String> {
    let mut cmd = tokio::process::Command::new("git");
    cmd.args(args)
        .current_dir(dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GH_PROMPT_DISABLED", "1")
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    output(cmd, "git").await
}

async fn gh(dir: &Path, args: &[&str]) -> Result<String, String> {
    let mut cmd = tokio::process::Command::new("gh");
    cmd.args(args).current_dir(dir).env("GH_PROMPT_DISABLED", "1").env("NO_COLOR", "1").stdin(std::process::Stdio::null()).kill_on_drop(true);
    output(cmd, "gh").await
}

async fn output(mut cmd: tokio::process::Command, program: &str) -> Result<String, String> {
    let out = tokio::time::timeout(LONG, cmd.output()).await.map_err(|_| format!("{program} timed out"))?.map_err(|e| format!("failed to run {program}: {e}"))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
        let err = if err.is_empty() { String::from_utf8_lossy(&out.stdout).trim().to_string() } else { err };
        Err(if err.is_empty() { format!("{program} exited with {}", out.status) } else { crate::util::truncate(&err, 800) })
    }
}

/// Upstream's `requireGitRepo`: NOT_GIT_REPO when `cwd` isn't inside a repo.
async fn require_repo(dir: &Path, cwd: &str) -> Result<(), Value> {
    if dir.is_dir() && git(dir, &["rev-parse", "--git-dir"]).await.is_ok() {
        Ok(())
    } else {
        Err(checkout_error(format!("Not a git repository: {cwd}"), "NOT_GIT_REPO"))
    }
}

fn unknown(message: impl Into<String>) -> Value {
    checkout_error(message, "UNKNOWN")
}

async fn current_branch(dir: &Path) -> Option<String> {
    git_ok(dir, &["rev-parse", "--abbrev-ref", "HEAD"]).await.filter(|b| b != "HEAD")
}

/// Upstream's `assertSafeGitRef`.
fn safe_ref(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || "._/-".contains(c))
        && !name.contains("..")
        && !name.starts_with('-')
        && !name.starts_with('/')
        && !name.ends_with('/')
        && !name.ends_with(".lock")
}

fn normalize_branch(name: &str) -> &str {
    let n = name.strip_prefix("refs/heads/").or_else(|| name.strip_prefix("refs/remotes/")).unwrap_or(name);
    n.strip_prefix("origin/").unwrap_or(n)
}

/// Where a branch name lives: local, remote-only (origin), or nowhere.
async fn resolve_branch(dir: &Path, name: &str) -> Option<(String, bool)> {
    let n = normalize_branch(name);
    if git(dir, &["rev-parse", "--verify", "--quiet", &format!("refs/heads/{n}")]).await.is_ok() {
        return Some((n.to_string(), false));
    }
    if git(dir, &["rev-parse", "--verify", "--quiet", &format!("refs/remotes/origin/{n}")]).await.is_ok() {
        return Some((n.to_string(), true));
    }
    None
}

impl GitCtx {
    /// After a mutation: recompute diff subscriptions and push the new status.
    async fn after_mutation(&self, cwd: &str) {
        self.watcher.poke();
        let mut payload = status(cwd, &format!("subscription:{cwd}")).await;
        if let Some(obj) = payload.as_object_mut() {
            obj.remove("subscriptionId");
        }
        let _ = self.bcast.send(wrap(json!({ "type": "checkout_status_update", "payload": payload })));
    }
}

/// `{ cwd, success, error, requestId }`, the shape most actions answer with.
fn simple(cwd: &str, rid: &str, result: Result<(), Value>) -> Value {
    match result {
        Ok(()) => json!({ "cwd": cwd, "success": true, "error": null, "requestId": rid }),
        Err(e) => json!({ "cwd": cwd, "success": false, "error": e, "requestId": rid }),
    }
}

fn dir_of(msg: &Value) -> (String, PathBuf) {
    let cwd = s(msg, "cwd").unwrap_or_default();
    let dir = expand(&cwd);
    (cwd, dir)
}

/// Handle a git action request. Returns false when `kind` isn't one.
pub fn handle(ctx: &GitCtx, kind: &str, msg: &Value, rid: &str, out: &Out) -> bool {
    let response = match kind {
        "checkout_commit_request" => "checkout_commit_response",
        "checkout_push_request" => "checkout_push_response",
        "checkout_pull_request" => "checkout_pull_response",
        "checkout_switch_branch_request" => "checkout_switch_branch_response",
        "branch_suggestions_request" => "branch_suggestions_response",
        "validate_branch_request" => "validate_branch_response",
        "checkout.rename_branch.request" => "checkout.rename_branch.response",
        "checkout.discard_changes.request" => "checkout.discard_changes.response",
        "checkout.refresh.request" => "checkout.refresh.response",
        "checkout.commits.list.request" => "checkout.commits.list.response",
        "checkout.commits.file_diff.request" => "checkout.commits.file_diff.response",
        "stash_save_request" => "stash_save_response",
        "stash_pop_request" => "stash_pop_response",
        "stash_list_request" => "stash_list_response",
        "checkout_merge_request" => "checkout_merge_response",
        "checkout_merge_from_base_request" => "checkout_merge_from_base_response",
        "checkout_pr_status_request" => "checkout_pr_status_response",
        "checkout_pr_create_request" => "checkout_pr_create_response",
        "checkout_pr_merge_request" => "checkout_pr_merge_response",
        _ => return false,
    };
    let (ctx, kind, msg, rid, out) = (ctx.clone(), kind.to_string(), msg.clone(), rid.to_string(), out.clone());
    tokio::spawn(async move {
        let payload = run(&ctx, &kind, &msg, &rid).await;
        reply(&out, json!({ "type": response, "payload": payload }));
    });
    true
}

async fn run(ctx: &GitCtx, kind: &str, msg: &Value, rid: &str) -> Value {
    let (cwd, dir) = dir_of(msg);
    match kind {
        "checkout_commit_request" => {
            let r = commit(&dir, &cwd, s(msg, "message").unwrap_or_default(), msg.get("addAll").and_then(|v| v.as_bool()).unwrap_or(true)).await;
            mutated(ctx, &cwd, simple(&cwd, rid, r)).await
        }
        "checkout_push_request" => mutated(ctx, &cwd, simple(&cwd, rid, push(&dir, &cwd).await)).await,
        "checkout_pull_request" => mutated(ctx, &cwd, simple(&cwd, rid, pull(&dir, &cwd).await)).await,
        "checkout_switch_branch_request" => {
            let branch = s(msg, "branch").unwrap_or_default();
            let payload = match switch_branch(&dir, &cwd, &branch).await {
                Ok(source) => json!({ "cwd": cwd, "success": true, "branch": branch, "source": source, "error": null, "requestId": rid }),
                Err(e) => json!({ "cwd": cwd, "success": false, "branch": branch, "error": e, "requestId": rid }),
            };
            mutated(ctx, &cwd, payload).await
        }
        "branch_suggestions_request" => branch_suggestions(&dir, &cwd, msg, rid).await,
        "validate_branch_request" => validate_branch(&dir, &cwd, &s(msg, "branchName").unwrap_or_default(), rid).await,
        "checkout.rename_branch.request" => {
            let payload = match rename_branch(&dir, &cwd, &s(msg, "branch").unwrap_or_default()).await {
                Ok(b) => json!({ "requestId": rid, "success": true, "cwd": cwd, "currentBranch": b, "error": null }),
                Err(e) => json!({ "requestId": rid, "success": false, "cwd": cwd, "currentBranch": null, "error": e }),
            };
            mutated(ctx, &cwd, payload).await
        }
        "checkout.discard_changes.request" => {
            let paths: Vec<String> = msg.get("paths").and_then(|p| p.as_array()).map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect()).unwrap_or_default();
            mutated(ctx, &cwd, simple(&cwd, rid, discard(&dir, &cwd, &paths).await)).await
        }
        "checkout.refresh.request" => {
            let r = match require_repo(&dir, &cwd).await {
                // Best effort: offline or no remote still refreshes local state.
                Ok(()) => {
                    let _ = git_long(&dir, &["fetch", "--quiet", "--prune"]).await;
                    Ok(())
                }
                Err(e) => Err(e),
            };
            mutated(ctx, &cwd, simple(&cwd, rid, r)).await
        }
        "checkout.commits.list.request" => commits_list(&dir, &cwd, rid).await,
        "checkout.commits.file_diff.request" => commit_file_diff(&dir, &cwd, msg, rid).await,
        "stash_save_request" => {
            let label = s(msg, "branch").map(|b| b.trim().to_string()).filter(|b| !b.is_empty()).unwrap_or_else(|| "unnamed".into());
            let r = match require_repo(&dir, &cwd).await {
                Ok(()) => git_long(&dir, &["stash", "push", "--include-untracked", "-m", &format!("paseo-auto-stash: {label}")]).await.map(|_| ()).map_err(unknown),
                Err(e) => Err(e),
            };
            mutated(ctx, &cwd, simple(&cwd, rid, r)).await
        }
        "stash_pop_request" => {
            let index = msg.get("stashIndex").and_then(|v| v.as_u64()).unwrap_or(0);
            let r = match require_repo(&dir, &cwd).await {
                Ok(()) => git_long(&dir, &["stash", "pop", &format!("stash@{{{index}}}")]).await.map(|_| ()).map_err(unknown),
                Err(e) => Err(e),
            };
            mutated(ctx, &cwd, simple(&cwd, rid, r)).await
        }
        "stash_list_request" => stash_list(&dir, &cwd, msg.get("paseoOnly").and_then(|v| v.as_bool()).unwrap_or(true), rid).await,
        "checkout_merge_request" => {
            let r = merge_to_base(&dir, &cwd, msg).await;
            mutated(ctx, &cwd, simple(&cwd, rid, r)).await
        }
        "checkout_merge_from_base_request" => {
            let r = merge_from_base(&dir, &cwd, msg).await;
            mutated(ctx, &cwd, simple(&cwd, rid, r)).await
        }
        "checkout_pr_status_request" => pr_status(&dir, &cwd, rid).await,
        "checkout_pr_create_request" => {
            let payload = pr_create(&dir, &cwd, msg, rid).await;
            mutated(ctx, &cwd, payload).await
        }
        "checkout_pr_merge_request" => {
            let r = pr_merge(&dir, &cwd, &s(msg, "mergeMethod").unwrap_or_else(|| "merge".into())).await;
            mutated(ctx, &cwd, simple(&cwd, rid, r)).await
        }
        _ => Value::Null,
    }
}

/// Pass the response through after refreshing diffs and status.
async fn mutated(ctx: &GitCtx, cwd: &str, payload: Value) -> Value {
    ctx.after_mutation(cwd).await;
    payload
}

/// Upstream asks a model for a message when none is given; paseo-lite writes
/// a plain one from the staged changes instead of spending a model call.
async fn auto_commit_message(dir: &Path) -> String {
    let names = git(dir, &["diff", "--cached", "--name-only"]).await.unwrap_or_default();
    let files: Vec<&str> = names.lines().filter(|l| !l.trim().is_empty()).collect();
    match files.len() {
        0 => "Update".to_string(),
        1 => format!("Update {}", files[0]),
        n if n <= 3 => format!("Update {}", files.join(", ")),
        n => format!("Update {} files ({}, ...)", n, files[..2].join(", ")),
    }
}

async fn commit(dir: &Path, cwd: &str, message: String, add_all: bool) -> Result<(), Value> {
    require_repo(dir, cwd).await?;
    if add_all {
        git(dir, &["add", "-A"]).await.map_err(unknown)?;
    }
    let message = match message.trim() {
        "" => auto_commit_message(dir).await,
        m => m.to_string(),
    };
    git_long(dir, &["commit", "-m", &message]).await.map(|_| ()).map_err(unknown)
}

async fn push(dir: &Path, cwd: &str) -> Result<(), Value> {
    require_repo(dir, cwd).await?;
    let branch = current_branch(dir).await.ok_or_else(|| unknown("Unable to determine current branch for push"))?;
    // An existing upstream is respected; otherwise publish to origin and track it.
    let remote = git_ok(dir, &["config", "--get", &format!("branch.{branch}.remote")]).await;
    let merge = git_ok(dir, &["config", "--get", &format!("branch.{branch}.merge")]).await;
    if let (Some(remote), Some(merge)) = (remote, merge) {
        if let Some(target) = merge.strip_prefix("refs/heads/") {
            if git_ok(dir, &["remote", "get-url", &remote]).await.is_some() {
                return git_long(dir, &["push", "-u", &remote, &format!("HEAD:refs/heads/{target}")]).await.map(|_| ()).map_err(unknown);
            }
        }
    }
    if git_ok(dir, &["remote", "get-url", "origin"]).await.is_none() {
        return Err(unknown("Remote 'origin' is not configured."));
    }
    git_long(dir, &["push", "-u", "origin", &branch]).await.map(|_| ()).map_err(unknown)
}

async fn pull(dir: &Path, cwd: &str) -> Result<(), Value> {
    require_repo(dir, cwd).await?;
    current_branch(dir).await.ok_or_else(|| unknown("Unable to determine current branch for pull"))?;
    if git_ok(dir, &["remote", "get-url", "origin"]).await.is_none() {
        return Err(unknown("Remote 'origin' is not configured."));
    }
    match git_long(dir, &["pull"]).await {
        Ok(_) => Ok(()),
        Err(e) => {
            // Never leave the checkout mid-merge or mid-rebase.
            let _ = git(dir, &["merge", "--abort"]).await;
            let _ = git(dir, &["rebase", "--abort"]).await;
            Err(unknown(e))
        }
    }
}

async fn switch_branch(dir: &Path, cwd: &str, branch: &str) -> Result<&'static str, Value> {
    require_repo(dir, cwd).await?;
    if !safe_ref(branch) {
        return Err(unknown(format!("Invalid branch: {branch}")));
    }
    let (name, remote_only) = resolve_branch(dir, branch).await.ok_or_else(|| unknown(format!("Branch not found: {branch}")))?;
    if git(dir, &["status", "--porcelain"]).await.map(|s| !s.trim().is_empty()).unwrap_or(false) {
        return Err(unknown("Working directory has uncommitted changes. Commit or stash before switching branches."));
    }
    if remote_only {
        git(dir, &["checkout", "-b", &name, "--track", &format!("origin/{name}")]).await.map_err(unknown)?;
        return Ok("remote");
    }
    if current_branch(dir).await.as_deref() != Some(name.as_str()) {
        git(dir, &["checkout", &name]).await.map_err(unknown)?;
    }
    Ok("local")
}

/// Upstream's `validateBranchSlug` for renames.
fn branch_slug_error(name: &str) -> Option<&'static str> {
    if name.is_empty() {
        Some("Branch name is required")
    } else if name.len() > 100 {
        Some("Branch name must be 100 characters or fewer")
    } else if !name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '/') {
        Some("Branch name may only contain lowercase letters, numbers, hyphens and slashes")
    } else if name.starts_with('-') || name.ends_with('-') || name.contains("--") {
        Some("Branch name cannot start or end with a hyphen or contain consecutive hyphens")
    } else {
        None
    }
}

async fn rename_branch(dir: &Path, cwd: &str, branch: &str) -> Result<Option<String>, Value> {
    if let Some(e) = branch_slug_error(branch) {
        return Err(unknown(e));
    }
    require_repo(dir, cwd).await?;
    current_branch(dir).await.ok_or_else(|| unknown("Cannot rename branch in detached HEAD state"))?;
    git(dir, &["branch", "-m", branch]).await.map_err(unknown)?;
    Ok(current_branch(dir).await)
}

async fn discard(dir: &Path, cwd: &str, paths: &[String]) -> Result<(), Value> {
    require_repo(dir, cwd).await?;
    if paths.is_empty() {
        return Err(unknown("No paths to discard"));
    }
    let with = |head: &[&str]| -> Vec<String> { head.iter().map(|s| s.to_string()).chain(std::iter::once("--".into())).chain(paths.iter().cloned()).collect() };
    let reset = with(&["--literal-pathspecs", "reset", "-q", "HEAD"]);
    if git(dir, &reset.iter().map(String::as_str).collect::<Vec<_>>()).await.is_err() {
        // Unborn HEAD: just unstage.
        let rm = with(&["--literal-pathspecs", "rm", "--cached", "-r", "-q", "--ignore-unmatch"]);
        let _ = git(dir, &rm.iter().map(String::as_str).collect::<Vec<_>>()).await;
    }
    let st = with(&["--literal-pathspecs", "status", "--porcelain=v1", "-z"]);
    let out = git(dir, &st.iter().map(String::as_str).collect::<Vec<_>>()).await.map_err(unknown)?;
    let (mut untracked, mut tracked) = (Vec::new(), Vec::new());
    let mut tokens = out.split('\0').filter(|t| !t.is_empty());
    while let Some(t) = tokens.next() {
        if t.len() < 4 {
            continue;
        }
        let (code, path) = t.split_at(3);
        if code.starts_with("??") {
            untracked.push(path.to_string());
        } else {
            tracked.push(path.to_string());
            // Renames/copies carry the original path as the next token.
            if code.starts_with('R') || code.starts_with('C') {
                tokens.next();
            }
        }
    }
    if !untracked.is_empty() {
        let mut a = vec!["--literal-pathspecs", "clean", "-fd", "-q", "--"];
        a.extend(untracked.iter().map(String::as_str));
        git(dir, &a).await.map_err(unknown)?;
    }
    if !tracked.is_empty() {
        let mut a = vec!["--literal-pathspecs", "checkout", "-q", "--"];
        a.extend(tracked.iter().map(String::as_str));
        git(dir, &a).await.map_err(unknown)?;
    }
    Ok(())
}

async fn branch_suggestions(dir: &Path, cwd: &str, msg: &Value, rid: &str) -> Value {
    if let Err(e) = require_repo(dir, cwd).await {
        return json!({ "branches": [], "branchDetails": [], "error": e["message"], "requestId": rid });
    }
    let query = s(msg, "query").unwrap_or_default().trim().to_ascii_lowercase();
    let query = normalize_branch(&query).to_string();
    let limit = msg.get("limit").and_then(|v| v.as_u64()).unwrap_or(50).clamp(1, 200) as usize;
    // name -> (committerDate, local oid, remote oid)
    let mut by_name: std::collections::BTreeMap<String, (i64, Option<String>, Option<String>)> = Default::default();
    for (prefix, remote) in [("refs/heads", false), ("refs/remotes/origin", true)] {
        let out = git(dir, &["for-each-ref", "--sort=-committerdate", "--format=%(refname)%09%(committerdate:unix)%09%(objectname)", prefix]).await.unwrap_or_default();
        for line in out.lines() {
            let mut parts = line.split('\t');
            let (Some(refname), Some(date), Some(oid)) = (parts.next(), parts.next(), parts.next()) else { continue };
            let name = normalize_branch(refname).to_string();
            if name.is_empty() || name == "HEAD" || name == "origin" {
                continue;
            }
            let entry = by_name.entry(name).or_insert((0, None, None));
            entry.0 = entry.0.max(date.parse().unwrap_or(0));
            if remote { entry.2 = Some(oid.to_string()) } else { entry.1 = Some(oid.to_string()) }
        }
    }
    let mut rows: Vec<(String, (i64, Option<String>, Option<String>))> = by_name.into_iter().filter(|(n, _)| n.to_ascii_lowercase().contains(&query)).collect();
    rows.sort_by(|a, b| {
        let pa = !query.is_empty() && a.0.to_ascii_lowercase().starts_with(&query);
        let pb = !query.is_empty() && b.0.to_ascii_lowercase().starts_with(&query);
        pb.cmp(&pa).then(b.1 .0.cmp(&a.1 .0)).then(a.0.cmp(&b.0))
    });
    rows.truncate(limit);
    let mut details = Vec::new();
    for (name, (date, local, remote)) in &rows {
        let mut d = json!({ "name": name, "committerDate": date, "hasLocal": local.is_some(), "hasRemote": remote.is_some() });
        if let (Some(l), Some(r)) = (local, remote) {
            let (ahead, behind) = if l == r {
                (0, 0)
            } else {
                let counts = git_ok(dir, &["rev-list", "--left-right", "--count", &format!("refs/heads/{name}...refs/remotes/origin/{name}")]).await.unwrap_or_default();
                let v: Vec<u64> = counts.split_whitespace().filter_map(|n| n.parse().ok()).collect();
                (v.first().copied().unwrap_or(0), v.get(1).copied().unwrap_or(0))
            };
            d["localAhead"] = json!(ahead);
            d["localBehind"] = json!(behind);
        }
        details.push(d);
    }
    let branches: Vec<&str> = rows.iter().map(|(n, _)| n.as_str()).collect();
    json!({ "branches": branches, "branchDetails": details, "error": null, "requestId": rid })
}

async fn validate_branch(dir: &Path, cwd: &str, name: &str, rid: &str) -> Value {
    let fail = |e: String| json!({ "exists": false, "resolvedRef": null, "isRemote": false, "error": e, "requestId": rid });
    if !safe_ref(name) {
        return fail(format!("Invalid branch: {name}"));
    }
    if let Err(e) = require_repo(dir, cwd).await {
        return fail(e["message"].as_str().unwrap_or("Not a git repository").to_string());
    }
    match resolve_branch(dir, name).await {
        Some((n, false)) => json!({ "exists": true, "resolvedRef": n, "isRemote": false, "error": null, "requestId": rid }),
        Some((n, true)) => json!({ "exists": true, "resolvedRef": format!("origin/{n}"), "isRemote": true, "error": null, "requestId": rid }),
        None => json!({ "exists": false, "resolvedRef": null, "isRemote": false, "error": null, "requestId": rid }),
    }
}

/// Compare against whichever of `<base>` / `origin/<base>` is further ahead.
async fn comparison_ref(dir: &Path, base: &str) -> Option<String> {
    let local = git(dir, &["rev-parse", "--verify", "--quiet", &format!("refs/heads/{base}")]).await.is_ok();
    let origin = git(dir, &["rev-parse", "--verify", "--quiet", &format!("refs/remotes/origin/{base}")]).await.is_ok();
    match (local, origin) {
        (false, false) => None,
        (true, false) => Some(base.to_string()),
        (false, true) => Some(format!("origin/{base}")),
        (true, true) => {
            let counts = git_ok(dir, &["rev-list", "--left-right", "--count", &format!("{base}...origin/{base}")]).await.unwrap_or_default();
            let v: Vec<u64> = counts.split_whitespace().filter_map(|n| n.parse().ok()).collect();
            let (local_only, origin_only) = (v.first().copied().unwrap_or(0), v.get(1).copied().unwrap_or(0));
            Some(if origin_only > local_only { format!("origin/{base}") } else { base.to_string() })
        }
    }
}

/// Parse `git log --format=%x1e%H%x00%h%x00%an%x00%aI%x00%s --raw --numstat -M`.
fn parse_log(text: &str) -> Vec<(String, String, String, String, String, Vec<Value>)> {
    let mut commits = Vec::new();
    for record in text.split('\u{1e}').filter(|r| !r.trim().is_empty()) {
        let mut lines = record.lines();
        let header = lines.next().unwrap_or("");
        let f: Vec<&str> = header.split('\0').collect();
        if f.len() < 5 {
            continue;
        }
        let mut statuses: std::collections::HashMap<String, &str> = Default::default();
        let mut files: Vec<Value> = Vec::new();
        for line in lines {
            if let Some(raw) = line.strip_prefix(':') {
                // ":100644 100644 abc def M\tpath" (renames: "R100\told\tnew")
                let mut parts = raw.split('\t');
                let meta = parts.next().unwrap_or("");
                let letter = meta.split_whitespace().last().unwrap_or("").chars().next().unwrap_or('M');
                let path = parts.last().unwrap_or("").to_string();
                let st = match letter {
                    'A' | 'C' => "added",
                    'D' => "deleted",
                    'R' => "renamed",
                    _ => "modified",
                };
                statuses.insert(path, st);
            } else if !line.trim().is_empty() {
                let mut parts = line.splitn(3, '\t');
                let (Some(a), Some(d), Some(p)) = (parts.next(), parts.next(), parts.next()) else { continue };
                // Rename paths come as "old => new" or "dir/{a => b}/f"; keep the destination.
                let path = if let (Some(open), Some(close)) = (p.find('{'), p.find('}')) {
                    let inner = &p[open + 1..close];
                    let new = inner.split(" => ").last().unwrap_or(inner);
                    format!("{}{}{}", &p[..open], new, &p[close + 1..]).replace("//", "/")
                } else {
                    p.split(" => ").last().unwrap_or(p).to_string()
                };
                files.push(json!({ "path": path, "additions": a.parse::<u64>().unwrap_or(0), "deletions": d.parse::<u64>().unwrap_or(0) }));
            }
        }
        for file in files.iter_mut() {
            if let Some(st) = file["path"].as_str().and_then(|p| statuses.get(p)) {
                file["status"] = json!(st);
            }
        }
        commits.push((f[0].trim().to_string(), f[1].to_string(), f[2].to_string(), f[3].to_string(), f[4].to_string(), files));
    }
    commits
}

async fn commits_list(dir: &Path, cwd: &str, rid: &str) -> Value {
    let fail = |e: Value| json!({ "cwd": cwd, "baseRef": null, "commits": [], "error": e, "requestId": rid });
    if let Err(e) = require_repo(dir, cwd).await {
        return fail(e);
    }
    let Some(branch) = current_branch(dir).await else {
        return json!({ "cwd": cwd, "baseRef": null, "commits": [], "error": null, "requestId": rid });
    };
    let base = default_branch(dir).await;
    let cmp = match &base {
        Some(b) if b != &branch => comparison_ref(dir, b).await,
        _ => None,
    };
    const FORMAT: &str = "--format=%x1e%H%x00%h%x00%an%x00%aI%x00%s";
    let mut workspace = Vec::new();
    if let Some(c) = &cmp {
        let out = git(dir, &["log", &format!("{c}..HEAD"), "--diff-merges=first-parent", FORMAT, "--raw", "--numstat", "-M"]).await.map_err(unknown);
        match out {
            Ok(t) => workspace = parse_log(&t),
            Err(e) => return fail(e),
        }
    }
    let from = match &cmp {
        Some(c) => git_ok(dir, &["merge-base", c, "HEAD"]).await.unwrap_or_else(|| "HEAD".into()),
        None => "HEAD".into(),
    };
    let base_commits = git(dir, &["log", &from, "--max-count=10", "--diff-merges=first-parent", FORMAT, "--raw", "--numstat", "-M"]).await.map(|t| parse_log(&t)).unwrap_or_default();
    let unpushed: std::collections::HashSet<String> = git(dir, &["rev-list", "HEAD", "--not", "--remotes"]).await.unwrap_or_default().lines().map(|l| l.trim().to_string()).collect();
    let workspace_shas: std::collections::HashSet<String> = workspace.iter().map(|c| c.0.clone()).collect();
    let mut seen = std::collections::HashSet::new();
    let commits: Vec<Value> = workspace
        .into_iter()
        .chain(base_commits)
        .filter(|c| seen.insert(c.0.clone()))
        .map(|(sha, short, author, date, subject, files)| {
            json!({
                "sha": sha, "shortSha": short, "subject": subject, "authorName": author, "authorDate": date,
                "isOnRemote": !unpushed.contains(&sha), "isOnBase": !workspace_shas.contains(&sha), "files": files,
            })
        })
        .collect();
    json!({ "cwd": cwd, "baseRef": cmp, "commits": commits, "error": null, "requestId": rid })
}

async fn commit_file_diff(dir: &Path, cwd: &str, msg: &Value, rid: &str) -> Value {
    let sha = s(msg, "sha").unwrap_or_default();
    let path = s(msg, "path").unwrap_or_default();
    let base = |file: Value, error: Value| json!({ "cwd": cwd, "sha": sha, "path": path, "file": file, "error": error, "requestId": rid });
    if let Err(e) = require_repo(dir, cwd).await {
        return base(Value::Null, e);
    }
    if !safe_ref(&sha) {
        return base(Value::Null, unknown(format!("Invalid ref: {sha}")));
    }
    if path.is_empty() || path.starts_with('/') || path.split('/').any(|seg| seg == "..") {
        return base(Value::Null, unknown(format!("Invalid path: {path}")));
    }
    match git(dir, &["show", &sha, "--format=", "--diff-merges=first-parent", "--no-color", "--src-prefix=a/", "--dst-prefix=b/", "--", &path]).await {
        Ok(text) if text.trim().is_empty() => base(Value::Null, Value::Null),
        Ok(text) => {
            let file = parse_diff(&text).into_iter().find(|f| f["path"].as_str() == Some(path.as_str())).filter(|f| f["status"] != "binary").unwrap_or(Value::Null);
            base(file, Value::Null)
        }
        Err(e) => base(Value::Null, unknown(e)),
    }
}

async fn stash_list(dir: &Path, cwd: &str, paseo_only: bool, rid: &str) -> Value {
    if let Err(e) = require_repo(dir, cwd).await {
        return json!({ "cwd": cwd, "entries": [], "error": e, "requestId": rid });
    }
    let out = git(dir, &["stash", "list", "--format=%gd%x00%s"]).await.unwrap_or_default();
    let entries: Vec<Value> = out
        .lines()
        .filter_map(|line| {
            let (gd, subject) = line.split_once('\0')?;
            let index: u64 = gd.split_once('{')?.1.trim_end_matches('}').parse().ok()?;
            let marker = "paseo-auto-stash:";
            let branch = subject.find(marker).map(|i| subject[i + marker.len()..].trim().to_string()).filter(|b| !b.is_empty());
            let is_paseo = subject.contains(marker);
            (!paseo_only || is_paseo).then(|| json!({ "index": index, "message": subject, "branch": branch, "isPaseo": is_paseo }))
        })
        .collect();
    json!({ "cwd": cwd, "entries": entries, "error": null, "requestId": rid })
}

async fn has_conflicts(dir: &Path, error: &str) -> bool {
    error.contains("CONFLICT") || error.contains("Automatic merge failed") || git_ok(dir, &["diff", "--name-only", "--diff-filter=U"]).await.is_some()
}

/// Merge the current branch INTO the base branch (upstream's checkout_merge).
async fn merge_to_base(dir: &Path, cwd: &str, msg: &Value) -> Result<(), Value> {
    if require_repo(dir, cwd).await.is_err() {
        return Err(unknown(format!("Not a git repository: {cwd}")));
    }
    let dirty = git(dir, &["status", "--porcelain"]).await.map(|s| !s.trim().is_empty()).unwrap_or(false);
    if msg.get("requireCleanTarget").and_then(|v| v.as_bool()).unwrap_or(false) && dirty {
        return Err(unknown("Working directory has uncommitted changes."));
    }
    let current = current_branch(dir).await.ok_or_else(|| unknown("Unable to determine current branch for merge"))?;
    let base = match s(msg, "baseRef").filter(|b| !b.trim().is_empty()) {
        Some(b) => b,
        None => default_branch(dir).await.ok_or_else(|| unknown("Base branch is required for merge"))?,
    };
    let base = normalize_branch(&base).to_string();
    if !safe_ref(&base) {
        return Err(unknown(format!("Invalid branch: {base}")));
    }
    if base == current {
        return Ok(());
    }
    git(dir, &["checkout", &base]).await.map_err(unknown)?;
    let result = if s(msg, "strategy").as_deref() == Some("squash") {
        match git_long(dir, &["merge", "--squash", &current]).await {
            Ok(_) => git_long(dir, &["commit", "-m", &format!("Squash merge {current} into {base}")]).await.map(|_| ()),
            Err(e) => Err(e),
        }
    } else {
        git_long(dir, &["merge", &current]).await.map(|_| ())
    };
    let outcome = match result {
        Ok(()) => Ok(()),
        Err(e) if has_conflicts(dir, &e).await => {
            let _ = git(dir, &["merge", "--abort"]).await;
            let _ = git(dir, &["reset", "--merge"]).await;
            Err(checkout_error(format!("Merge conflict while merging {current} into {base}"), "MERGE_CONFLICT"))
        }
        Err(e) => Err(unknown(e)),
    };
    // Always return to the branch the user was on.
    let _ = git(dir, &["checkout", &current]).await;
    outcome
}

/// Merge the base branch INTO the current branch.
async fn merge_from_base(dir: &Path, cwd: &str, msg: &Value) -> Result<(), Value> {
    require_repo(dir, cwd).await?;
    if msg.get("requireCleanTarget").and_then(|v| v.as_bool()).unwrap_or(true) && git(dir, &["status", "--porcelain"]).await.map(|s| !s.trim().is_empty()).unwrap_or(false) {
        return Err(unknown("Working directory has uncommitted changes."));
    }
    let current = current_branch(dir).await.ok_or_else(|| unknown("Unable to determine current branch for merge"))?;
    let base = match s(msg, "baseRef").filter(|b| !b.trim().is_empty()) {
        Some(b) => normalize_branch(&b).to_string(),
        None => default_branch(dir).await.ok_or_else(|| unknown("Unable to determine base branch for merge"))?,
    };
    let best = comparison_ref(dir, &base).await.unwrap_or_else(|| base.clone());
    if best == current {
        return Ok(());
    }
    match git_long(dir, &["merge", &best]).await {
        Ok(_) => Ok(()),
        Err(e) if has_conflicts(dir, &e).await => {
            let _ = git(dir, &["merge", "--abort"]).await;
            Err(checkout_error(format!("Merge conflict while merging {best} into {current}. Please merge manually."), "MERGE_CONFLICT"))
        }
        Err(e) => Err(unknown(e)),
    }
}

fn gh_available() -> bool {
    crate::providers::binary_available("gh")
}

/// Pull request for the current branch, in upstream's CheckoutPrStatus shape.
async fn pr_status(dir: &Path, cwd: &str, rid: &str) -> Value {
    let empty = |auth: &str, error: Value| json!({ "cwd": cwd, "status": null, "githubFeaturesEnabled": auth == "authenticated", "authState": auth, "forge": "github", "error": error, "requestId": rid });
    if require_repo(dir, cwd).await.is_err() || git_ok(dir, &["remote", "get-url", "origin"]).await.is_none() {
        return empty("no_remote", Value::Null);
    }
    if !gh_available() {
        return empty("cli_missing", Value::Null);
    }
    let fields = "number,url,title,state,isDraft,baseRefName,headRefName,mergedAt,reviewDecision,mergeable,headRepositoryOwner,statusCheckRollup";
    let raw = match gh(dir, &["pr", "view", "--json", fields]).await {
        Ok(r) => r,
        // No PR for this branch is not an error.
        Err(e) if e.contains("no pull requests found") || e.contains("no open pull requests") => return empty("authenticated", Value::Null),
        Err(e) if e.to_ascii_lowercase().contains("auth") || e.contains("gh auth login") => return empty("unauthenticated", Value::Null),
        Err(e) => return empty("error", unknown(e)),
    };
    let Ok(pr) = serde_json::from_str::<Value>(&raw) else { return empty("error", unknown("could not parse gh output")) };
    let merged = pr["mergedAt"].as_str().map(|m| !m.is_empty()).unwrap_or(false);
    let state = if merged { "merged".to_string() } else { pr["state"].as_str().unwrap_or("open").to_ascii_lowercase() };
    let checks: Vec<Value> = pr["statusCheckRollup"]
        .as_array()
        .map(|list| {
            list.iter()
                .map(|c| {
                    let conclusion = c["conclusion"].as_str().or(c["state"].as_str()).unwrap_or("").to_ascii_uppercase();
                    let status = match (c["status"].as_str().unwrap_or("COMPLETED"), conclusion.as_str()) {
                        (s, _) if s != "COMPLETED" && !c["status"].is_null() => "pending",
                        (_, "SUCCESS") => "success",
                        (_, "SKIPPED") | (_, "NEUTRAL") => "skipped",
                        (_, "CANCELLED") => "cancelled",
                        (_, "PENDING") | (_, "EXPECTED") | (_, "") => "pending",
                        _ => "failure",
                    };
                    let mut check = json!({ "name": c["name"].as_str().or(c["context"].as_str()).unwrap_or("check"), "status": status, "url": c["detailsUrl"].as_str().or(c["targetUrl"].as_str()) });
                    if let Some(w) = c["workflowName"].as_str() {
                        check["workflow"] = json!(w);
                    }
                    check
                })
                .collect()
        })
        .unwrap_or_default();
    let checks_status = if checks.is_empty() {
        "none"
    } else if checks.iter().any(|c| c["status"] == "failure") {
        "failure"
    } else if checks.iter().any(|c| c["status"] == "pending") {
        "pending"
    } else {
        "success"
    };
    let review = pr["reviewDecision"].as_str().filter(|r| !r.is_empty()).map(|r| r.to_ascii_lowercase().replace("review_required", "pending"));
    let mergeable = match pr["mergeable"].as_str() {
        Some(m @ ("MERGEABLE" | "CONFLICTING")) => m,
        _ => "UNKNOWN",
    };
    let mut status = json!({
        "forge": "github",
        "url": pr["url"], "title": pr["title"], "state": state,
        "baseRefName": pr["baseRefName"], "headRefName": pr["headRefName"],
        "isMerged": merged, "isDraft": pr["isDraft"].as_bool().unwrap_or(false),
        "mergeable": mergeable, "checks": checks, "checksStatus": checks_status, "reviewDecision": review,
    });
    if let Some(n) = pr["number"].as_u64() {
        status["number"] = json!(n);
    }
    json!({ "cwd": cwd, "status": status, "githubFeaturesEnabled": true, "authState": "authenticated", "forge": "github", "error": null, "requestId": rid })
}

async fn pr_create(dir: &Path, cwd: &str, msg: &Value, rid: &str) -> Value {
    let fail = |e: Value| json!({ "cwd": cwd, "url": null, "number": null, "error": e, "requestId": rid });
    if let Err(e) = require_repo(dir, cwd).await {
        return fail(e);
    }
    if !gh_available() {
        return fail(unknown("The GitHub CLI (gh) is not installed"));
    }
    if git_ok(dir, &["remote", "get-url", "origin"]).await.is_none() {
        return fail(unknown(format!("No supported forge remote is configured for {cwd}")));
    }
    let Some(head) = current_branch(dir).await else { return fail(unknown("Unable to determine head branch for PR")) };
    let base = match s(msg, "baseRef").filter(|b| !b.trim().is_empty()) {
        Some(b) => normalize_branch(&b).to_string(),
        None => match default_branch(dir).await {
            Some(b) => b,
            None => return fail(unknown("Unable to determine base branch for PR")),
        },
    };
    // Upstream asks a model for missing text; use the commits instead (no model call).
    let mut title = s(msg, "title").unwrap_or_default().trim().to_string();
    let mut body = s(msg, "body").unwrap_or_default().trim().to_string();
    let cmp = best_base_ref(dir, &base).await;
    if title.is_empty() {
        title = git_ok(dir, &["log", "-1", "--format=%s", &format!("{cmp}..HEAD")]).await.unwrap_or_else(|| head.replace(['-', '_', '/'], " "));
    }
    if body.is_empty() {
        body = git(dir, &["log", "--format=- %s", &format!("{cmp}..HEAD")]).await.unwrap_or_default().trim().to_string();
    }
    if let Err(e) = git_long(dir, &["push", "-u", "origin", &head]).await {
        return fail(unknown(e));
    }
    let args = ["pr", "create", "--head", head.as_str(), "--base", base.as_str(), "--title", title.as_str(), "--body", body.as_str()];
    match gh(dir, &args).await {
        Ok(out) => {
            let url = out.lines().rev().find(|l| l.starts_with("https://")).map(|l| l.trim().to_string());
            let number = url.as_deref().and_then(|u| u.rsplit('/').next()).and_then(|n| n.parse::<u64>().ok());
            json!({ "cwd": cwd, "url": url, "number": number, "error": null, "requestId": rid })
        }
        Err(e) => fail(unknown(e)),
    }
}

async fn pr_merge(dir: &Path, cwd: &str, method: &str) -> Result<(), Value> {
    require_repo(dir, cwd).await?;
    if !gh_available() {
        return Err(unknown("The GitHub CLI (gh) is not installed"));
    }
    let flag = match method {
        "squash" => "--squash",
        "rebase" => "--rebase",
        _ => "--merge",
    };
    let number = gh(dir, &["pr", "view", "--json", "number", "--jq", ".number"]).await.map_err(|_| unknown("Unable to determine current change request number for merge"))?;
    gh(dir, &["pr", "merge", number.trim(), flag]).await.map(|_| ()).map_err(unknown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_refs() {
        assert!(safe_ref("feature/x-1.2"));
        assert!(!safe_ref(""));
        assert!(!safe_ref("a..b"));
        assert!(!safe_ref("-x"));
        assert!(!safe_ref("x y"));
        assert!(!safe_ref("x.lock"));
        assert!(!safe_ref("x;rm"));
    }

    #[test]
    fn branch_normalization() {
        assert_eq!(normalize_branch("refs/heads/main"), "main");
        assert_eq!(normalize_branch("refs/remotes/origin/dev"), "dev");
        assert_eq!(normalize_branch("origin/feat/a"), "feat/a");
        assert_eq!(normalize_branch("plain"), "plain");
    }

    #[test]
    fn slug_rules() {
        assert!(branch_slug_error("feat/new-thing").is_none());
        assert!(branch_slug_error("").is_some());
        assert!(branch_slug_error("Upper").is_some());
        assert!(branch_slug_error("-x").is_some());
        assert!(branch_slug_error("a--b").is_some());
    }

    #[test]
    fn parses_log_with_raw_and_numstat() {
        let text = "\u{1e}abc123\u{0}abc\u{0}Ann\u{0}2026-10-11T00:00:00+00:00\u{0}Add things\n\n:100644 100644 1111111 2222222 M\tsrc/a.rs\n:000000 100644 0000000 3333333 A\tsrc/b.rs\n:100644 100644 4444444 5555555 R090\told.rs\tnew.rs\n3\t1\tsrc/a.rs\n10\t0\tsrc/b.rs\n1\t1\t{old.rs => new.rs}\n";
        let commits = parse_log(text);
        assert_eq!(commits.len(), 1);
        let (sha, short, author, _date, subject, files) = &commits[0];
        assert_eq!((sha.as_str(), short.as_str(), author.as_str(), subject.as_str()), ("abc123", "abc", "Ann", "Add things"));
        assert_eq!(files.len(), 3);
        assert_eq!(files[0]["status"], "modified");
        assert_eq!(files[0]["additions"], 3);
        assert_eq!(files[1]["status"], "added");
        assert_eq!(files[2]["path"], "new.rs");
        assert_eq!(files[2]["status"], "renamed");
    }

    #[test]
    fn binary_numstat_counts_zero() {
        let text = "\u{1e}s\u{0}s\u{0}a\u{0}d\u{0}bin\n\n:100644 100644 1 2 M\timg.png\n-\t-\timg.png\n";
        let files = &parse_log(text)[0].5;
        assert_eq!(files[0]["additions"], 0);
    }
}

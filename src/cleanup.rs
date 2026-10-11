//! Deleting a removed project's local directory.
//!
//! The rule is "never destroy work that can't be recovered": a directory is
//! deleted when git shows everything in it already exists on a remote (or it
//! is a still-empty folder paseo-lite created). Anything else is kept and the
//! reason is reported back. `PASEO_DELETE_PROJECT_FILES=all` drops the git
//! check, `never` disables deletion. Whatever the policy, only directories
//! strictly inside the user's home are deleted, never one holding
//! paseo-lite's data or another known project.

use crate::checkout::git;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Policy {
    Safe,
    All,
    Never,
}

impl Policy {
    pub fn parse(v: Option<&str>) -> Policy {
        match v.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("all") => Policy::All,
            Some("never") | Some("off") | Some("0") | Some("false") => Policy::Never,
            _ => Policy::Safe,
        }
    }
}

/// The user's home directory: `$HOME`, else the password database (systemd
/// services often run without `HOME`).
fn home_dir() -> Option<PathBuf> {
    match std::env::var_os("HOME").filter(|h| !h.is_empty()) {
        Some(h) => Some(PathBuf::from(h)),
        None => passwd_home(),
    }
}

#[cfg(unix)]
fn passwd_home() -> Option<PathBuf> {
    // SAFETY: getpwuid returns a pointer into static storage (or null); the
    // string is copied out immediately and the daemon calls it from one thread.
    unsafe {
        let pw = libc::getpwuid(libc::getuid());
        if pw.is_null() || (*pw).pw_dir.is_null() {
            return None;
        }
        let dir = std::ffi::CStr::from_ptr((*pw).pw_dir).to_string_lossy().into_owned();
        (!dir.is_empty()).then(|| PathBuf::from(dir))
    }
}

#[cfg(not(unix))]
fn passwd_home() -> Option<PathBuf> {
    None
}

/// Why `path` must never be deleted, if it mustn't. `others` are every other
/// directory paseo-lite knows about (projects and agent cwds).
fn protected(path: &Path, daemon_home: &Path, others: &[String]) -> Option<String> {
    if !path.is_absolute() {
        return Some("not an absolute path".into());
    }
    let Ok(meta) = std::fs::symlink_metadata(path) else { return Some("it cannot be inspected".into()) };
    if meta.file_type().is_symlink() {
        return Some("it is a symlink".into());
    }
    if !meta.is_dir() {
        return Some("not a directory".into());
    }
    let Ok(canon) = std::fs::canonicalize(path) else { return Some("it cannot be resolved".into()) };
    // Only ever delete strictly inside the user's home directory: never the
    // home itself, another user's home, or system locations.
    let Some(home) = home_dir().and_then(|h| std::fs::canonicalize(h).ok()) else {
        return Some("the home directory is unknown".into());
    };
    if canon == home || !canon.starts_with(&home) {
        return Some("it is not inside your home directory".into());
    }
    let inside = |other: &Path| std::fs::canonicalize(other).map(|o| o.starts_with(&canon)).unwrap_or(false);
    if inside(daemon_home) {
        return Some("it contains paseo-lite's data".into());
    }
    if let Some(other) = others.iter().find(|o| Path::new(o.as_str()) != path && inside(Path::new(o.as_str()))) {
        return Some(format!("it contains another project ({other})"));
    }
    None
}

/// Ignored paths that are safe to lose because a build or install recreates them.
fn regenerable(rel: &str) -> bool {
    const DIRS: [&str; 16] = [
        "node_modules", "target", "dist", "build", "out", ".next", ".nuxt", ".turbo", ".cache", "coverage", ".venv", "venv", "__pycache__", ".gradle", ".pytest_cache", ".mypy_cache",
    ];
    let rel = rel.trim_end_matches('/');
    rel.split('/').any(|part| DIRS.contains(&part)) || rel.ends_with(".pyc") || rel.ends_with(".log") || rel.ends_with(".DS_Store")
}

/// Why the work in `dir` can't be recovered from a remote; None when it can.
/// Any git error counts as unrecoverable.
async fn unrecoverable(dir: &Path) -> Option<String> {
    let Ok(top) = git(dir, &["rev-parse", "--show-toplevel"]).await else {
        return Some("it is not a git repository, so there is no remote copy".into());
    };
    let same = std::fs::canonicalize(top.trim()).ok() == std::fs::canonicalize(dir).ok();
    if !same {
        return Some("it is a folder inside a larger git repository".into());
    }
    let status = match git(dir, &["status", "--porcelain", "--ignored=matching"]).await {
        Ok(s) => s,
        Err(e) => return Some(format!("git status failed: {e}")),
    };
    for line in status.lines().filter(|l| !l.trim().is_empty()) {
        match line.strip_prefix("!! ") {
            Some(rel) if regenerable(rel) => {}
            Some(rel) => return Some(format!("it has ignored files git can't restore (e.g. {})", rel.trim())),
            None => return Some("it has uncommitted changes".into()),
        }
    }
    // HEAD too, so commits on a detached HEAD count.
    match git(dir, &["log", "HEAD", "--branches", "--not", "--remotes", "--oneline"]).await {
        Ok(s) if !s.trim().is_empty() => return Some(format!("{} commit(s) are not on any remote", s.trim().lines().count())),
        Err(e) => return Some(format!("git log failed: {e}")),
        _ => {}
    }
    match git(dir, &["stash", "list"]).await {
        Ok(s) if !s.trim().is_empty() => return Some("it has stashed changes".into()),
        Err(e) => return Some(format!("git stash list failed: {e}")),
        _ => {}
    }
    match git(dir, &["worktree", "list", "--porcelain"]).await {
        Ok(s) if s.lines().filter(|l| l.starts_with("worktree ")).count() > 1 => return Some("other git worktrees depend on it".into()),
        Err(e) => return Some(format!("git worktree list failed: {e}")),
        _ => {}
    }
    None
}

fn is_empty_dir(path: &Path) -> bool {
    std::fs::read_dir(path).map(|mut d| d.next().is_none()).unwrap_or(false)
}

/// Delete a removed project's directory if the policy allows it.
/// `Ok(true)` deleted, `Ok(false)` nothing to delete, `Err(reason)` kept.
pub async fn delete_project_dir(dir: String, owned: bool, policy: Policy, daemon_home: PathBuf, others: Vec<String>) -> Result<bool, String> {
    let path = Path::new(&dir);
    if policy == Policy::Never {
        return Err("kept: deleting project files is disabled (PASEO_DELETE_PROJECT_FILES=never)".into());
    }
    if std::fs::symlink_metadata(path).is_err() {
        return Ok(false);
    }
    if let Some(reason) = protected(path, &daemon_home, &others) {
        return Err(format!("kept {dir}: {reason}"));
    }
    // A folder paseo-lite created and nobody used yet goes without checks;
    // everything else (even our own clones, which may hold weeks of work)
    // must be recoverable from a remote.
    let fresh = owned && is_empty_dir(path);
    if policy == Policy::Safe && !fresh {
        if let Some(reason) = unrecoverable(path).await {
            return Err(format!("kept {dir}: {reason}"));
        }
    }
    // std's remove_dir_all never follows symlinks out of the tree; it runs on
    // the blocking pool so a big node_modules doesn't stall the daemon.
    tokio::fs::remove_dir_all(path).await.map_err(|e| format!("could not delete {dir}: {e}"))?;
    crate::log!("deleted project directory {dir}");
    Ok(true)
}

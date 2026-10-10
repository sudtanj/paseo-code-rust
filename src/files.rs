//! `file_explorer_request`: list a directory or read a file, confined to the
//! workspace root (same rules as the Node daemon's file-explorer service:
//! symlinks may not escape the root, entries newest first, dangling links
//! skipped, images base64, binaries metadata-only).

use serde_json::{json, Value};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;

/// Text/image payloads above this are refused rather than shipped over the relay.
const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024;
const OUTSIDE: &str = "Access outside of workspace is not allowed";

fn expand_home(path: &str) -> String {
    match (path.strip_prefix('~'), std::env::var("HOME")) {
        (Some(rest), Ok(home)) if rest.is_empty() || rest.starts_with('/') => format!("{home}{rest}"),
        _ => path.to_string(),
    }
}

/// Lexical join + normalisation (no symlink resolution).
fn lexical(base: &Path, rel: &str) -> PathBuf {
    let mut out = PathBuf::new();
    for c in base.join(rel).components() {
        match c {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

struct Scoped {
    /// Root-relative path as requested ("." for the root), forward slashes.
    rel: String,
    /// Canonical path on disk (or the lexical one if it does not exist).
    real: PathBuf,
}

fn scope(root: &Path, canon_root: &Path, rel: &str) -> Result<Scoped, String> {
    let requested = lexical(root, if rel.trim().is_empty() { "." } else { rel });
    let rel_path = requested.strip_prefix(root).map_err(|_| OUTSIDE.to_string())?;
    let rel = if rel_path.as_os_str().is_empty() { ".".to_string() } else { rel_path.to_string_lossy().replace('\\', "/") };
    let real = match std::fs::canonicalize(&requested) {
        Ok(p) => {
            if !p.starts_with(canon_root) {
                return Err(OUTSIDE.into());
            }
            p
        }
        Err(_) => requested,
    };
    Ok(Scoped { rel, real })
}

fn iso(t: std::io::Result<SystemTime>) -> String {
    t.map(|t| humantime::format_rfc3339_millis(t).to_string()).unwrap_or_else(|_| crate::util::now_iso())
}

fn mime_for_image(ext: &str) -> Option<&'static str> {
    Some(match ext {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        _ => return None,
    })
}

fn list(root: &Path, canon_root: &Path, rel: &str) -> Result<Value, String> {
    let dir = scope(root, canon_root, rel)?;
    let meta = std::fs::metadata(&dir.real).map_err(|e| e.to_string())?;
    if !meta.is_dir() {
        return Err("Requested path is not a directory".into());
    }
    let mut entries: Vec<(SystemTime, Value)> = Vec::new();
    for dirent in std::fs::read_dir(&dir.real).map_err(|e| e.to_string())?.flatten() {
        let name = dirent.file_name().to_string_lossy().into_owned();
        let entry_rel = if dir.rel == "." { name.clone() } else { format!("{}/{}", dir.rel, name) };
        // Follow symlinks but keep them inside the root; skip dangling ones.
        let Ok(target) = scope(root, canon_root, &entry_rel) else { continue };
        let Ok(m) = std::fs::metadata(&target.real) else { continue };
        let modified = m.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        entries.push((
            modified,
            json!({
                "name": name,
                "path": entry_rel,
                "kind": if m.is_dir() { "directory" } else { "file" },
                "size": m.len(),
                "modifiedAt": iso(Ok(modified)),
            }),
        ));
    }
    entries.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1["name"].as_str().cmp(&b.1["name"].as_str())));
    Ok(json!({ "path": dir.rel, "entries": entries.into_iter().map(|(_, v)| v).collect::<Vec<_>>() }))
}

fn read(root: &Path, canon_root: &Path, rel: &str, max_bytes: Option<u64>) -> Result<Value, String> {
    let file = scope(root, canon_root, rel)?;
    let meta = std::fs::metadata(&file.real).map_err(|e| e.to_string())?;
    if !meta.is_file() {
        return Err("Requested path is not a file".into());
    }
    let size = meta.len();
    if size > max_bytes.unwrap_or(MAX_FILE_BYTES).min(MAX_FILE_BYTES) {
        return Err("File is too large to display".into());
    }
    let revision = format!("{}:{}:{}:{}", meta.dev(), meta.ino(), size, meta.mtime_nsec() + meta.mtime() * 1_000_000_000);
    let ext = file.real.extension().and_then(|e| e.to_str()).unwrap_or("").to_ascii_lowercase();
    let base = json!({ "path": file.rel, "size": size, "modifiedAt": iso(meta.modified()), "revision": revision });
    let bytes = std::fs::read(&file.real).map_err(|e| e.to_string())?;
    let mut out = base;
    if let Some(mime) = mime_for_image(&ext) {
        use base64::Engine;
        out["kind"] = json!("image");
        out["encoding"] = json!("base64");
        out["mimeType"] = json!(mime);
        out["content"] = json!(base64::engine::general_purpose::STANDARD.encode(&bytes));
        return Ok(out);
    }
    let sample = &bytes[..bytes.len().min(8192)];
    match (sample.contains(&0), String::from_utf8(bytes)) {
        (false, Ok(text)) => {
            out["kind"] = json!("text");
            out["encoding"] = json!("utf-8");
            out["mimeType"] = json!(if ext == "json" { "application/json" } else { "text/plain" });
            out["content"] = json!(text);
        }
        _ => {
            out["kind"] = json!("binary");
            out["encoding"] = json!("none");
            out["mimeType"] = json!("application/octet-stream");
        }
    }
    Ok(out)
}

/// Full `file_explorer_response` payload.
pub fn explore(msg: &Value, request_id: &str) -> Value {
    let s = |k: &str| msg.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
    let (cwd, path, mode) = (s("cwd"), s("path"), s("mode"));
    let mode = if mode == "file" { "file" } else { "list" };
    let mut payload = json!({ "cwd": cwd, "path": if path.is_empty() { "." } else { path.as_str() }, "mode": mode, "directory": null, "file": null, "error": null, "requestId": request_id });
    if cwd.trim().is_empty() {
        payload["error"] = json!("cwd is required");
        return payload;
    }
    let root = lexical(Path::new("/"), &expand_home(cwd.trim()));
    let result = std::fs::canonicalize(&root).map_err(|e| format!("{}: {e}", root.display())).and_then(|canon_root| {
        if mode == "list" {
            list(&root, &canon_root, &path).map(|d| ("directory", d))
        } else {
            read(&root, &canon_root, &path, msg.get("maxBytes").and_then(|v| v.as_u64())).map(|f| ("file", f))
        }
    });
    match result {
        Ok((key, value)) => {
            payload["path"] = value["path"].clone();
            payload[key] = value;
        }
        Err(e) => payload["error"] = json!(e),
    }
    payload
}

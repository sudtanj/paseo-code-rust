//! Images and file uploads from the app.
//!
//! - Files arrive as `file.upload.request` followed by binary frames
//!   (upstream's file-transfer.ts: `0x10` begin, `0x11` chunk, `0x12` end, each
//!   `[opcode][idLen][requestId][body]`). They are written to
//!   `<home>/uploads/<id>/<name>` and the app gets back an `uploaded_file`
//!   reference it attaches to a later message.
//! - Images sent inline (`images: [{data, mimeType}]`) are saved the same way.
//!
//! The prompt the agent receives names every attachment; images are marked
//! with an `Attached image: <path>` line, which the providers turn into a real
//! image input (Claude image block, Codex local image). Because everything is
//! carried as text, queueing, merging, retries and restart-resume keep working.

use base64::Engine;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio::io::AsyncWriteExt;

pub const IMAGE_LINE: &str = "Attached image: ";
/// Separates the user's own text from the attachment notes appended for the agent.
pub const ATTACHMENTS_HEADER: &str = "\n\n[Attachments]\n";
const MAX_UPLOAD_BYTES: u64 = 100 * 1024 * 1024;
/// Uploads are working files for agents, not an archive.
const KEEP_FOR: Duration = Duration::from_secs(14 * 24 * 3600);

const OP_BEGIN: u8 = 0x10;
const OP_CHUNK: u8 = 0x11;
const OP_END: u8 = 0x12;

/// A binary frame the session should handle (file transfer), as opposed to JSON text.
pub fn is_file_frame(bytes: &[u8]) -> bool {
    matches!(bytes.first(), Some(&(OP_BEGIN | OP_CHUNK | OP_END)))
}

/// `(opcode, requestId, body)` of a file-transfer frame.
fn decode_frame(bytes: &[u8]) -> Option<(u8, String, &[u8])> {
    let (&op, rest) = bytes.split_first()?;
    let (&id_len, rest) = rest.split_first()?;
    let id_len = id_len as usize;
    if id_len == 0 || id_len > rest.len() {
        return None;
    }
    let id = String::from_utf8(rest[..id_len].to_vec()).ok()?;
    Some((op, id, &rest[id_len..]))
}

/// Keep the client's name, replacing what can't appear in a file name.
pub fn sanitize_file_name(name: &str) -> String {
    let base = name.rsplit(['/', '\\']).next().unwrap_or("");
    let cleaned: String = base.chars().map(|c| if c.is_control() || "\\/:*?\"<>|".contains(c) { '_' } else { c }).collect();
    let cleaned = cleaned.trim().to_string();
    let mut out = if cleaned.is_empty() || cleaned == "." || cleaned == ".." { "upload".to_string() } else { cleaned };
    while out.len() > 255 {
        out.pop();
    }
    out
}

fn new_upload_id() -> String {
    format!("upload_{}", uuid::Uuid::new_v4())
}

fn uploads_dir(home: &Path) -> PathBuf {
    home.join("uploads")
}

struct Pending {
    id: String,
    file_name: String,
    mime: String,
    size: u64,
    path: PathBuf,
    received: u64,
    file: Option<tokio::fs::File>,
}

/// In-flight uploads of one session (one connected client).
#[derive(Default)]
pub struct Uploads {
    pending: HashMap<String, Pending>,
}

impl Uploads {
    /// `file.upload.request`: remember the upload; bytes follow as frames.
    /// Returns an immediate error response when it can't be accepted.
    pub fn begin(&mut self, home: &Path, msg: &Value) -> Option<Value> {
        let rid = msg.get("requestId").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let size = msg.get("size").and_then(|v| v.as_u64()).unwrap_or(0);
        if size > MAX_UPLOAD_BYTES {
            return Some(response(&rid, None, Some(format!("File is too large ({} MB max)", MAX_UPLOAD_BYTES / 1024 / 1024))));
        }
        let file_name = sanitize_file_name(msg.get("fileName").and_then(|v| v.as_str()).unwrap_or("upload"));
        let mime = msg.get("mimeType").and_then(|v| v.as_str()).unwrap_or("application/octet-stream").to_string();
        let id = new_upload_id();
        let path = uploads_dir(home).join(&id).join(&file_name);
        self.pending.insert(rid, Pending { id, file_name, mime, size, path, received: 0, file: None });
        None
    }

    /// Apply one binary frame. Returns the `file.upload.response` when the
    /// upload finished or failed.
    pub async fn frame(&mut self, bytes: &[u8]) -> Option<Value> {
        let (op, rid, body) = decode_frame(bytes)?;
        let result = match self.pending.get_mut(&rid) {
            None => return None,
            Some(p) => apply(p, op, body).await,
        };
        match result {
            Ok(false) => None,
            Ok(true) => {
                let p = self.pending.remove(&rid)?;
                if p.received != p.size {
                    remove_upload(&p).await;
                    return Some(response(&rid, None, Some(format!("Upload size mismatch: expected {}, received {}.", p.size, p.received))));
                }
                crate::log!("upload {} saved ({} bytes)", p.path.display(), p.size);
                let file = json!({ "type": "uploaded_file", "id": p.id, "fileName": p.file_name, "mimeType": p.mime, "size": p.size, "path": p.path.to_string_lossy() });
                Some(response(&rid, Some(file), None))
            }
            Err(e) => {
                if let Some(p) = self.pending.remove(&rid) {
                    remove_upload(&p).await;
                }
                Some(response(&rid, None, Some(e)))
            }
        }
    }
}

/// Ok(true) when the upload is complete.
async fn apply(p: &mut Pending, op: u8, body: &[u8]) -> Result<bool, String> {
    match op {
        OP_BEGIN => {
            let dir = p.path.parent().ok_or("bad upload path")?;
            tokio::fs::create_dir_all(dir).await.map_err(|e| e.to_string())?;
            p.file = Some(tokio::fs::File::create(&p.path).await.map_err(|e| e.to_string())?);
            Ok(false)
        }
        OP_CHUNK => {
            let file = p.file.as_mut().ok_or("Upload chunks arrived before file begin.")?;
            let next = p.received + body.len() as u64;
            if next > p.size {
                return Err(format!("Upload exceeded declared size: expected {}, received {}.", p.size, next));
            }
            file.write_all(body).await.map_err(|e| e.to_string())?;
            p.received = next;
            Ok(false)
        }
        OP_END => {
            if let Some(mut f) = p.file.take() {
                f.flush().await.map_err(|e| e.to_string())?;
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

async fn remove_upload(p: &Pending) {
    if let Some(dir) = p.path.parent() {
        let _ = tokio::fs::remove_dir_all(dir).await;
    }
}

fn response(rid: &str, file: Option<Value>, error: Option<String>) -> Value {
    json!({ "type": "file.upload.response", "payload": { "requestId": rid, "file": file, "error": error } })
}

fn extension_for(mime: &str) -> &'static str {
    match mime {
        "image/png" => "png",
        "image/jpeg" | "image/jpg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "image/heic" => "heic",
        _ => "img",
    }
}

/// Save an inline (base64) image and return its path.
fn save_inline_image(home: &Path, data: &str, mime: &str) -> Option<PathBuf> {
    // Data URLs are accepted too.
    let raw = data.split_once("base64,").map(|(_, b)| b).unwrap_or(data);
    let bytes = base64::engine::general_purpose::STANDARD.decode(raw.trim()).ok()?;
    let dir = uploads_dir(home).join(new_upload_id());
    std::fs::create_dir_all(&dir).ok()?;
    let path = dir.join(format!("image.{}", extension_for(mime)));
    std::fs::write(&path, bytes).ok()?;
    Some(path)
}

/// One attachment as text for the agent (mirrors upstream's renderPromptAttachmentAsText).
fn render_attachment(a: &Value) -> Option<String> {
    let s = |k: &str| a.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
    match a.get("type").and_then(|t| t.as_str())? {
        "uploaded_file" => {
            let mut out = format!("Uploaded file: {}\nPath: {}\nMIME: {}\nSize: {} bytes", s("fileName"), s("path"), s("mimeType"), a.get("size").and_then(|v| v.as_u64()).unwrap_or(0));
            if s("mimeType").starts_with("image/") {
                out.push_str(&format!("\n{IMAGE_LINE}{}", s("path")));
            }
            Some(out)
        }
        "text" => Some(s("text")),
        "review" => {
            let mut lines = vec![format!("Paseo review attachment ({})", s("mode")), format!("CWD: {}", s("cwd"))];
            for (i, c) in a.get("comments").and_then(|c| c.as_array()).into_iter().flatten().enumerate() {
                let g = |k: &str| c.get(k).map(|v| v.as_str().map(String::from).unwrap_or_else(|| v.to_string())).unwrap_or_default();
                lines.push(format!("\nComment {}: {}:{}:{}\n{}", i + 1, g("filePath"), g("side"), g("lineNumber"), g("body")));
            }
            Some(lines.join("\n"))
        }
        kind => {
            // PRs and issues (GitHub/forge): title, link, body.
            let number = a.get("number").map(|n| format!(" #{n}")).unwrap_or_default();
            let mut out = format!("{kind}{number}: {}\n{}", s("title"), s("url"));
            let body = s("body");
            if !body.is_empty() {
                out.push_str(&format!("\n\n{body}"));
            }
            Some(out)
        }
    }
}

/// The prompt for a message with images and/or attachments. Plain messages
/// are returned unchanged.
pub fn build_prompt(home: &Path, text: &str, images: Option<&Value>, attachments: Option<&Value>) -> String {
    let mut notes: Vec<String> = Vec::new();
    for img in images.and_then(|v| v.as_array()).into_iter().flatten() {
        let data = img.get("data").and_then(|v| v.as_str()).unwrap_or("");
        let mime = img.get("mimeType").and_then(|v| v.as_str()).unwrap_or("image/png");
        match save_inline_image(home, data, mime) {
            Some(path) => notes.push(format!("{IMAGE_LINE}{}", path.to_string_lossy())),
            None => crate::warn!("dropped an image attachment that was not valid base64"),
        }
    }
    for a in attachments.and_then(|v| v.as_array()).into_iter().flatten() {
        if let Some(rendered) = render_attachment(a) {
            notes.push(rendered);
        }
    }
    if notes.is_empty() {
        return text.to_string();
    }
    format!("{}{ATTACHMENTS_HEADER}{}", text.trim_end(), notes.join("\n\n"))
}

/// What the chat bubble shows: the user's text plus a short list of attachments.
pub fn bubble_text(prompt: &str) -> String {
    let Some((text, notes)) = prompt.split_once(ATTACHMENTS_HEADER) else { return prompt.to_string() };
    let names: Vec<String> = notes
        .lines()
        .filter_map(|l| {
            l.strip_prefix("Uploaded file: ").map(String::from).or_else(|| {
                l.strip_prefix(IMAGE_LINE).filter(|p| !notes.contains(&format!("Path: {p}"))).map(|p| Path::new(p).file_name().map(|f| f.to_string_lossy().into_owned()).unwrap_or_else(|| "image".into()))
            })
        })
        .collect();
    let mut out = text.to_string();
    if !names.is_empty() {
        out.push_str(&format!("\n\n📎 {}", names.join(", ")));
    }
    out.trim_start().to_string()
}

/// Paths of images to hand to the model as images.
pub fn image_paths(prompt: &str) -> Vec<String> {
    prompt.lines().filter_map(|l| l.strip_prefix(IMAGE_LINE)).map(|p| p.trim().to_string()).filter(|p| Path::new(p).is_file()).collect()
}

pub fn mime_for_path(path: &str) -> &'static str {
    match path.rsplit('.').next().map(|e| e.to_ascii_lowercase()).as_deref() {
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        _ => "image/png",
    }
}

/// Delete uploads older than two weeks (run at startup).
pub fn prune(home: &Path) {
    let Ok(entries) = std::fs::read_dir(uploads_dir(home)) else { return };
    let now = SystemTime::now();
    for e in entries.flatten() {
        let old = e.metadata().and_then(|m| m.modified()).ok().and_then(|t| now.duration_since(t).ok()).map(|age| age > KEEP_FOR).unwrap_or(false);
        if old {
            let _ = std::fs::remove_dir_all(e.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(op: u8, id: &str, body: &[u8]) -> Vec<u8> {
        let mut v = vec![op, id.len() as u8];
        v.extend_from_slice(id.as_bytes());
        v.extend_from_slice(body);
        v
    }

    #[test]
    fn file_names_are_sanitized() {
        assert_eq!(sanitize_file_name("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_file_name("a:b*c?.txt"), "a_b_c_.txt");
        assert_eq!(sanitize_file_name(".."), "upload");
        assert_eq!(sanitize_file_name("  "), "upload");
        assert_eq!(sanitize_file_name("photo.png"), "photo.png");
    }

    #[test]
    fn decodes_frames() {
        let f = frame(OP_CHUNK, "r1", b"hello");
        assert!(is_file_frame(&f));
        assert_eq!(decode_frame(&f), Some((OP_CHUNK, "r1".to_string(), &b"hello"[..])));
        assert!(!is_file_frame(b"{\"type\":\"x\"}"));
        assert!(decode_frame(&[OP_END, 0]).is_none());
    }

    #[tokio::test]
    async fn full_upload_round_trip() {
        let home = std::env::temp_dir().join(format!("paseo-lite-up-{}", uuid::Uuid::new_v4()));
        let mut up = Uploads::default();
        let req = json!({ "type": "file.upload.request", "requestId": "r1", "fileName": "notes.txt", "mimeType": "text/plain", "size": 11, "modifiedAt": "x" });
        assert!(up.begin(&home, &req).is_none());
        assert!(up.frame(&frame(OP_BEGIN, "r1", b"\x00\x02{}")).await.is_none());
        assert!(up.frame(&frame(OP_CHUNK, "r1", b"hello ")).await.is_none());
        assert!(up.frame(&frame(OP_CHUNK, "r1", b"world")).await.is_none());
        let done = up.frame(&frame(OP_END, "r1", b"")).await.unwrap();
        let file = &done["payload"]["file"];
        assert!(done["payload"]["error"].is_null(), "{done}");
        assert_eq!(file["fileName"], "notes.txt");
        assert_eq!(std::fs::read_to_string(file["path"].as_str().unwrap()).unwrap(), "hello world");
        let _ = std::fs::remove_dir_all(&home);
    }

    #[tokio::test]
    async fn oversize_and_mismatch_are_rejected() {
        let home = std::env::temp_dir().join(format!("paseo-lite-up-{}", uuid::Uuid::new_v4()));
        let mut up = Uploads::default();
        let too_big = json!({ "requestId": "big", "fileName": "x", "mimeType": "a/b", "size": MAX_UPLOAD_BYTES + 1 });
        assert!(up.begin(&home, &too_big).unwrap()["payload"]["error"].is_string());
        up.begin(&home, &json!({ "requestId": "r2", "fileName": "y", "mimeType": "a/b", "size": 2 }));
        up.frame(&frame(OP_BEGIN, "r2", b"")).await;
        let over = up.frame(&frame(OP_CHUNK, "r2", b"abc")).await.unwrap();
        assert!(over["payload"]["error"].as_str().unwrap().contains("exceeded"));
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn prompt_and_bubble() {
        let home = std::env::temp_dir().join(format!("paseo-lite-up-{}", uuid::Uuid::new_v4()));
        let png = base64::engine::general_purpose::STANDARD.encode(b"\x89PNG fake");
        let att = json!([{ "type": "uploaded_file", "id": "u", "fileName": "spec.pdf", "mimeType": "application/pdf", "size": 3, "path": "/tmp/spec.pdf" }]);
        let prompt = build_prompt(&home, "look at this", Some(&json!([{ "data": png, "mimeType": "image/png" }])), Some(&att));
        assert!(prompt.starts_with("look at this"));
        assert!(prompt.contains("Path: /tmp/spec.pdf"));
        let imgs = image_paths(&prompt);
        assert_eq!(imgs.len(), 1);
        assert_eq!(std::fs::read(&imgs[0]).unwrap(), b"\x89PNG fake");
        let bubble = bubble_text(&prompt);
        assert!(bubble.starts_with("look at this\n\n📎 "), "{bubble}");
        assert!(bubble.contains("spec.pdf") && bubble.contains("image.png"));
        assert_eq!(build_prompt(&home, "plain", None, None), "plain");
        assert_eq!(bubble_text("plain"), "plain");
        let _ = std::fs::remove_dir_all(&home);
    }
}

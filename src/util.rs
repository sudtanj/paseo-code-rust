use std::io::Write;
use std::path::Path;
use std::time::SystemTime;

/// Info-level log line (suppressed by `PASEO_LOG_CONSOLE_LEVEL=warn|error`).
#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        if $crate::util::log_enabled(2) {
            eprintln!("[{}] {}", $crate::util::now_iso(), format!($($arg)*))
        }
    };
}

/// Warning-level log line, for failures the operator should see.
#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {
        if $crate::util::log_enabled(3) {
            eprintln!("[{}] WARN {}", $crate::util::now_iso(), format!($($arg)*))
        }
    };
}

/// 1=debug 2=info 3=warn 4=error, from `PASEO_LOG_CONSOLE_LEVEL` (default info).
pub fn log_enabled(level: u8) -> bool {
    static MIN: std::sync::OnceLock<u8> = std::sync::OnceLock::new();
    let min = *MIN.get_or_init(|| match std::env::var("PASEO_LOG_CONSOLE_LEVEL").unwrap_or_default().trim().to_ascii_lowercase().as_str() {
        "trace" | "debug" => 1,
        "warn" | "warning" => 3,
        "error" | "fatal" | "silent" => 4,
        _ => 2,
    });
    level >= min
}

pub fn now_iso() -> String {
    humantime::format_rfc3339_millis(SystemTime::now()).to_string()
}

pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Write via temp file + rename so a crash never leaves a torn file behind.
pub fn write_atomic(path: &Path, data: &[u8], private: bool) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension(format!("tmp{}", std::process::id()));
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        if private {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        #[cfg(not(unix))]
        let _ = private;
        let mut f = opts.open(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

pub fn basename(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string())
}

/// Truncate to at most `max` bytes on a char boundary.
pub fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_keeps_short_strings() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn truncate_cuts_on_char_boundary() {
        // "é" is two bytes; cutting at byte 1 must back up to 0.
        assert_eq!(truncate("éa", 1), "…");
        assert_eq!(truncate("abcdef", 3), "abc…");
    }

    #[test]
    fn basename_of_paths() {
        assert_eq!(basename("/home/u/project"), "project");
    }

    #[test]
    fn ids_are_unique() {
        assert_ne!(new_id(), new_id());
    }
}

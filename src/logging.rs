//! Structured JSON logging in the style of Go's slog: one JSON object per line with msg, level, ts and key-values.

use slog::{o, Drain, Logger};
use std::io::Write;
use std::sync::Mutex;

/// Build a logger writing JSON to `out` at or above `level` (synchronous drain).
pub fn build_logger<W>(out: W, level: slog::Level) -> Logger
where
    W: Write + Send + 'static,
{
    let drain = slog_json::Json::new(out)
        .add_default_keys() // emits "msg", "level", "ts"
        .build()
        .fuse();
    // slog_json::Json's internal RefCell makes it neither Sync nor RefUnwindSafe;
    // Mutex restores both unconditionally so the drain satisfies Logger::root's bounds.
    let drain = Mutex::new(drain).fuse();
    Logger::root(
        drain.filter_level(level).fuse(),
        o!("service" => "dicom-router"),
    )
}

/// Build the process root logger (JSON to stdout, async drain) and install the
/// bridges that route dependency diagnostics into it:
/// - rustls emits `log` records (its `logging` feature is enabled)
/// - dicom-ul emits `tracing` events, which fall back to `log` records
///   (the `tracing/log` feature is enabled in Cargo.toml)
///
/// Both are captured by slog-stdlog into the slog-scope global logger.
/// Returns the slog-scope guard: THE CALLER MUST KEEP IT ALIVE for the
/// process lifetime, otherwise bridged records are silently discarded.
pub fn init_logger(level: slog::Level) -> (Logger, slog_scope::GlobalLoggerGuard) {
    let drain = slog_json::Json::new(std::io::stdout())
        .add_default_keys()
        .build()
        .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = Logger::root(
        drain.filter_level(level).fuse(),
        o!("service" => "dicom-router"),
    );
    let guard = slog_scope::set_global_logger(log.clone());
    slog_stdlog::init().expect("failed to install slog-stdlog bridge");
    (log, guard)
}

/// Parse a log level name (slog-style, case-insensitive).
pub fn parse_level(s: &str) -> slog::Level {
    match s.to_ascii_lowercase().as_str() {
        "critical" | "crit" => slog::Level::Critical,
        "error" => slog::Level::Error,
        "warn" | "warning" => slog::Level::Warning,
        "debug" => slog::Level::Debug,
        "trace" => slog::Level::Trace,
        _ => slog::Level::Info,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);
    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn emits_json_with_msg_field() {
        let buf = SharedBuf(Arc::new(Mutex::new(Vec::new())));
        let log = build_logger(buf.clone(), slog::Level::Info);
        slog::info!(log, "hello world"; "destination" => "pacs-1");
        let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        let line: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
        assert_eq!(line["msg"], "hello world");
        assert_eq!(line["level"], "INFO");
        assert_eq!(line["destination"], "pacs-1");
    }

    #[test]
    fn level_filtering_works() {
        let buf = SharedBuf(Arc::new(Mutex::new(Vec::new())));
        let log = build_logger(buf.clone(), slog::Level::Warning);
        slog::info!(log, "not emitted");
        slog::warn!(log, "emitted");
        let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(!out.contains("not emitted"));
        assert!(out.contains("\"msg\":\"emitted\""));
    }
}

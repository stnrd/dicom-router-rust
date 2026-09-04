//! Structured JSON logging in the style of Go's slog: one JSON object per line with msg, level, ts and key-values.
//!
//! Downstream code should never build additional root loggers. Instead, attach context once
//! by deriving a child logger — e.g. `log.new(o!("destination" => name))` per association or
//! per destination — so every record logged through it automatically carries that key-value.

use slog::{o, Drain, FnValue, Logger, PushFnValue, Record};
use std::io::Write;
use std::sync::{Mutex, Once};

/// Root-logger key identifying this process in aggregated log output.
const SERVICE: &str = "dicom-router";

/// Build the shared JSON key-value drain used by both [`build_logger`] and [`init_logger`].
///
/// Emits `ts` (RFC 3339 UTC timestamp), `level` (Go slog's level strings, e.g. `"WARN"` and
/// `"ERROR"` — neither slog-json's default 4-char short names like `"WARN"`/`"ERRO"` nor
/// slog's own long names like `"WARNING"`), `msg`, and `src` (the `log` target for bridged
/// dependency records, or the Rust module path for records logged natively through this
/// crate's loggers).
///
/// Write errors are ignored (`ignore_res`) rather than fused into a panic: a transient stdout
/// error must not kill the async worker thread in [`init_logger`] and permanently disable
/// logging for the rest of the process.
fn json_drain<W>(out: W) -> impl Drain<Ok = (), Err = slog::Never> + Send + 'static
where
    W: Write + Send + 'static,
{
    slog_json::Json::new(out)
        .add_key_value(o!(
            "ts" => FnValue(|_: &Record| {
                time::OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)
                    .ok()
            }),
            // Explicit table (not `Level::as_str()`) so `Warning` renders as Go slog's "WARN"
            // rather than slog's own "WARNING".
            "level" => FnValue(|r: &Record| match r.level() {
                slog::Level::Critical => "CRITICAL",
                slog::Level::Error => "ERROR",
                slog::Level::Warning => "WARN",
                slog::Level::Info => "INFO",
                slog::Level::Debug => "DEBUG",
                slog::Level::Trace => "TRACE",
            }),
            "msg" => PushFnValue(|r: &Record, ser| ser.emit(r.msg())),
            // PushFnValue serializes directly, avoiding a per-record String allocation that
            // FnValue would require to unify the `&str` lifetimes of `tag()` and `module()`.
            "src" => PushFnValue(|r: &Record, ser| {
                ser.emit(if r.tag().is_empty() { r.module() } else { r.tag() })
            }),
        ))
        .build()
        .ignore_res()
}

/// Build a logger writing JSON to `out` at or above `level` (synchronous drain).
///
/// Every record carries the top-level `service = "dicom-router"` key from the root logger's
/// own key-values, in addition to the shared `ts`/`level`/`msg`/`src` keys.
pub fn build_logger<W>(out: W, level: slog::Level) -> Logger
where
    W: Write + Send + 'static,
{
    // slog_json::Json's internal RefCell makes it neither Sync nor RefUnwindSafe;
    // Mutex restores both unconditionally so the drain satisfies Logger::root's bounds.
    let drain = Mutex::new(json_drain(out)).fuse();
    Logger::root(drain.filter_level(level).fuse(), o!("service" => SERVICE))
}

/// Returned by [`init_logger`]. Its `Drop` impl flushes and joins the async drain's
/// background worker thread, guaranteeing buffered records are written before the
/// process exits.
///
/// The global logger installed by `init_logger` is never uninstalled (slog-scope's
/// fallback drain panics on every record, so resetting it on drop would turn a normal
/// shutdown into a crash) — dropping this guard early does NOT panic. It just means
/// any record logged afterwards is silently dropped rather than reaching stdout, since
/// the worker thread that writes JSON has already stopped. Keep it alive for the
/// process lifetime to avoid losing buffered records.
#[must_use = "dropping the guard early may silently lose log records; keep it alive until shutdown"]
pub struct LoggerGuard(#[allow(dead_code)] slog_async::AsyncGuard);

/// Ensures the `log` -> slog-scope bridge is installed at most once per process.
static STDLOG_INIT: Once = Once::new();

/// Build the process root logger (JSON to stdout, async drain) and install the
/// bridges that route dependency diagnostics into it:
/// - rustls emits `log` records (its `logging` feature is enabled)
/// - dicom-ul emits `tracing` events, which fall back to `log` records
///   (the `tracing/log` feature is enabled in Cargo.toml)
///
/// Both are captured by slog-stdlog into the slog-scope global logger.
/// Returns a [`LoggerGuard`]: THE CALLER MUST KEEP IT ALIVE for the process lifetime,
/// otherwise buffered records may be lost (see [`LoggerGuard`] for the exact semantics).
///
/// The `log` crate backend can only be installed once per process; on the first call this
/// installs it filtered at `level` (mapped to the nearest `log::Level`), and that filter
/// applies for the remainder of the process regardless of the `level` passed to later calls.
///
/// # Panics
/// Panics if a `log` backend was already installed by something other than a previous call
/// to `init_logger` (e.g. `env_logger::init()`), since only one `log` backend can exist per
/// process.
pub fn init_logger(level: slog::Level) -> (Logger, LoggerGuard) {
    let drain = json_drain(std::io::stdout());
    // Drop rather than block on overflow: blocking would stall a tokio worker
    // thread on stdout I/O. DropAndReport emits a record with the dropped count.
    let (drain, async_guard) = slog_async::Async::new(drain)
        .chan_size(4096)
        .overflow_strategy(slog_async::OverflowStrategy::DropAndReport)
        .thread_name("slog-async".into())
        .build_with_guard();
    let log = Logger::root(
        drain.fuse().filter_level(level).fuse(),
        o!("service" => SERVICE),
    );
    // Never uninstall the global logger: slog-scope's fallback drain panics on every
    // record, so a dropped guard would turn a shutdown into a crash.
    slog_scope::set_global_logger(log.clone()).cancel_reset();

    STDLOG_INIT.call_once(|| {
        let log_level = match level {
            slog::Level::Critical | slog::Level::Error => log::Level::Error,
            slog::Level::Warning => log::Level::Warn,
            slog::Level::Info => log::Level::Info,
            slog::Level::Debug => log::Level::Debug,
            slog::Level::Trace => log::Level::Trace,
        };
        slog_stdlog::init_with_level(log_level).expect("failed to install slog-stdlog bridge");
    });

    (log, LoggerGuard(async_guard))
}

/// Parse a log level name. Matching is case-insensitive and by prefix against the long
/// (`"warning"`) or short (`"warn"`) canonical names, so `"warn"`, `"WARNING"`, and `"warni"`
/// all resolve to `Warning`. Returns `None` for `"off"` (slog's `Level` has no such variant)
/// or any other unrecognized input.
pub fn parse_level(s: &str) -> Option<slog::Level> {
    s.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Arc;

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
        assert!(line["ts"].is_string());
    }

    #[test]
    fn emits_full_level_name_for_error() {
        let buf = SharedBuf(Arc::new(Mutex::new(Vec::new())));
        let log = build_logger(buf.clone(), slog::Level::Info);
        slog::error!(log, "boom");
        let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        let line: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
        assert_eq!(line["level"], "ERROR");
    }

    #[test]
    fn emits_go_slog_level_name_for_warning() {
        let buf = SharedBuf(Arc::new(Mutex::new(Vec::new())));
        let log = build_logger(buf.clone(), slog::Level::Info);
        slog::warn!(log, "careful");
        let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        let line: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
        // Go slog uses "WARN", not slog's own "WARNING".
        assert_eq!(line["level"], "WARN");
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

    #[test]
    fn parse_level_accepts_long_and_short_forms_case_insensitively() {
        assert_eq!(parse_level("warn"), Some(slog::Level::Warning));
        assert_eq!(parse_level("DEBUG"), Some(slog::Level::Debug));
    }

    #[test]
    fn parse_level_rejects_invalid_input() {
        assert_eq!(parse_level("warining"), None);
    }
}

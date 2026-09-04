//! Integration test for the `log` -> slog-scope bridge installed by `logging::init_logger`.
//!
//! Runs in its own process (cargo builds each file under `tests/` as a separate test binary),
//! which sidesteps the process-global state touched by `log::set_boxed_logger` and
//! `slog_scope::set_global_logger` — calling `init_logger` from unit tests sharing a process
//! with other tests would risk cross-test interference.

use dicom_router::logging;

#[test]
fn bridge_install_does_not_panic_and_logger_still_works() {
    let (log, _guard) = logging::init_logger(slog::Level::Info);

    assert_eq!(
        log::max_level(),
        log::LevelFilter::Info,
        "bridge should install a log backend filtered at the requested level"
    );

    // A bridged `log` record must be accepted without panicking, even though the
    // underlying `log` backend can only be installed once per process.
    log::warn!("bridge test");

    // The native slog logger returned alongside the guard must still work.
    slog::info!(log, "native record after bridge install");
}

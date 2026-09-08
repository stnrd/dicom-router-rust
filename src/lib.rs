#![forbid(unsafe_code)]

pub mod config;
pub mod dimse;
pub mod dispatcher;
pub mod logging;
pub mod queue;
pub mod retry;
pub mod scp;
pub mod scu;
pub mod tls;

use std::path::PathBuf;
use std::sync::Arc;

use slog::{error, info, warn};
use tokio_util::sync::CancellationToken;

pub fn run(config_path: PathBuf, check_only: bool) -> i32 {
  let cfg = match config::Config::load(&config_path) {
    Ok(c) => c,
    Err(e) => {
      eprintln!("configuration error: {e}");
      return 2;
    }
  };
  if check_only {
    println!("configuration OK ({} destinations)", cfg.destinations.len());
    return 0;
  }

  let (log, _guard) = logging::init_logger(logging::parse_level(&cfg.log_level).unwrap_or(slog::Level::Info));
  let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
    Ok(rt) => rt,
    Err(e) => {
      error!(log, "failed to build tokio runtime"; "error" => %e);
      return 2;
    }
  };
  runtime.block_on(async_main(cfg, log))
}

async fn async_main(cfg: config::Config, log: slog::Logger) -> i32 {
  let cfg = Arc::new(cfg);
  let shutdown = CancellationToken::new();

  let server_tls =
    match tls::build_server_config(&cfg.tls.server_cert, &cfg.tls.server_key, cfg.tls.client_ca.as_deref()) {
      Ok(c) => c,
      Err(e) => {
        error!(log, "failed to build server TLS config"; "error" => %e);
        return 2;
      }
    };

  for dest in &cfg.destinations {
    if let Err(e) = std::fs::create_dir_all(cfg.queue_dir_for(&dest.name)) {
      error!(
          log,
          "cannot create queue dir";
          "destination" => &dest.name,
          "error" => %e
      );
      return 2;
    }
  }
  if let Err(e) = std::fs::create_dir_all(&cfg.dead_letter_dir) {
    error!(log, "cannot create dead-letter dir"; "error" => %e);
    return 2;
  }

  for dest in &cfg.destinations {
    let qdir = cfg.queue_dir_for(&dest.name);
    match queue::cleanup_stale(&qdir, queue::DEFAULT_STALE_MAX_AGE) {
      Ok(stats) if stats.part_files_removed > 0 || stats.orphan_dcm_removed > 0 => {
        info!(
            log,
            "removed stale queue debris at startup";
            "destination" => &dest.name,
            "part_files_removed" => stats.part_files_removed,
            "orphan_dcm_removed" => stats.orphan_dcm_removed
        );
      }
      Ok(_) => {}
      Err(e) => {
        warn!(
            log,
            "queue cleanup failed at startup";
            "destination" => &dest.name,
            "error" => %e
        );
      }
    }
  }

  let scp_handle = match scp::spawn(cfg.clone(), server_tls, log.clone(), shutdown.clone()).await {
    Ok(h) => h,
    Err(e) => {
      error!(
          log,
          "failed to bind listener";
          "listen_addr" => &cfg.listen_addr,
          "error" => %e
      );
      return 2;
    }
  };

  let mut dispatcher_handles = Vec::new();
  for dest in &cfg.destinations {
    let client_tls =
      match tls::build_client_config(&dest.ca_cert, dest.client_cert.as_deref(), dest.client_key.as_deref()) {
        Ok(c) => c,
        Err(e) => {
          error!(
              log,
              "failed to build client TLS config";
              "destination" => &dest.name,
              "error" => %e
          );
          shutdown.cancel();
          let _ = scp_handle.await;
          return 2;
        }
      };
    dispatcher_handles.push(dispatcher::spawn(dispatcher::WorkerConfig {
      destination: dest.clone(),
      client_tls,
      queue_root: cfg.queue_dir.clone(),
      dead_letter_dir: cfg.dead_letter_dir.clone(),
      retry_cfg: cfg.retry.clone(),
      calling_ae_title: cfg.ae_title.clone(),
      max_pdu_length: cfg.max_pdu_length,
      max_concurrent_sends: cfg.max_concurrent_sends,
      log: log.clone(),
      shutdown: shutdown.clone(),
    }));
  }

  info!(
      log,
      "dicom-router started";
      "ae_title" => &cfg.ae_title,
      "destinations" => cfg.destinations.len() as u64
  );

  let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    .expect("failed to install SIGTERM handler");
  tokio::select! {
      _ = tokio::signal::ctrl_c() => {}
      _ = sigterm.recv() => {}
  }
  info!(log, "shutdown requested, draining");
  shutdown.cancel();

  let _ = scp_handle.await;
  for h in dispatcher_handles {
    let _ = h.await;
  }
  info!(log, "shutdown complete");
  0
}

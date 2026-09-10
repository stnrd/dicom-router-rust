//! Per-destination queue worker: scans the spool, forwards with retry,
//! dead-letters on exhaustion.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use slog::{error, info, o, warn, Logger};
use tokio_util::sync::CancellationToken;

use crate::config::{Destination, RetryConfig};
use crate::outbound_session::{OutboundSessionConfig, OutboundSessionHandle};
use crate::retry::Backoff;
use crate::{outbound_session, queue};

const RESCAN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Configuration for one per-destination dispatcher worker.
pub struct WorkerConfig {
  pub destination:          Destination,
  pub client_tls:           Arc<rustls::ClientConfig>,
  pub queue_root:           PathBuf,
  pub dead_letter_dir:      PathBuf,
  pub retry_cfg:            RetryConfig,
  pub calling_ae_title:     String,
  pub max_pdu_length:       u32,
  pub max_concurrent_sends: usize,
  pub log:                  Logger,
  pub shutdown:             CancellationToken,
}

pub fn spawn(config: WorkerConfig) -> tokio::task::JoinHandle<()> {
  tokio::spawn(async move {
    let WorkerConfig {
      destination,
      client_tls,
      queue_root,
      dead_letter_dir,
      retry_cfg,
      calling_ae_title,
      max_pdu_length,
      max_concurrent_sends: _,
      log,
      shutdown,
    } = config;

    let log = log.new(o!("destination" => destination.name.clone()));
    let dir = queue_root.join(&destination.name);
    if let Err(e) = tokio::task::spawn_blocking({
      let dir = dir.clone();
      move || std::fs::create_dir_all(&dir)
    })
    .await
    .expect("create_dir_all task panicked")
    {
      error!(log, "cannot create queue directory"; "dir" => %dir.display(), "error" => %e);
      return;
    }

    let session = outbound_session::spawn(OutboundSessionConfig {
      destination: destination.clone(),
      client_tls,
      calling_ae_title,
      max_pdu_length,
      log: log.clone(),
      shutdown: shutdown.clone(),
    });

    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
    let _watcher = {
      use notify::Watcher;
      let tx2 = tx.clone();
      notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if res.is_ok() {
          let _ = tx2.try_send(());
        }
      })
      .and_then(|mut w| {
        w.watch(&dir, notify::RecursiveMode::NonRecursive)?;
        Ok(w)
      })
      .map_err(|e| {
        warn!(
            log,
            "filesystem watcher unavailable, falling back to polling";
            "error" => %e
        )
      })
      .ok()
    };

    let mut interval = tokio::time::interval(RESCAN_INTERVAL);
    info!(log, "dispatcher started"; "queue_dir" => %dir.display());

    loop {
      tokio::select! {
          _ = shutdown.cancelled() => break,
          _ = interval.tick() => {}
          _ = rx.recv() => {}
      }

      process_pending(&dir, &dead_letter_dir, &retry_cfg, &session, &log, &shutdown).await;
    }

    session.shutdown().await;
    info!(log, "dispatcher stopped");
  })
}

async fn process_pending(
  dir: &Path,
  dead_letter_dir: &Path,
  retry_cfg: &RetryConfig,
  session: &OutboundSessionHandle,
  log: &Logger,
  shutdown: &CancellationToken,
) {
  let pending = match tokio::task::spawn_blocking({
    let dir = dir.to_path_buf();
    move || queue::scan(&dir)
  })
  .await
  .expect("scan task panicked")
  {
    Ok(p) => p,
    Err(e) => {
      error!(log, "queue scan failed"; "error" => %e);
      return;
    }
  };

  for spooled in pending {
    if shutdown.is_cancelled() {
      break;
    }
    if spooled.attempts >= retry_cfg.max_attempts {
      match tokio::task::spawn_blocking({
        let spooled = spooled.clone();
        let dead_letter_dir = dead_letter_dir.to_path_buf();
        move || queue::move_to_dead_letter(&spooled, &dead_letter_dir)
      })
      .await
      .expect("dead-letter task panicked")
      {
        Ok(()) => error!(
            log,
            "object dead-lettered after retries";
            "file" => %spooled.dcm_path.display(),
            "attempts" => spooled.attempts
        ),
        Err(e) => error!(log, "failed to dead-letter object"; "error" => %e),
      }
      continue;
    }

    let mut backoff = Backoff::new(retry_cfg);
    let delay = (0..spooled.attempts).filter_map(|_| backoff.next_delay()).last();
    if let Some(d) = delay {
      tokio::select! {
          _ = shutdown.cancelled() => break,
          _ = tokio::time::sleep(d) => {}
      }
    }

    if spooled.delivered {
      if let Err(e) = tokio::task::spawn_blocking(move || queue::acknowledge(&spooled))
        .await
        .expect("acknowledge task panicked")
      {
        error!(
            log,
            "delivered but failed to delete spool file";
            "error" => %e
        );
      }
      continue;
    }

    if let Err(e) = session.submit(spooled).await {
      warn!(log, "forward submit failed"; "error" => %e);
    }
  }
}

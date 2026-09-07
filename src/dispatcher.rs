//! Per-destination queue worker: scans the spool, forwards with retry,
//! dead-letters on exhaustion.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use slog::{error, info, o, warn, Logger};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::config::{Destination, RetryConfig};
use crate::retry::Backoff;
use crate::{queue, scu};

const RESCAN_INTERVAL: Duration = Duration::from_secs(5);

/// Configuration for one per-destination dispatcher worker.
pub struct WorkerConfig {
  pub destination:          Destination,
  pub client_tls:         Arc<rustls::ClientConfig>,
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
      max_concurrent_sends,
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

    let send_sem = Arc::new(Semaphore::new(max_concurrent_sends));
    let mut interval = tokio::time::interval(RESCAN_INTERVAL);
    info!(log, "dispatcher started"; "queue_dir" => %dir.display());

    loop {
      tokio::select! {
          _ = shutdown.cancelled() => break,
          _ = interval.tick() => {}
          _ = rx.recv() => {}
      }

      let pending = match tokio::task::spawn_blocking({
        let dir = dir.clone();
        move || queue::scan(&dir)
      })
      .await
      .expect("scan task panicked")
      {
        Ok(p) => p,
        Err(e) => {
          error!(log, "queue scan failed"; "error" => %e);
          continue;
        }
      };

      let mut sends = tokio::task::JoinSet::new();
      for spooled in pending {
        if shutdown.is_cancelled() {
          break;
        }
        if spooled.attempts >= retry_cfg.max_attempts {
          match tokio::task::spawn_blocking({
            let spooled = spooled.clone();
            let dead_letter_dir = dead_letter_dir.clone();
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

        let mut backoff = Backoff::new(&retry_cfg);
        let delay = (0..spooled.attempts).filter_map(|_| backoff.next_delay()).last();
        if let Some(d) = delay {
          tokio::select! {
              _ = shutdown.cancelled() => break,
              _ = tokio::time::sleep(d) => {}
          }
        }

        let permit = match send_sem.clone().acquire_owned().await {
          Ok(p) => p,
          Err(_) => break,
        };
        let dest = destination.clone();
        let tls = client_tls.clone();
        let ae = calling_ae_title.clone();
        let slog = log.clone();
        sends.spawn(async move {
          let _permit = permit;
          if spooled.delivered {
            if let Err(e) = tokio::task::spawn_blocking(move || queue::acknowledge(&spooled))
              .await
              .expect("acknowledge task panicked")
            {
              error!(
                  slog,
                  "delivered but failed to delete spool file";
                  "error" => %e
              );
            }
            return;
          }
          match scu::forward(&dest, tls, &spooled, &ae, max_pdu_length, &slog).await {
            Ok(()) => {
              if let Err(e) = tokio::task::spawn_blocking({
                let spooled = spooled.clone();
                move || queue::mark_delivered(&spooled)
              })
              .await
              .expect("mark_delivered task panicked")
              {
                error!(
                    slog,
                    "forwarded but failed to mark delivered";
                    "error" => %e
                );
              }
              if let Err(e) = tokio::task::spawn_blocking(move || queue::acknowledge(&spooled))
                .await
                .expect("acknowledge task panicked")
              {
                error!(
                    slog,
                    "forwarded but failed to delete spool file";
                    "error" => %e
                );
              }
            }
            Err(e) => {
              warn!(
                  slog,
                  "forward failed, will retry";
                  "file" => %spooled.dcm_path.display(),
                  "attempt" => spooled.attempts + 1,
                  "error" => %e
              );
              if let Err(e2) = tokio::task::spawn_blocking(move || queue::record_failure(&spooled, &e.to_string()))
                .await
                .expect("record_failure task panicked")
              {
                error!(slog, "failed to record retry state"; "error" => %e2);
              }
            }
          }
        });
      }
      while sends.join_next().await.is_some() {}
    }
    info!(log, "dispatcher stopped");
  })
}

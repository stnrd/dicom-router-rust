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

#[allow(clippy::too_many_arguments)]
pub fn spawn(
  destination: Destination,
  client_tls: Arc<rustls::ClientConfig>,
  queue_root: PathBuf,
  dead_letter_dir: PathBuf,
  retry_cfg: RetryConfig,
  calling_ae_title: String,
  max_pdu_length: u32,
  max_concurrent_sends: usize,
  log: Logger,
  shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
  tokio::spawn(async move {
    let log = log.new(o!("destination" => destination.name.clone()));
    let dir = queue_root.join(&destination.name);
    if let Err(e) = std::fs::create_dir_all(&dir) {
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

      let pending = match queue::scan(&dir) {
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
          match queue::move_to_dead_letter(&spooled, &dead_letter_dir) {
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
          match scu::forward(&dest, tls, &spooled, &ae, max_pdu_length, &slog).await {
            Ok(()) =>
              if let Err(e) = queue::acknowledge(&spooled) {
                error!(
                    slog,
                    "forwarded but failed to delete spool file";
                    "error" => %e
                );
              },
            Err(e) => {
              warn!(
                  slog,
                  "forward failed, will retry";
                  "file" => %spooled.dcm_path.display(),
                  "attempt" => spooled.attempts + 1,
                  "error" => %e
              );
              if let Err(e2) = queue::record_failure(&spooled, &e.to_string()) {
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

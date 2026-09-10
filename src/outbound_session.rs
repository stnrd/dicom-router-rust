//! Per-destination outbound session: one reused TLS association, many C-STOREs.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use dicom_ul::association::client::AsyncTlsStream;
use dicom_ul::association::AsyncClientAssociation;
use slog::{error, info, o, warn, Logger};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::config::Destination;
use crate::queue::SpooledObject;
use crate::scu::{self, ScuError, SpooledMeta};

const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const JOB_CHANNEL_SIZE: usize = 64;

pub struct OutboundSessionConfig {
  pub destination:      Destination,
  pub client_tls:       Arc<rustls::ClientConfig>,
  pub calling_ae_title: String,
  pub max_pdu_length:   u32,
  pub log:              Logger,
  pub shutdown:         CancellationToken,
}

struct SessionJob {
  spooled: SpooledObject,
  done:    oneshot::Sender<Result<(), ScuError>>,
}

struct SessionState {
  assoc:      Option<AsyncClientAssociation<AsyncTlsStream>>,
  negotiated: HashSet<(String, String)>,
}

impl SessionState {
  fn new() -> Self {
    Self {
      assoc:      None,
      negotiated: HashSet::new(),
    }
  }
}

pub struct OutboundSessionHandle {
  tx:   mpsc::Sender<SessionJob>,
  task: tokio::task::JoinHandle<()>,
}

impl OutboundSessionHandle {
  pub async fn submit(&self, spooled: SpooledObject) -> Result<(), ScuError> {
    let (done_tx, done_rx) = oneshot::channel();
    self
      .tx
      .send(SessionJob { spooled, done: done_tx })
      .await
      .map_err(|_| ScuError::SessionUnavailable {
        reason: "outbound session stopped".into(),
      })?;
    done_rx.await.unwrap_or_else(|_| {
      Err(ScuError::SessionUnavailable {
        reason: "session dropped response".into(),
      })
    })
  }

  pub async fn shutdown(self) { self.task.await.ok(); }
}

pub fn spawn(config: OutboundSessionConfig) -> OutboundSessionHandle {
  let (tx, rx) = mpsc::channel(JOB_CHANNEL_SIZE);
  let task = tokio::spawn(run(config, rx));
  OutboundSessionHandle { tx, task }
}

async fn run(config: OutboundSessionConfig, mut rx: mpsc::Receiver<SessionJob>) {
  let log = config.log.new(o!("component" => "outbound_session"));
  let mut session = SessionState::new();

  loop {
    if config.shutdown.is_cancelled() {
      break;
    }

    match tokio::time::timeout(IDLE_TIMEOUT, rx.recv()).await {
      Ok(Some(job)) => {
        let result = process_job(&config, &log, &mut session, job.spooled).await;
        let _ = job.done.send(result);
      }
      Ok(None) => break,
      Err(_) =>
        if session.assoc.is_some() {
          disconnect(&mut session, &log).await;
          info!(log, "outbound association released (idle)");
        },
    }
  }

  disconnect(&mut session, &log).await;
  info!(log, "outbound session stopped");
}

async fn process_job(
  config: &OutboundSessionConfig,
  log: &Logger,
  session: &mut SessionState,
  spooled: SpooledObject,
) -> Result<(), ScuError> {
  let meta = scu::read_spooled_meta(&spooled.dcm_path)?;
  ensure_connected(config, log, session, &meta).await?;

  let file = dicom_object::open_file(&spooled.dcm_path).map_err(|e| ScuError::ReadFile {
    path:   spooled.dcm_path.clone(),
    source: Box::new(e),
  })?;

  let assoc = session.assoc.as_mut().ok_or_else(|| ScuError::SessionUnavailable {
    reason: "not connected".into(),
  })?;

  let attempt = spooled.attempts + 1;
  match scu::send_object(assoc, &file, &meta, log).await {
    Ok(()) => {
      if let Err(e) = tokio::task::spawn_blocking({
        let spooled = spooled.clone();
        move || crate::queue::mark_delivered(&spooled)
      })
      .await
      .expect("mark_delivered task panicked")
      {
        error!(log, "forwarded but failed to mark delivered"; "error" => %e);
      }
      if let Err(e) = tokio::task::spawn_blocking(move || crate::queue::acknowledge(&spooled))
        .await
        .expect("acknowledge task panicked")
      {
        error!(log, "forwarded but failed to delete spool file"; "error" => %e);
      }
      info!(
          log,
          "object forwarded";
          "destination" => &config.destination.name,
          "sop_instance_uid" => &meta.sop_instance_uid,
          "attempt" => attempt
      );
      Ok(())
    }
    Err(e) => {
      warn!(
          log,
          "forward failed, will retry";
          "file" => %spooled.dcm_path.display(),
          "attempt" => attempt,
          "error" => %e
      );
      disconnect(session, log).await;
      let err_msg = e.to_string();
      if let Err(e2) = tokio::task::spawn_blocking(move || crate::queue::record_failure(&spooled, &err_msg))
        .await
        .expect("record_failure task panicked")
      {
        error!(log, "failed to record retry state"; "error" => %e2);
      }
      Err(e)
    }
  }
}

async fn ensure_connected(
  config: &OutboundSessionConfig,
  log: &Logger,
  session: &mut SessionState,
  meta: &SpooledMeta,
) -> Result<(), ScuError> {
  let key = (meta.sop_class_uid.clone(), meta.transfer_syntax.clone());
  if session.assoc.is_some() && session.negotiated.contains(&key) {
    return Ok(());
  }

  let pcs = if session.negotiated.is_empty() {
    scu::presentation_keys_for_meta(meta)
  } else {
    scu::merge_presentation_keys(&session.negotiated, meta)?
  };

  reconnect(config, log, session, pcs).await
}

async fn reconnect(
  config: &OutboundSessionConfig,
  log: &Logger,
  session: &mut SessionState,
  pcs: Vec<scu::PresentationKey>,
) -> Result<(), ScuError> {
  disconnect(session, log).await;
  let assoc = scu::connect(
    &config.destination,
    config.client_tls.clone(),
    &config.calling_ae_title,
    config.max_pdu_length,
    &pcs,
  )
  .await?;
  session.negotiated = pcs
    .iter()
    .map(|pc| (pc.abstract_syntax.clone(), pc.transfer_syntax.clone()))
    .collect();
  session.assoc = Some(assoc);
  info!(
      log,
      "outbound association established";
      "destination" => &config.destination.name,
      "presentation_contexts" => session.negotiated.len()
  );
  Ok(())
}

async fn disconnect(session: &mut SessionState, log: &Logger) {
  if let Some(assoc) = session.assoc.take() {
    if let Err(e) = scu::release(assoc).await {
      warn!(log, "error releasing outbound association"; "error" => %e);
    }
  }
  session.negotiated.clear();
}

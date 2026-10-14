//! Router configuration, loaded from YAML (Kubernetes ConfigMap friendly).

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use snafu::Snafu;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
  /// Address to listen on for inbound DICOM TLS associations, e.g.
  /// "0.0.0.0:2762".
  pub listen_addr:                 String,
  /// This router's AE title (max 16 chars, DICOM AE rules).
  pub ae_title:                    String,
  /// Maximum PDU length accepted/requested.
  #[serde(default = "default_max_pdu_length")]
  pub max_pdu_length:              u32,
  /// Root spool directory; each destination gets a subdirectory.
  pub queue_dir:                   PathBuf,
  /// Directory for objects that exhausted retries.
  #[serde(default = "default_dead_letter_dir")]
  pub dead_letter_dir:             PathBuf,
  /// Max simultaneous inbound associations.
  #[serde(default = "default_max_concurrent_associations")]
  pub max_concurrent_associations: usize,
  /// Max simultaneous outbound sends per destination.
  #[serde(default = "default_max_concurrent_sends")]
  pub max_concurrent_sends:        usize,
  /// Accept unknown storage SOP classes (promiscuous mode). Default false.
  #[serde(default)]
  pub promiscuous:                 bool,
  /// Log level: critical|error|warn|info|debug|trace. Default info.
  #[serde(default = "default_log_level")]
  pub log_level:                   String,
  /// Minimum free bytes in the queue filesystem before refusing C-STOREs.
  #[serde(default = "default_min_free_bytes")]
  pub min_free_bytes:              u64,
  #[serde(default)]
  pub retry:                       RetryConfig,
  /// Inbound TLS settings. When `tls.enabled` is false, associations are plain
  /// DICOM.
  pub tls:                         ServerTls,
  /// Forwarding destinations (at least one).
  pub destinations:                Vec<Destination>,
  /// When set, inbound C-FIND / C-GET (Patient Root and Study Root) are
  /// proxied to this destination (must match `destinations[].name`).
  #[serde(default)]
  pub query_retrieve:              Option<QueryRetrieveConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerTls {
  /// When false, inbound associations use cleartext DICOM (typical port 104).
  #[serde(default = "default_true")]
  pub enabled:     bool,
  /// PEM certificate chain presented to inbound clients (required when
  /// `enabled`).
  #[serde(default)]
  pub server_cert: Option<PathBuf>,
  /// PEM private key for the server certificate (required when `enabled`).
  #[serde(default)]
  pub server_key:  Option<PathBuf>,
  /// Optional CA bundle: when set, inbound clients MUST present a client
  /// certificate signed by one of these CAs (mutual TLS).
  pub client_ca:   Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Destination {
  /// Unique name; used for the queue subdirectory and log key-values.
  pub name:             String,
  /// Called AE title to request at the destination.
  pub ae_title:         String,
  /// Destination host (DNS name or IP).
  pub host:             String,
  /// Destination port (2762 for DICOM TLS, 104 for cleartext).
  pub port:             u16,
  /// When false, outbound associations use cleartext DICOM.
  #[serde(default = "default_true")]
  pub tls:              bool,
  /// TLS server name (SNI / certificate verification). Defaults to `host`.
  pub server_name:      Option<String>,
  /// PEM CA bundle used to verify the destination's certificate (required when
  /// `tls`).
  #[serde(default)]
  pub ca_cert:          Option<PathBuf>,
  /// Optional client certificate/key for destinations requiring mTLS.
  pub client_cert:      Option<PathBuf>,
  pub client_key:       Option<PathBuf>,
  /// If non-empty, only objects from these calling AE titles are forwarded
  /// to this destination. Empty = forward everything (fan-out).
  #[serde(default)]
  pub source_ae_titles: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryRetrieveConfig {
  /// `destinations[].name` used for C-FIND / C-GET outbound.
  pub destination:       String,
  /// Calling AE titles allowed to query/retrieve. Required and non-empty:
  /// the proxy exposes the destination's archive, so access is opt-in.
  #[serde(default)]
  pub allowed_ae_titles: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetryConfig {
  #[serde(default = "default_initial_delay_ms")]
  pub initial_delay_ms: u64,
  #[serde(default = "default_max_delay_ms")]
  pub max_delay_ms:     u64,
  #[serde(default = "default_multiplier")]
  pub multiplier:       f64,
  #[serde(default = "default_max_attempts")]
  pub max_attempts:     u32,
}

impl Default for RetryConfig {
  fn default() -> Self {
    Self {
      initial_delay_ms: default_initial_delay_ms(),
      max_delay_ms:     default_max_delay_ms(),
      multiplier:       default_multiplier(),
      max_attempts:     default_max_attempts(),
    }
  }
}

fn default_true() -> bool { true }
fn default_max_pdu_length() -> u32 { 131_072 }
fn default_dead_letter_dir() -> PathBuf { PathBuf::from("/var/lib/dicom-router/dead-letter") }
fn default_max_concurrent_associations() -> usize { 32 }
fn default_max_concurrent_sends() -> usize { 4 }
fn default_log_level() -> String { "info".to_string() }
fn default_min_free_bytes() -> u64 { 512 * 1024 * 1024 }
fn default_initial_delay_ms() -> u64 { 1_000 }
fn default_max_delay_ms() -> u64 { 60_000 }
fn default_multiplier() -> f64 { 2.0 }
fn default_max_attempts() -> u32 { 10 }

#[derive(Debug, Snafu)]
pub enum ConfigError {
  #[snafu(display("could not read config file {}: {source}", path.display()))]
  ReadFile { path: PathBuf, source: std::io::Error },
  #[snafu(display("invalid YAML in config file: {source}"))]
  Parse { source: serde_yaml::Error },
  #[snafu(display("invalid configuration: {reason}"))]
  Invalid { reason: String },
}

impl Config {
  pub fn load(path: &Path) -> Result<Self, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|e| ConfigError::ReadFile {
      path:   path.to_path_buf(),
      source: e,
    })?;
    let cfg: Config = serde_yaml::from_str(&text).map_err(|e| ConfigError::Parse { source: e })?;
    cfg.validate()?;
    Ok(cfg)
  }

  pub fn validate(&self) -> Result<(), ConfigError> {
    let invalid = |reason: String| ConfigError::Invalid { reason };
    if self.ae_title.is_empty() || self.ae_title.len() > 16 {
      return Err(invalid(format!("ae_title {:?} must be 1..=16 chars", self.ae_title)));
    }
    if self.destinations.is_empty() {
      return Err(invalid("at least one destination is required".into()));
    }
    if self.tls.enabled {
      if self.tls.server_cert.is_none() {
        return Err(invalid("tls.server_cert is required when tls.enabled is true".into()));
      }
      if self.tls.server_key.is_none() {
        return Err(invalid("tls.server_key is required when tls.enabled is true".into()));
      }
    } else if self.tls.client_ca.is_some() {
      return Err(invalid("tls.client_ca requires tls.enabled to be true".into()));
    }
    let mut names = HashSet::new();
    for d in &self.destinations {
      if !names.insert(d.name.clone()) {
        return Err(invalid(format!("duplicate destination name {:?}", d.name)));
      }
      if d.ae_title.is_empty() || d.ae_title.len() > 16 {
        return Err(invalid(format!(
          "destination {:?}: ae_title must be 1..=16 chars",
          d.name
        )));
      }
      if d.tls && d.ca_cert.is_none() {
        return Err(invalid(format!(
          "destination {:?}: ca_cert is required when tls is true",
          d.name
        )));
      }
      if !d.tls && (d.client_cert.is_some() || d.client_key.is_some()) {
        return Err(invalid(format!(
          "destination {:?}: client_cert/client_key require tls: true",
          d.name
        )));
      }
      if (d.client_cert.is_some()) != (d.client_key.is_some()) {
        return Err(invalid(format!(
          "destination {:?}: client_cert and client_key must be set together",
          d.name
        )));
      }
    }
    if let Some(qr) = &self.query_retrieve {
      if qr.destination.is_empty() {
        return Err(invalid("query_retrieve.destination must not be empty".into()));
      }
      if qr.allowed_ae_titles.is_empty() {
        return Err(invalid(
          "query_retrieve.allowed_ae_titles must list the calling AE titles allowed to query".into(),
        ));
      }
      if !self.destinations.iter().any(|d| d.name == qr.destination) {
        return Err(invalid(format!(
          "query_retrieve.destination {:?} does not match any destinations[].name",
          qr.destination
        )));
      }
    }
    if self.max_pdu_length < 1_018 {
      return Err(invalid("max_pdu_length must be >= 1018".into()));
    }
    if self.max_concurrent_associations == 0 {
      return Err(invalid("max_concurrent_associations must be >= 1".into()));
    }
    if self.max_concurrent_sends == 0 {
      return Err(invalid("max_concurrent_sends must be >= 1".into()));
    }
    if self.retry.max_attempts == 0 {
      return Err(invalid("retry.max_attempts must be >= 1".into()));
    }
    if self.retry.multiplier <= 0.0 {
      return Err(invalid("retry.multiplier must be > 0".into()));
    }
    if self.retry.initial_delay_ms == 0 {
      return Err(invalid("retry.initial_delay_ms must be >= 1".into()));
    }
    if self.retry.max_delay_ms < self.retry.initial_delay_ms {
      return Err(invalid("retry.max_delay_ms must be >= retry.initial_delay_ms".into()));
    }
    if crate::logging::parse_level(&self.log_level).is_none() {
      return Err(invalid(format!("invalid log_level {:?}", self.log_level)));
    }
    Ok(())
  }

  /// Queue directory for a destination.
  pub fn queue_dir_for(&self, destination: &str) -> PathBuf { self.queue_dir.join(destination) }

  /// Destinations accepting objects from the given calling AE title.
  pub fn destinations_for_source(&self, calling_ae: &str) -> Vec<&Destination> {
    self
      .destinations
      .iter()
      .filter(|d| d.source_ae_titles.is_empty() || d.source_ae_titles.iter().any(|t| t == calling_ae))
      .collect()
  }

  pub fn qr_destination(&self) -> Option<&Destination> {
    let name = &self.query_retrieve.as_ref()?.destination;
    self.destinations.iter().find(|d| d.name == *name)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  const VALID_YAML: &str = r#"
listen_addr: "0.0.0.0:2762"
ae_title: "ROUTER"
queue_dir: "/var/lib/dicom-router/queue"
tls:
  server_cert: "tests/certs/server.crt"
  server_key: "tests/certs/server.key"
destinations:
  - name: pacs-main
    ae_title: "PACS_MAIN"
    host: pacs.dicom.svc
    port: 2762
    ca_cert: "tests/certs/ca.crt"
  - name: pacs-backup
    ae_title: "PACS_BKP"
    host: pacs2.dicom.svc
    port: 2762
    ca_cert: "tests/certs/ca.crt"
    source_ae_titles: ["MODALITY_1"]
"#;

  #[test]
  fn parses_valid_config_with_n_destinations() {
    let cfg: Config = serde_yaml::from_str(VALID_YAML).unwrap();
    assert_eq!(cfg.ae_title, "ROUTER");
    assert_eq!(cfg.destinations.len(), 2);
    assert_eq!(cfg.max_pdu_length, 131_072);
    assert_eq!(cfg.retry.max_attempts, 10);
    assert_eq!(cfg.destinations[1].source_ae_titles, vec!["MODALITY_1"]);
    assert!(cfg.destinations[0].source_ae_titles.is_empty());
  }

  #[test]
  fn rejects_invalid_ae_title() {
    let yaml = VALID_YAML.replace("ae_title: \"ROUTER\"", "ae_title: \"THIS_IS_WAY_TOO_LONG_AN_AE_TITLE\"");
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    assert!(cfg.validate().is_err());
  }

  #[test]
  fn rejects_duplicate_destination_names() {
    let yaml = VALID_YAML.replace("name: pacs-backup", "name: pacs-main");
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    assert!(cfg.validate().is_err());
  }

  #[test]
  fn rejects_empty_destinations() {
    let yaml = r#"
listen_addr: "0.0.0.0:2762"
ae_title: "ROUTER"
queue_dir: "/var/lib/dicom-router/queue"
tls:
  server_cert: "tests/certs/server.crt"
  server_key: "tests/certs/server.key"
destinations: []
"#;
    let cfg: Config = serde_yaml::from_str(yaml).unwrap();
    assert!(cfg.validate().is_err());
  }

  #[test]
  fn rejects_invalid_log_level() {
    let yaml = VALID_YAML.replace("listen_addr:", "log_level: \"warining\"\nlisten_addr:");
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    assert!(cfg.validate().is_err());
  }

  #[test]
  fn rejects_dest_ae_title_too_long() {
    let yaml = VALID_YAML.replace("ae_title: \"PACS_MAIN\"", "ae_title: \"THIS_IS_WAY_TOO_LONG\"");
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    assert!(cfg.validate().is_err());
  }

  #[test]
  fn rejects_client_cert_without_key() {
    let yaml = VALID_YAML.replace(
      "    ca_cert: \"tests/certs/ca.crt\"\n  - name: pacs-backup",
      "    ca_cert: \"tests/certs/ca.crt\"\n    client_cert: \"tests/certs/client.crt\"\n  - name: pacs-backup",
    );
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    assert!(cfg.validate().is_err());
  }

  #[test]
  fn rejects_max_pdu_below_minimum() {
    let yaml = VALID_YAML.replace("listen_addr:", "max_pdu_length: 1017\nlisten_addr:");
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    assert!(cfg.validate().is_err());
  }

  #[test]
  fn accepts_max_pdu_at_minimum() {
    let yaml = VALID_YAML.replace("listen_addr:", "max_pdu_length: 1018\nlisten_addr:");
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    assert!(cfg.validate().is_ok());
  }

  #[test]
  fn rejects_zero_max_concurrent_associations() {
    let yaml = VALID_YAML.replace("listen_addr:", "max_concurrent_associations: 0\nlisten_addr:");
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    assert!(cfg.validate().is_err());
  }

  #[test]
  fn rejects_zero_max_concurrent_sends() {
    let yaml = VALID_YAML.replace("listen_addr:", "max_concurrent_sends: 0\nlisten_addr:");
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    assert!(cfg.validate().is_err());
  }

  #[test]
  fn rejects_zero_retry_attempts() {
    let yaml = VALID_YAML.replace("listen_addr:", "retry:\n  max_attempts: 0\nlisten_addr:");
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    assert!(cfg.validate().is_err());
  }

  #[test]
  fn rejects_non_positive_retry_multiplier() {
    let yaml = VALID_YAML.replace("listen_addr:", "retry:\n  multiplier: 0\nlisten_addr:");
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    assert!(cfg.validate().is_err());
  }

  #[test]
  fn rejects_retry_max_delay_below_initial() {
    let yaml = VALID_YAML.replace(
      "listen_addr:",
      "retry:\n  initial_delay_ms: 5000\n  max_delay_ms: 1000\nlisten_addr:",
    );
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    assert!(cfg.validate().is_err());
  }

  #[test]
  fn load_rejects_malformed_yaml() {
    let dir = std::env::temp_dir().join("dicom-router-config-test");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("bad.yaml");
    std::fs::write(&path, "listen_addr: [\n").unwrap();
    let err = Config::load(&path).unwrap_err();
    assert!(err.to_string().contains("invalid YAML"));
  }

  #[test]
  fn routes_by_source_ae_with_fanout_default() {
    let cfg: Config = serde_yaml::from_str(VALID_YAML).unwrap();
    let for_mod1: Vec<_> = cfg
      .destinations_for_source("MODALITY_1")
      .iter()
      .map(|d| d.name.as_str())
      .collect();
    assert_eq!(for_mod1, vec!["pacs-main", "pacs-backup"]);
    let for_other: Vec<_> = cfg
      .destinations_for_source("OTHER")
      .iter()
      .map(|d| d.name.as_str())
      .collect();
    assert_eq!(for_other, vec!["pacs-main"]);
  }

  #[test]
  fn qr_destination_resolves_named_dest() {
    let yaml =
      VALID_YAML.to_string() + "query_retrieve:\n  destination: pacs-main\n  allowed_ae_titles: [\"VIEWER\"]\n";
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    cfg.validate().unwrap();
    assert_eq!(cfg.qr_destination().unwrap().name, "pacs-main");
  }

  #[test]
  fn qr_destination_unknown_name_is_invalid() {
    let yaml =
      VALID_YAML.to_string() + "query_retrieve:\n  destination: no-such-dest\n  allowed_ae_titles: [\"VIEWER\"]\n";
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("query_retrieve.destination"));
  }

  #[test]
  fn qr_requires_allowed_ae_titles() {
    let yaml = VALID_YAML.to_string() + "query_retrieve:\n  destination: pacs-main\n";
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("allowed_ae_titles"));
  }

  #[test]
  fn qr_omitted_means_no_proxy() {
    let cfg: Config = serde_yaml::from_str(VALID_YAML).unwrap();
    cfg.validate().unwrap();
    assert!(cfg.qr_destination().is_none());
  }

  #[test]
  fn inbound_cleartext_requires_no_server_cert() {
    let yaml = r#"
listen_addr: "127.0.0.1:11104"
ae_title: "ROUTER"
queue_dir: "/tmp/q"
tls:
  enabled: false
destinations:
  - name: orthanc
    ae_title: "ORTHANC"
    host: 127.0.0.1
    port: 4242
    tls: false
"#;
    let cfg: Config = serde_yaml::from_str(yaml).unwrap();
    cfg.validate().unwrap();
    assert!(!cfg.tls.enabled);
    assert!(!cfg.destinations[0].tls);
  }

  #[test]
  fn cleartext_destination_rejects_client_cert() {
    let yaml = r#"
listen_addr: "127.0.0.1:11104"
ae_title: "ROUTER"
queue_dir: "/tmp/q"
tls:
  enabled: false
destinations:
  - name: orthanc
    ae_title: "ORTHANC"
    host: 127.0.0.1
    port: 4242
    tls: false
    client_cert: "tests/certs/client.crt"
    client_key: "tests/certs/client.key"
"#;
    let cfg: Config = serde_yaml::from_str(yaml).unwrap();
    assert!(cfg.validate().is_err());
  }
}

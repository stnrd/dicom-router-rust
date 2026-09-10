use clap::Parser;

#[derive(Debug, Parser)]
#[command(version, about = "Secure DICOM C-STORE router (TLS in, TLS out)")]
struct Cli {
  /// Path to YAML configuration file
  #[arg(short, long, default_value = "/etc/dicom-router/config.yaml")]
  config: std::path::PathBuf,
}

fn main() {
  let cli = Cli::parse();
  std::process::exit(dicom_router::run(cli.config));
}

use clap::Parser;

/// Galactica Control Plane -- cluster state, scheduling, placement, artifact service
#[derive(Parser, Debug)]
#[command(name = "galactica-control-plane", version, about)]
struct Cli {
    /// Listen address for the gRPC server
    #[arg(long, default_value = "0.0.0.0:9090")]
    listen_addr: String,

    /// Path to configuration file
    #[arg(long, short)]
    config: Option<String>,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    galactica_observability::init_tracing("galactica-control-plane", &cli.log_level);

    let addr = cli.listen_addr.parse()?;
    galactica_control_plane::server::run_server(addr).await
}

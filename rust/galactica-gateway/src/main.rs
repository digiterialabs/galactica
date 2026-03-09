use clap::Parser;

/// Galactica Gateway — OpenAI-compatible HTTP API + gRPC server
#[derive(Parser, Debug)]
#[command(name = "galactica-gateway", version, about)]
struct Cli {
    /// Listen address for the HTTP server
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen_addr: String,

    /// Control plane address to connect to
    #[arg(long, default_value = "http://localhost:9090")]
    control_plane_addr: String,

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

    galactica_observability::init_tracing("galactica-gateway", &cli.log_level);

    let state = galactica_gateway::server::AppState {
        control_plane_addr: cli.control_plane_addr,
    };

    let addr = cli.listen_addr.parse()?;
    galactica_gateway::server::run_http_server(addr, state).await
}

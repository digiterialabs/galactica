use clap::Parser;

/// Galactica Gateway — OpenAI-compatible HTTP API + gRPC server
#[derive(Parser, Debug)]
#[command(name = "galactica-gateway", version, about)]
struct Cli {
    /// Listen address for the HTTP/gRPC server
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen_addr: String,

    /// Control plane address to connect to
    #[arg(long, default_value = "http://localhost:9090")]
    control_plane_addr: String,

    /// Path to configuration file
    #[arg(long, short)]
    config: Option<String>,
}

fn main() {
    let cli = Cli::parse();
    println!("galactica-gateway starting on {}", cli.listen_addr);
}

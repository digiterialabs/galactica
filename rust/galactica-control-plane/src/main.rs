use clap::Parser;

/// Galactica Control Plane — cluster state, scheduling, placement, artifact service
#[derive(Parser, Debug)]
#[command(name = "galactica-control-plane", version, about)]
struct Cli {
    /// Listen address for the gRPC server
    #[arg(long, default_value = "0.0.0.0:9090")]
    listen_addr: String,

    /// Path to configuration file
    #[arg(long, short)]
    config: Option<String>,
}

fn main() {
    let cli = Cli::parse();
    println!("galactica-control-plane starting on {}", cli.listen_addr);
}

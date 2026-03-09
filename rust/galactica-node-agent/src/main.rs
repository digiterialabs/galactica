use clap::Parser;

/// Galactica Node Agent — per-machine hardware detection, process supervision, runtime management
#[derive(Parser, Debug)]
#[command(name = "galactica-node-agent", version, about)]
struct Cli {
    /// Control plane address to connect to
    #[arg(long, default_value = "http://localhost:9090")]
    control_plane_addr: String,

    /// Path to configuration file
    #[arg(long, short)]
    config: Option<String>,
}

fn main() {
    let cli = Cli::parse();
    println!(
        "galactica-node-agent connecting to {}",
        cli.control_plane_addr
    );
}

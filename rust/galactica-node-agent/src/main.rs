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

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    galactica_observability::init_tracing("galactica-node-agent", &cli.log_level);

    tracing::info!(
        control_plane = %cli.control_plane_addr,
        "galactica-node-agent starting"
    );

    let config = galactica_common::types::config::NodeAgentConfig {
        control_plane_addr: cli.control_plane_addr,
        ..Default::default()
    };

    let agent = galactica_node_agent::agent::NodeAgent::new(config);
    agent.run().await?;

    Ok(())
}

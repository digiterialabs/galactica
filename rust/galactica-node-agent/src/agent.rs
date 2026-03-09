use galactica_common::types::config::NodeAgentConfig;

pub struct NodeAgent {
    config: NodeAgentConfig,
}

impl NodeAgent {
    pub fn new(config: NodeAgentConfig) -> Self {
        Self { config }
    }

    /// Main agent loop: register, heartbeat, process commands.
    pub async fn run(&self) -> galactica_common::Result<()> {
        tracing::info!(
            control_plane = %self.config.control_plane_addr,
            "node agent starting"
        );

        // 1. Detect hardware
        // 2. Register with control plane
        // 3. Start heartbeat loop
        // 4. Listen for commands

        todo!("implement node agent main loop")
    }
}

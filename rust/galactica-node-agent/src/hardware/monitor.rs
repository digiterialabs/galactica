use std::time::Duration;

/// Periodically reports hardware utilization to the control plane.
pub struct HardwareMonitor {
    interval: Duration,
}

impl HardwareMonitor {
    pub fn new(interval: Duration) -> Self {
        Self { interval }
    }

    /// Start monitoring in a background task.
    pub async fn start(&self) -> galactica_common::Result<()> {
        tracing::info!(interval = ?self.interval, "starting hardware monitor");
        todo!("implement periodic hardware reporting")
    }
}

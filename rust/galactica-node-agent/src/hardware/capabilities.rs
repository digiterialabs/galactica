use std::collections::HashMap;

use galactica_common::types::node::{Locality, NetworkProfile, NodeCapabilities};

use super::detect::HardwareDetector;

/// Build NodeCapabilities from detected hardware.
pub async fn build_capabilities(detector: &dyn HardwareDetector) -> NodeCapabilities {
    let os = detector.detect_os().await;
    let cpu_arch = detector.detect_cpu_arch().await;
    let memory = detector.detect_memory().await;
    let accelerators = detector.detect_accelerators().await;

    NodeCapabilities {
        os,
        cpu_arch,
        accelerators,
        system_memory: memory,
        network_profile: NetworkProfile::Lan,
        runtime_backends: Vec::new(),
        locality: Locality {
            labels: HashMap::new(),
        },
    }
}

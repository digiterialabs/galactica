use galactica_common::events::command::Command;
use galactica_common::types::cluster::ClusterState;

/// Generate commands from the current state using a short-circuit OR chain.
///
/// Each planner function returns Some(commands) if it has work to do.
/// The first planner that returns commands wins.
pub fn plan(_state: &ClusterState) -> Vec<Command> {
    // Short-circuit OR chain:
    // 1. Check for nodes to drain
    // 2. Check for models to place
    // 3. Check for stale instances to unload
    // ... etc.

    Vec::new()
}

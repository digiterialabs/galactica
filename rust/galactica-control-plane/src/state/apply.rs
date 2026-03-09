use galactica_common::events::event::Event;
use galactica_common::types::cluster::ClusterState;

/// Pure function: apply an event to produce a new cluster state.
pub fn event_apply(event: &Event, state: &ClusterState) -> ClusterState {
    let new_state = state.clone();

    match event {
        Event::NodeRegistered { .. } => {
            // Will add node to state
        }
        Event::NodeHeartbeat { .. } => {
            // Will update last-seen
        }
        Event::NodeTimedOut { .. } => {
            // Will mark node offline
        }
        Event::InstanceCreated { .. } => {
            // Will add instance
        }
        Event::InstanceDeleted { .. } => {
            // Will remove instance
        }
        Event::InstanceStatusChanged { .. } => {
            // Will update instance status
        }
        Event::TaskCreated { .. }
        | Event::TaskStatusChanged { .. }
        | Event::DownloadProgress { .. } => {
            // Handled elsewhere
        }
    }

    new_state
}

// Galactica networking: discovery, transport, and peer management.

pub mod discovery;
pub mod peer;
pub mod transport;

pub use discovery::bootstrap::BootstrapDiscovery;
pub use discovery::mdns::MdnsDiscovery;
pub use peer::manager::PeerManager;
pub use transport::tls::TlsConfig;

// Galactica networking: mDNS discovery, TLS/mTLS, peer management.

pub mod bootstrap;
pub mod discovery;

pub use bootstrap::{
    BootstrapClient, BootstrapDiscovery, BootstrapDiscoveryConfig, BootstrapRegistration,
    DiscoveryCoordinator, NatTraversalMode, TlsTransportConfig,
};
pub use discovery::{
    DiscoverySource, MdnsDiscovery, MdnsDiscoveryConfig, MdnsEvent, MdnsEventSource,
    MdnsPeerAdvertisement, PeerEvent, PeerInfo, PeerManager, PeerRole,
};

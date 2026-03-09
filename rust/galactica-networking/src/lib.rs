// Galactica networking: mDNS discovery, TLS/mTLS, peer management.

pub mod bootstrap;
pub mod discovery;
pub mod tls;

pub use bootstrap::{
    BootstrapClient, BootstrapDiscovery, BootstrapDiscoveryConfig, BootstrapRegistration,
    DiscoveryCoordinator, NatTraversalMode, TlsTransportConfig,
};
pub use discovery::{
    DiscoverySource, MdnsDiscovery, MdnsDiscoveryConfig, MdnsEvent, MdnsEventSource,
    MdnsPeerAdvertisement, PeerEvent, PeerInfo, PeerManager, PeerRole,
};
pub use tls::{
    IssuedTlsIdentity, fingerprint_certificate, issue_tls_identity, verify_certificate_fingerprint,
};

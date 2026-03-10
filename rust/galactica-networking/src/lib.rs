// Galactica networking: mDNS discovery, TLS/mTLS, peer management.

pub mod bootstrap;
pub mod discovery;
pub mod endpoints;
pub mod tls;

pub use bootstrap::{
    BootstrapClient, BootstrapDiscovery, BootstrapDiscoveryConfig, BootstrapRegistration,
    DiscoveryCoordinator, NatTraversalMode, TlsTransportConfig,
};
pub use discovery::{
    DiscoverySource, MdnsAnnouncementHandle, MdnsDiscovery, MdnsDiscoveryConfig, MdnsEvent,
    MdnsEventSource, MdnsPeerAdvertisement, PeerEvent, PeerInfo, PeerManager, PeerRole,
    discover_mdns_control_plane_endpoints, spawn_control_plane_mdns_advertiser,
    spawn_mdns_advertiser,
};
pub use endpoints::{
    detect_bind_endpoints, detect_lan_scan_control_plane_endpoints,
    detect_tailscale_peer_control_plane_endpoints, endpoint_from_url, loopback_endpoint,
    preferred_endpoint,
};
pub use tls::{
    IssuedTlsIdentity, fingerprint_certificate, issue_tls_identity, verify_certificate_fingerprint,
};

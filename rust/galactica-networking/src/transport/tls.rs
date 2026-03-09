use std::path::PathBuf;

/// TLS/mTLS configuration for secure node-to-node and node-to-control-plane communication.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_cert_path: Option<PathBuf>,
    pub require_client_auth: bool,
}

impl TlsConfig {
    pub fn new(cert_path: PathBuf, key_path: PathBuf) -> Self {
        Self {
            cert_path,
            key_path,
            ca_cert_path: None,
            require_client_auth: false,
        }
    }

    /// Enable mutual TLS by requiring client certificates.
    pub fn with_mtls(mut self, ca_cert_path: PathBuf) -> Self {
        self.ca_cert_path = Some(ca_cert_path);
        self.require_client_auth = true;
        self
    }

    /// Configure TLS for a tonic gRPC server.
    pub fn configure_server_tls(&self) -> galactica_common::Result<()> {
        tracing::info!(cert = ?self.cert_path, "configuring server TLS");
        todo!("implement server TLS configuration with tonic")
    }

    /// Configure TLS for a tonic gRPC client.
    pub fn configure_client_tls(&self) -> galactica_common::Result<()> {
        tracing::info!(cert = ?self.cert_path, "configuring client TLS");
        todo!("implement client TLS configuration with tonic")
    }
}

use chrono::{DateTime, Duration, Utc};
use galactica_common::{GalacticaError, Result};
use rcgen::{CertificateParams, DnType, KeyPair};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedTlsIdentity {
    pub certificate_pem: String,
    pub private_key_pem: String,
    pub fingerprint: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

pub fn issue_tls_identity(
    node_id: &str,
    hostname: &str,
    _secret: &str,
    validity: Duration,
) -> Result<IssuedTlsIdentity> {
    if validity <= Duration::zero() {
        return Err(GalacticaError::invalid_argument(
            "certificate validity must be positive",
        ));
    }

    let issued_at = Utc::now();
    let expires_at = issued_at.checked_add_signed(validity).ok_or_else(|| {
        GalacticaError::invalid_argument("certificate validity exceeds supported range")
    })?;

    let mut params = CertificateParams::new(vec![hostname.to_string(), node_id.to_string()])
        .map_err(|error| {
            GalacticaError::internal(format!("failed to build certificate params: {error}"))
        })?;
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Galactica");
    params.distinguished_name.push(DnType::CommonName, node_id);

    let key_pair = KeyPair::generate().map_err(|error| {
        GalacticaError::internal(format!("failed to generate TLS key pair: {error}"))
    })?;
    let certificate = params.self_signed(&key_pair).map_err(|error| {
        GalacticaError::internal(format!(
            "failed to generate self-signed certificate: {error}"
        ))
    })?;
    let certificate_pem = certificate.pem();
    let private_key_pem = key_pair.serialize_pem();
    let fingerprint = fingerprint_certificate(&certificate_pem);

    Ok(IssuedTlsIdentity {
        certificate_pem,
        private_key_pem,
        fingerprint,
        issued_at,
        expires_at,
    })
}

pub fn fingerprint_certificate(certificate_pem: &str) -> String {
    let digest = Sha256::digest(certificate_pem.as_bytes());
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

pub fn verify_certificate_fingerprint(certificate_pem: &str, expected_fingerprint: &str) -> bool {
    fingerprint_certificate(certificate_pem) == expected_fingerprint
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issued_identity_contains_certificate_material() {
        let identity =
            issue_tls_identity("node-a", "mac-mini", "ignored", Duration::days(30)).unwrap();

        assert!(identity.certificate_pem.contains("BEGIN CERTIFICATE"));
        assert!(identity.private_key_pem.contains("BEGIN PRIVATE KEY"));
        assert!(verify_certificate_fingerprint(
            &identity.certificate_pem,
            &identity.fingerprint
        ));
        assert!(identity.expires_at > identity.issued_at);
    }

    #[test]
    fn issuing_identity_requires_positive_validity() {
        let error =
            issue_tls_identity("node-a", "mac-mini", "ignored", Duration::zero()).unwrap_err();

        assert_eq!(
            error,
            GalacticaError::InvalidArgument("certificate validity must be positive".to_string())
        );
    }
}

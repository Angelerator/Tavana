//! TLS configuration utilities for secure communication

use crate::error::{Result, TavanaError};
use rcgen::{CertificateParams, DnType, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// TLS configuration holder
pub struct TlsConfig {
    /// Certificate chain in DER format
    pub cert_chain: Vec<CertificateDer<'static>>,
    /// Private key in DER format (stored as bytes for cloning)
    private_key_bytes: Vec<u8>,
    /// Optional CA certificate for client verification
    pub ca_cert: Option<CertificateDer<'static>>,
}

impl TlsConfig {
    /// Get the private key
    pub fn private_key(&self) -> PrivateKeyDer<'static> {
        // Parse from stored bytes - this should always succeed as we validated on construction
        PrivateKeyDer::try_from(self.private_key_bytes.clone())
            .expect("private key bytes were validated at TlsConfig construction")
    }
}

impl Clone for TlsConfig {
    fn clone(&self) -> Self {
        Self {
            cert_chain: self.cert_chain.clone(),
            private_key_bytes: self.private_key_bytes.clone(),
            ca_cert: self.ca_cert.clone(),
        }
    }
}

impl TlsConfig {
    /// Load TLS config from PEM files
    pub fn from_pem_files(
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
        ca_path: Option<impl AsRef<Path>>,
    ) -> Result<Self> {
        let cert_pem = fs::read(cert_path.as_ref())
            .map_err(|e| TavanaError::TlsError(format!("Failed to read cert: {}", e)))?;
        let key_pem = fs::read(key_path.as_ref())
            .map_err(|e| TavanaError::TlsError(format!("Failed to read key: {}", e)))?;

        let cert_chain = Self::parse_pem_certs(&cert_pem)?;
        let private_key = Self::parse_pem_key(&key_pem)?;

        let ca_cert = if let Some(ca_path) = ca_path {
            let ca_pem = fs::read(ca_path.as_ref())
                .map_err(|e| TavanaError::TlsError(format!("Failed to read CA: {}", e)))?;
            Some(
                Self::parse_pem_certs(&ca_pem)?
                    .into_iter()
                    .next()
                    .ok_or_else(|| TavanaError::TlsError("No CA certificate found".into()))?,
            )
        } else {
            None
        };

        // Store key bytes for cloning
        let private_key_bytes = match &private_key {
            PrivateKeyDer::Pkcs1(key) => key.secret_pkcs1_der().to_vec(),
            PrivateKeyDer::Sec1(key) => key.secret_sec1_der().to_vec(),
            PrivateKeyDer::Pkcs8(key) => key.secret_pkcs8_der().to_vec(),
            _ => return Err(TavanaError::TlsError("Unsupported key format".into())),
        };

        Ok(Self {
            cert_chain,
            private_key_bytes,
            ca_cert,
        })
    }

    /// Generate self-signed certificate for development
    pub fn generate_self_signed(
        common_name: &str,
        san_dns: &[String],
        san_ips: &[std::net::IpAddr],
    ) -> Result<(String, String)> {
        let mut params = CertificateParams::default();
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        params
            .distinguished_name
            .push(DnType::OrganizationName, "Tavana");

        // Add Subject Alternative Names
        let mut subject_alt_names = Vec::new();
        for dns in san_dns {
            subject_alt_names.push(rcgen::SanType::DnsName(
                dns.clone()
                    .try_into()
                    .map_err(|e| TavanaError::TlsError(format!("Invalid DNS name: {}", e)))?,
            ));
        }
        for ip in san_ips {
            subject_alt_names.push(rcgen::SanType::IpAddress(*ip));
        }
        params.subject_alt_names = subject_alt_names;

        // Generate key pair
        let key_pair = KeyPair::generate()
            .map_err(|e| TavanaError::TlsError(format!("Failed to generate key pair: {}", e)))?;

        let cert = params
            .self_signed(&key_pair)
            .map_err(|e| TavanaError::TlsError(format!("Failed to generate certificate: {}", e)))?;

        let cert_pem = cert.pem();
        let key_pem = key_pair.serialize_pem();

        Ok((cert_pem, key_pem))
    }

    /// Parse PEM-encoded certificates
    fn parse_pem_certs(pem_data: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
        let mut reader = std::io::BufReader::new(pem_data);
        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| TavanaError::TlsError(format!("Failed to parse certificates: {}", e)))?;
        Ok(certs)
    }

    /// Parse PEM-encoded private key
    fn parse_pem_key(pem_data: &[u8]) -> Result<PrivateKeyDer<'static>> {
        let mut reader = std::io::BufReader::new(pem_data);

        // Try to read any type of private key
        let key = rustls_pemfile::private_key(&mut reader)
            .map_err(|e| TavanaError::TlsError(format!("Failed to parse private key: {}", e)))?
            .ok_or_else(|| TavanaError::TlsError("No private key found".into()))?;

        Ok(key)
    }
}

/// Create a rustls client config for connecting to TLS servers
pub fn create_client_tls_config(
    config: &TlsConfig,
    verify_server: bool,
) -> Result<rustls::ClientConfig> {
    let builder = rustls::ClientConfig::builder();

    let config = if verify_server {
        // Use webpki roots for production
        let mut root_store = rustls::RootCertStore::empty();

        // Add system roots
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        // Add custom CA if provided
        if let Some(ca) = &config.ca_cert {
            root_store.add(ca.clone()).map_err(|e| {
                TavanaError::TlsError(format!("Failed to add CA certificate: {}", e))
            })?;
        }

        builder
            .with_root_certificates(root_store)
            .with_no_client_auth()
    } else {
        // Skip verification (for development only!)
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertificateVerification))
            .with_no_client_auth()
    };

    Ok(config)
}

/// Create a rustls server config for accepting TLS connections
pub fn create_server_tls_config(config: &TlsConfig) -> Result<rustls::ServerConfig> {
    let builder = rustls::ServerConfig::builder().with_no_client_auth();

    let server_config = builder
        .with_single_cert(config.cert_chain.clone(), config.private_key())
        .map_err(|e| TavanaError::TlsError(format!("Failed to create server config: {}", e)))?;

    Ok(server_config)
}

/// Certificate verifier that accepts any certificate (FOR DEVELOPMENT ONLY)
#[derive(Debug)]
struct NoCertificateVerification;

impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_self_signed() {
        let (cert_pem, key_pem) = TlsConfig::generate_self_signed(
            "localhost",
            &["localhost".to_string()],
            &["127.0.0.1".parse().unwrap()],
        )
        .unwrap();

        assert!(cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(key_pem.contains("BEGIN PRIVATE KEY"));
    }
}

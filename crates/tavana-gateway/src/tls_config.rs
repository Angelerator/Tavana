//! TLS configuration for PostgreSQL wire protocol
//!
//! Supports both self-signed certificates (for development) and
//! custom certificates (for production).

use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, SanType};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

/// TLS configuration for the PostgreSQL server
#[derive(Clone)]
pub struct TlsConfig {
    pub server_config: Arc<ServerConfig>,
}

impl TlsConfig {
    /// Create TLS config from certificate and key files
    pub fn from_files(cert_path: &Path, key_path: &Path) -> anyhow::Result<Self> {
        info!("Loading TLS certificates from files");
        
        let cert_file = std::fs::File::open(cert_path)
            .map_err(|e| anyhow::anyhow!("Failed to open cert file: {}", e))?;
        let key_file = std::fs::File::open(key_path)
            .map_err(|e| anyhow::anyhow!("Failed to open key file: {}", e))?;

        let mut cert_reader = BufReader::new(cert_file);
        let mut key_reader = BufReader::new(key_file);

        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
            .filter_map(|r| r.ok())
            .collect();

        let key = rustls_pemfile::private_key(&mut key_reader)?
            .ok_or_else(|| anyhow::anyhow!("No private key found in key file"))?;

        Self::from_certs_and_key(certs, key)
    }

    /// Create TLS config with a self-signed certificate
    pub fn self_signed(common_name: &str) -> anyhow::Result<Self> {
        info!("Generating self-signed TLS certificate for: {}", common_name);
        
        let mut params = CertificateParams::default();
        
        // Set distinguished name
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, common_name);
        dn.push(DnType::OrganizationName, "Tavana");
        params.distinguished_name = dn;
        
        // Add Subject Alternative Names for various connection methods
        params.subject_alt_names = vec![
            SanType::DnsName(common_name.try_into()?),
            SanType::DnsName("localhost".try_into()?),
            SanType::DnsName("tavana-gateway".try_into()?),
            SanType::DnsName("*.tavana.svc.cluster.local".try_into()?),
        ];
        
        // Valid for 1 year
        params.not_after = rcgen::date_time_ymd(2026, 12, 31);
        
        // Generate key pair
        let key_pair = KeyPair::generate()?;
        
        // Generate certificate
        let cert = params.self_signed(&key_pair)?;
        
        let cert_der = CertificateDer::from(cert.der().to_vec());
        let key_der = PrivateKeyDer::try_from(key_pair.serialize_der())
            .map_err(|e| anyhow::anyhow!("Failed to serialize private key: {:?}", e))?;
        
        Self::from_certs_and_key(vec![cert_der], key_der)
    }

    /// Create TLS config from DER-encoded certificates and key
    fn from_certs_and_key(
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> anyhow::Result<Self> {
        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| anyhow::anyhow!("TLS configuration error: {}", e))?;

        Ok(Self {
            server_config: Arc::new(server_config),
        })
    }

    /// Get the TLS acceptor for async connections
    pub fn acceptor(&self) -> tokio_rustls::TlsAcceptor {
        tokio_rustls::TlsAcceptor::from(self.server_config.clone())
    }
}

/// Load TLS configuration based on environment variables
/// 
/// Environment variables:
/// - TAVANA_TLS_ENABLED: "true" to enable TLS (default: "true")
/// - TAVANA_TLS_CERT_PATH: Path to certificate file (PEM format)
/// - TAVANA_TLS_KEY_PATH: Path to private key file (PEM format)
/// - TAVANA_TLS_SELF_SIGNED: "true" to use self-signed cert (default if no cert/key provided)
/// - TAVANA_TLS_COMMON_NAME: Common name for self-signed cert (default: "tavana.local")
pub fn load_tls_config_from_env() -> Option<TlsConfig> {
    let enabled = std::env::var("TAVANA_TLS_ENABLED")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()
        .unwrap_or(true);

    if !enabled {
        info!("TLS is disabled via TAVANA_TLS_ENABLED=false");
        return None;
    }

    // Try to load from files first
    let cert_path = std::env::var("TAVANA_TLS_CERT_PATH").ok();
    let key_path = std::env::var("TAVANA_TLS_KEY_PATH").ok();

    if let (Some(cert), Some(key)) = (cert_path, key_path) {
        match TlsConfig::from_files(Path::new(&cert), Path::new(&key)) {
            Ok(config) => {
                info!("Loaded TLS configuration from certificate files");
                return Some(config);
            }
            Err(e) => {
                warn!("Failed to load TLS from files: {} - falling back to self-signed", e);
            }
        }
    }

    // Fall back to self-signed
    let use_self_signed = std::env::var("TAVANA_TLS_SELF_SIGNED")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()
        .unwrap_or(true);

    if use_self_signed {
        let common_name = std::env::var("TAVANA_TLS_COMMON_NAME")
            .unwrap_or_else(|_| "tavana.local".to_string());
        
        match TlsConfig::self_signed(&common_name) {
            Ok(config) => {
                info!("Generated self-signed TLS certificate for: {}", common_name);
                return Some(config);
            }
            Err(e) => {
                warn!("Failed to generate self-signed certificate: {}", e);
            }
        }
    }

    warn!("TLS is enabled but no certificate configured - SSL connections will be declined");
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_self_signed_cert() {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
        let config = TlsConfig::self_signed("test.tavana.local").unwrap();
        let _acceptor = config.acceptor();
    }
}


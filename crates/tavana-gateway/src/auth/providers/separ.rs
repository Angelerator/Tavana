//! Separ provider
//!
//! Integrates with Separ for centralized authentication and authorization.
//! Supports:
//! - OAuth/JWT token validation
//! - Personal Access Token (PAT) validation
//! - Service Account Key validation
//! - Authorization checks via SpiceDB

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::{AuthProvider, Credentials};
use crate::auth::config::{SeparConfig, TenantExtractionStrategy};
use crate::auth::identity::{AuthResult, AuthenticatedPrincipal, PrincipalType};

/// Separ authentication provider
pub struct SeparProvider {
    /// HTTP client for API calls
    client: Client,
    /// Configuration
    config: Arc<SeparConfig>,
}

impl SeparProvider {
    /// Create a new Separ provider with optimized connection pooling
    pub fn new(config: SeparConfig) -> anyhow::Result<Self> {
        let client = Client::builder()
            // Connection settings
            .connect_timeout(config.connect_timeout())
            .timeout(config.request_timeout())
            // Connection pooling - keep connections alive for reuse
            .pool_idle_timeout(config.pool_idle_timeout())
            .pool_max_idle_per_host(config.pool_max_idle_per_host)
            // TCP keep-alive to detect dead connections
            .tcp_keepalive(std::time::Duration::from_secs(30))
            // Enable HTTP/1.1 keep-alive
            .http1_only()
            .build()?;

        let provider = Self {
            client,
            config: Arc::new(config),
        };

        // Spawn a warmup task to pre-establish connection
        let warmup_client = provider.client.clone();
        let warmup_endpoint = provider.config.endpoint.clone();
        tokio::spawn(async move {
            // Try to establish connection early
            let _ = warmup_client
                .get(format!("{}/health", warmup_endpoint))
                .send()
                .await;
            tracing::debug!("Separ connection warmup completed");
        });

        Ok(provider)
    }

    /// Detect the credential type from the secret
    fn detect_credential_type(secret: &str) -> CredentialType {
        if secret.starts_with("eyJ") && secret.contains('.') {
            CredentialType::Jwt
        } else if secret.starts_with("pat_") {
            CredentialType::PersonalAccessToken
        } else if secret.starts_with("sak_") {
            CredentialType::ServiceAccountKey
        } else if secret.starts_with("sk_") || secret.starts_with("tvn_") {
            CredentialType::ApiKey
        } else {
            // Could be a password or unknown token format
            CredentialType::Password
        }
    }

    /// Extract tenant hint from username based on strategy
    fn extract_tenant_hint(&self, username: &str) -> Option<String> {
        match self.config.tenant_extraction {
            TenantExtractionStrategy::EmailDomain => {
                // user@tenant.example.com -> tenant
                if let Some(at_pos) = username.find('@') {
                    let domain = &username[at_pos + 1..];
                    domain.split('.').next().map(|s| s.to_string())
                } else {
                    None
                }
            }
            TenantExtractionStrategy::BackslashPrefix => {
                // tenant\user -> tenant
                username.split('\\').next().map(|s| s.to_string())
            }
            TenantExtractionStrategy::None => None,
        }
    }

    /// Validate credentials with Separ
    async fn validate_with_separ(
        &self,
        credentials: &Credentials,
        cred_type: CredentialType,
    ) -> AuthResult {
        let tenant_hint = self.extract_tenant_hint(&credentials.username);

        let request = SeparAuthRequest {
            username: credentials.username.clone(),
            credential: credentials.secret.clone(),
            credential_type: cred_type.as_str().to_string(),
            tenant_hint,
            application: Some("tavana".to_string()),
            client_ip: credentials.context.client_ip.clone(),
        };

        let endpoint = format!("{}/api/v1/auth/validate", self.config.endpoint);

        let response = match self
            .client
            .post(&endpoint)
            .header("X-API-Key", &self.config.api_key)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                tracing::error!(error = %e, "Failed to connect to Separ");
                return AuthResult::ProviderError(format!("Separ connection failed: {}", e));
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            
            tracing::warn!(
                status = %status,
                body = %body,
                "Separ authentication failed"
            );

            return if status.as_u16() == 401 || status.as_u16() == 403 {
                AuthResult::InvalidCredentials("Invalid credentials".to_string())
            } else {
                AuthResult::ProviderError(format!("Separ error: {} - {}", status, body))
            };
        }

        match response.json::<SeparAuthResponse>().await {
            Ok(separ_response) => {
                tracing::info!(
                    user_id = %separ_response.user_id,
                    tenant_id = %separ_response.tenant_id,
                    principal_type = %separ_response.principal_type,
                    "Separ authentication successful"
                );

                let principal = AuthenticatedPrincipal {
                    id: separ_response.user_id,
                    principal_type: parse_principal_type(&separ_response.principal_type),
                    display_name: separ_response.display_name,
                    email: separ_response.email,
                    tenant_id: Some(separ_response.tenant_id.clone()),
                    tenant_name: separ_response.tenant_name,
                    groups: separ_response.groups,
                    scopes: separ_response.permissions,
                    authenticated_at: Utc::now(),
                    expires_at: separ_response.expires_at.map(|ts| {
                        DateTime::from_timestamp(ts, 0).unwrap_or_else(Utc::now)
                    }),
                    provider: "separ".to_string(),
                    attributes: separ_response.attributes.unwrap_or_default(),
                };

                AuthResult::Success(principal)
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to parse Separ response");
                AuthResult::ProviderError(format!("Invalid Separ response: {}", e))
            }
        }
    }

    /// Check authorization with Separ
    async fn check_authz(
        &self,
        principal: &AuthenticatedPrincipal,
        permission: &str,
        resource: &str,
    ) -> Option<bool> {
        if !self.config.enable_authz {
            return None;
        }

        let request = SeparAuthzRequest {
            subject_type: principal.principal_type.to_string(),
            subject_id: principal.id.clone(),
            permission: permission.to_string(),
            resource_type: extract_resource_type(resource),
            resource_id: extract_resource_id(resource),
            tenant_id: principal.tenant_id.clone(),
        };

        let endpoint = format!("{}/api/v1/authz/check", self.config.endpoint);

        match self
            .client
            .post(&endpoint)
            .header("X-API-Key", &self.config.api_key)
            .json(&request)
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    match response.json::<SeparAuthzResponse>().await {
                        Ok(authz_response) => {
                            tracing::debug!(
                                allowed = %authz_response.allowed,
                                permission = %permission,
                                resource = %resource,
                                "Separ authorization check"
                            );
                            Some(authz_response.allowed)
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Failed to parse Separ authz response");
                            None
                        }
                    }
                } else {
                    tracing::error!(
                        status = %response.status(),
                        "Separ authorization check failed"
                    );
                    None
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to connect to Separ for authz");
                None
            }
        }
    }
}

#[async_trait]
impl AuthProvider for SeparProvider {
    fn name(&self) -> &str {
        "separ"
    }

    fn can_handle(&self, credentials: &Credentials) -> bool {
        // Separ can handle all credential types
        let cred_type = Self::detect_credential_type(&credentials.secret);
        matches!(
            cred_type,
            CredentialType::Jwt
                | CredentialType::PersonalAccessToken
                | CredentialType::ServiceAccountKey
                | CredentialType::Password
        )
    }

    async fn authenticate(&self, credentials: &Credentials) -> AuthResult {
        let cred_type = Self::detect_credential_type(&credentials.secret);
        
        tracing::debug!(
            username = %credentials.username,
            credential_type = ?cred_type,
            "Attempting Separ authentication"
        );

        self.validate_with_separ(credentials, cred_type).await
    }

    async fn check_permission(
        &self,
        principal: &AuthenticatedPrincipal,
        permission: &str,
        resource: &str,
    ) -> Option<bool> {
        self.check_authz(principal, permission, resource).await
    }
}

// ============================================================================
// Separ API Types
// ============================================================================

#[derive(Debug, Clone, Copy)]
enum CredentialType {
    Jwt,
    PersonalAccessToken,
    ServiceAccountKey,
    ApiKey,
    Password,
}

impl CredentialType {
    fn as_str(&self) -> &'static str {
        match self {
            CredentialType::Jwt => "jwt",
            CredentialType::PersonalAccessToken => "pat",
            CredentialType::ServiceAccountKey => "sak",
            CredentialType::ApiKey => "api_key",
            CredentialType::Password => "password",
        }
    }
}

/// Request to Separ for authentication validation
#[derive(Debug, Serialize)]
struct SeparAuthRequest {
    username: String,
    credential: String,
    credential_type: String,
    tenant_hint: Option<String>,
    application: Option<String>,
    client_ip: Option<String>,
}

/// Response from Separ authentication
#[derive(Debug, Deserialize)]
struct SeparAuthResponse {
    user_id: String,
    principal_type: String,
    tenant_id: String,
    tenant_name: Option<String>,
    display_name: Option<String>,
    email: Option<String>,
    groups: Vec<String>,
    permissions: Vec<String>,
    expires_at: Option<i64>,
    #[serde(default)]
    attributes: Option<HashMap<String, serde_json::Value>>,
}

/// Request to Separ for authorization check
#[derive(Debug, Serialize)]
struct SeparAuthzRequest {
    subject_type: String,
    subject_id: String,
    permission: String,
    resource_type: String,
    resource_id: String,
    tenant_id: Option<String>,
}

/// Response from Separ authorization check
#[derive(Debug, Deserialize)]
struct SeparAuthzResponse {
    allowed: bool,
    #[serde(default)]
    reason: Option<String>,
}

// ============================================================================
// Helper Functions
// ============================================================================

fn parse_principal_type(s: &str) -> PrincipalType {
    match s.to_lowercase().as_str() {
        "user" => PrincipalType::User,
        "application" | "app" => PrincipalType::Application,
        "service_account" | "service" => PrincipalType::ServiceAccount,
        "api_key" => PrincipalType::ApiKey,
        _ => PrincipalType::User,
    }
}

fn extract_resource_type(resource: &str) -> String {
    resource
        .split(':')
        .next()
        .unwrap_or("resource")
        .to_string()
}

fn extract_resource_id(resource: &str) -> String {
    resource
        .split(':')
        .nth(1)
        .unwrap_or(resource)
        .to_string()
}


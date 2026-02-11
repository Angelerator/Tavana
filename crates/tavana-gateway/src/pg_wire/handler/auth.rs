//! PostgreSQL authentication handlers
//!
//! Implements cleartext password authentication for PostgreSQL wire protocol.
//! Supports both TcpStream and generic async streams (for TLS connections).

use crate::auth::{AuthContext, AuthGateway, AuthenticatedPrincipal};
use super::messages::send_error_response;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error, info, warn};

/// Perform cleartext password authentication for TcpStream
pub(crate) async fn perform_md5_auth(
    socket: &mut tokio::net::TcpStream,
    user: &str,
    auth_gateway: Option<&Arc<AuthGateway>>,
    client_ip: Option<String>,
) -> anyhow::Result<Option<AuthenticatedPrincipal>> {
    perform_cleartext_auth_internal(socket, user, Some((auth_gateway, client_ip))).await
}

/// Perform cleartext password authentication with optional gateway validation
#[allow(dead_code)]
pub(crate) async fn perform_cleartext_auth_with_gateway(
    socket: &mut tokio::net::TcpStream,
    user: &str,
    auth_gateway: Option<&Arc<AuthGateway>>,
    client_ip: Option<String>,
) -> anyhow::Result<Option<AuthenticatedPrincipal>> {
    perform_cleartext_auth_internal(socket, user, Some((auth_gateway, client_ip))).await
}

/// Internal implementation of cleartext password authentication
async fn perform_cleartext_auth_internal(
    socket: &mut tokio::net::TcpStream,
    user: &str,
    auth_info: Option<(Option<&Arc<AuthGateway>>, Option<String>)>,
) -> anyhow::Result<Option<AuthenticatedPrincipal>> {
    // Send AuthenticationCleartextPassword (R with auth type 3)
    let auth_req = [b'R', 0, 0, 0, 8, 0, 0, 0, 3];
    socket.write_all(&auth_req).await?;
    socket.flush().await?;
    
    debug!("Sent cleartext auth request to client for user: {}", user);
    
    // Read password response
    let mut msg_type = [0u8; 1];
    socket.read_exact(&mut msg_type).await?;
    
    if msg_type[0] != b'p' {
        return Err(anyhow::anyhow!("Expected password message, got: {:?}", msg_type[0]));
    }
    
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize - 4;
    
    let mut password_data = vec![0u8; len];
    socket.read_exact(&mut password_data).await?;
    
    let password = String::from_utf8_lossy(&password_data)
        .trim_end_matches('\0')
        .to_string();
    
    // Validate with auth gateway if available
    if let Some((gateway_opt, client_ip)) = auth_info {
        if let Some(gateway) = gateway_opt {
            if !gateway.is_passthrough() {
                let context = AuthContext {
                    client_ip,
                    application_name: None,
                    client_id: None,
                };
                
                match gateway.authenticate(user, &password, context).await {
                    crate::auth::identity::AuthResult::Success(principal) => {
                        socket.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await?;
                        socket.flush().await?;
                        info!(
                            user_id = %principal.id,
                            principal_type = %principal.principal_type,
                            "Authentication successful for user: {}",
                            user
                        );
                        return Ok(Some(principal));
                    }
                    crate::auth::identity::AuthResult::InvalidCredentials(reason) => {
                        warn!("Authentication failed for user {}: {}", user, reason);
                        send_error_response(socket, "28P01", "password authentication failed").await?;
                        return Err(anyhow::anyhow!("Authentication failed: {}", reason));
                    }
                    crate::auth::identity::AuthResult::Expired => {
                        warn!("Authentication expired for user {}", user);
                        send_error_response(socket, "28P01", "authentication token expired").await?;
                        return Err(anyhow::anyhow!("Token expired"));
                    }
                    crate::auth::identity::AuthResult::ProviderError(msg) => {
                        error!("Auth provider error for user {}: {}", user, msg);
                        send_error_response(socket, "XX000", "authentication service error").await?;
                        return Err(anyhow::anyhow!("Provider error: {}", msg));
                    }
                    crate::auth::identity::AuthResult::NotAuthenticated => {
                        // Fall through to passthrough mode
                    }
                }
            }
        }
    }
    
    // Passthrough mode
    debug!("Received password response, accepting (passthrough mode)");
    socket.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await?;
    socket.flush().await?;
    
    info!("Authentication completed for user: {} (passthrough)", user);
    Ok(None)
}

/// Perform cleartext password authentication for generic async streams (TLS)
pub(crate) async fn perform_md5_auth_generic<S>(
    socket: &mut S,
    user: &str,
    auth_gateway: Option<&Arc<AuthGateway>>,
    client_ip: Option<String>,
) -> anyhow::Result<Option<AuthenticatedPrincipal>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    perform_cleartext_auth_generic_with_gateway(socket, user, auth_gateway, client_ip).await
}

/// Perform cleartext password authentication for TLS with optional gateway
#[allow(dead_code)]
pub(crate) async fn perform_cleartext_auth_generic_with_gateway<S>(
    socket: &mut S,
    user: &str,
    auth_gateway: Option<&Arc<AuthGateway>>,
    client_ip: Option<String>,
) -> anyhow::Result<Option<AuthenticatedPrincipal>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let auth_req = [b'R', 0, 0, 0, 8, 0, 0, 0, 3];
    socket.write_all(&auth_req).await?;
    socket.flush().await?;
    
    debug!("Sent cleartext auth request to client for user: {} (TLS)", user);
    
    let mut msg_type = [0u8; 1];
    socket.read_exact(&mut msg_type).await?;
    
    if msg_type[0] != b'p' {
        return Err(anyhow::anyhow!("Expected password message, got: {:?}", msg_type[0]));
    }
    
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize - 4;
    
    let mut password_data = vec![0u8; len];
    socket.read_exact(&mut password_data).await?;
    
    let password = String::from_utf8_lossy(&password_data)
        .trim_end_matches('\0')
        .to_string();
    
    if let Some(gateway) = auth_gateway {
        if !gateway.is_passthrough() {
            let context = AuthContext {
                client_ip,
                application_name: None,
                client_id: None,
            };
            
            match gateway.authenticate(user, &password, context).await {
                crate::auth::identity::AuthResult::Success(principal) => {
                    socket.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await?;
                    socket.flush().await?;
                    info!(
                        user_id = %principal.id,
                        "Authentication successful for user: {} (TLS)",
                        user
                    );
                    return Ok(Some(principal));
                }
                crate::auth::identity::AuthResult::InvalidCredentials(reason) => {
                    warn!("Authentication failed for user {} (TLS): {}", user, reason);
                    return Err(anyhow::anyhow!("Authentication failed: {}", reason));
                }
                _ => {
                    // Fall through to passthrough
                }
            }
        }
    }
    
    // Passthrough mode
    socket.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await?;
    socket.flush().await?;
    
    info!("Authentication completed for user: {} (TLS passthrough)", user);
    Ok(None)
}

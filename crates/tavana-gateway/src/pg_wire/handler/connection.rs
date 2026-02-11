//! Connection handling for PostgreSQL wire protocol
//!
//! TLS negotiation, startup message processing, TCP keepalive configuration,
//! and connection lifecycle management. After TLS negotiation, all handling
//! is generic over the stream type.

use crate::auth::AuthService;
use crate::query_queue::QueryQueue;
use crate::query_router::QueryRouter;
use crate::smart_scaler::SmartScaler;
use crate::tls_config::TlsConfig;
use crate::worker_client::{WorkerClient, WorkerClientPool};
use super::auth::perform_auth;
use super::config::PgWireConfig;
use super::messages::send_parameter_status;
use super::query_loop::run_query_loop;
use super::utils::extract_startup_param;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, info, warn};

/// Configure TCP keepalive on a socket for faster dead connection detection
pub(crate) fn configure_tcp_keepalive(stream: &tokio::net::TcpStream, keepalive_secs: u64) {
    use socket2::SockRef;

    if let Err(e) = stream.set_nodelay(true) {
        warn!("Failed to set TCP_NODELAY: {}", e);
    }

    let socket = SockRef::from(stream);

    if let Err(e) = socket.set_keepalive(true) {
        warn!("Failed to enable TCP keepalive: {}", e);
        return;
    }

    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(keepalive_secs))
        .with_interval(Duration::from_secs(keepalive_secs / 2 + 1));

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let keepalive = keepalive.with_retries(3);

    if let Err(e) = socket.set_tcp_keepalive(&keepalive) {
        warn!("Failed to configure TCP keepalive timing: {}", e);
    } else {
        debug!("TCP keepalive configured: {}s", keepalive_secs);
    }
}

/// Handle connection with optional TLS upgrade
///
/// This is the entry point for all new connections. It handles:
/// 1. Reading the first message (SSLRequest, GSSENCRequest, CancelRequest, or StartupMessage)
/// 2. TLS upgrade if requested and configured
/// 3. Delegating to the generic connection handler
pub(crate) async fn handle_connection_with_tls(
    mut socket: tokio::net::TcpStream,
    auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    worker_client_pool: Arc<WorkerClientPool>,
    query_router: Arc<QueryRouter>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    tls_config: Option<Arc<TlsConfig>>,
    config: Arc<PgWireConfig>,
) -> anyhow::Result<()> {
    let client_ip = socket.peer_addr().ok().map(|addr| addr.ip().to_string());

    let mut buf = [0u8; 4];

    socket.read_exact(&mut buf).await?;
    let len = u32::from_be_bytes(buf) as usize;

    if len < 8 || len > 10000 {
        return Err(anyhow::anyhow!("Invalid message length: {}", len));
    }

    let mut startup_msg = vec![0u8; len - 4];
    socket.read_exact(&mut startup_msg).await?;

    if len == 8 && startup_msg.len() >= 4 {
        let code = u32::from_be_bytes([
            startup_msg[0], startup_msg[1], startup_msg[2], startup_msg[3],
        ]);

        match code {
            80877103 => {
                // SSLRequest
                if let Some(ref tls) = tls_config {
                    debug!("SSL negotiation requested, accepting");
                    socket.write_all(&[b'S']).await?;
                    socket.flush().await?;

                    let acceptor = tls.acceptor();
                    let tls_stream = acceptor.accept(socket).await
                        .map_err(|e| anyhow::anyhow!("TLS handshake failed: {}", e))?;

                    info!("TLS connection established");

                    return handle_connection_inner(
                        tls_stream, auth_service, worker_client, worker_client_pool,
                        query_router, smart_scaler, query_queue, config, client_ip,
                    ).await;
                } else {
                    debug!("SSL negotiation requested, declining (no TLS config)");
                    socket.write_all(&[b'N']).await?;
                    socket.flush().await?;

                    return handle_connection_inner(
                        socket, auth_service, worker_client, worker_client_pool,
                        query_router, smart_scaler, query_queue, config, client_ip,
                    ).await;
                }
            }
            80877104 => {
                // GSSENCRequest
                debug!("GSSAPI negotiation requested, declining");
                socket.write_all(&[b'N']).await?;
                socket.flush().await?;

                return handle_connection_inner(
                    socket, auth_service, worker_client, worker_client_pool,
                    query_router, smart_scaler, query_queue, config, client_ip,
                ).await;
            }
            80877102 => {
                // CancelRequest
                let backend_pid = if startup_msg.len() >= 8 {
                    u32::from_be_bytes([startup_msg[4], startup_msg[5], startup_msg[6], startup_msg[7]])
                } else { 0 };

                warn!(
                    backend_pid = backend_pid,
                    "CancelRequest received. Query cancellation not yet fully implemented. \
                     The query will continue running. Use LIMIT clause for large queries."
                );

                return Ok(());
            }
            _ => {
                // Normal startup message — already read
            }
        }
    }

    // First message was the startup message (no SSL negotiation)
    handle_connection_with_startup(
        socket, startup_msg, auth_service, worker_client, worker_client_pool,
        query_router, smart_scaler, query_queue, config, client_ip,
    ).await
}

/// Generic connection handler — reads startup message from stream
///
/// Used after SSL negotiation (accepted or declined), when the startup
/// message hasn't been read yet.
async fn handle_connection_inner<S>(
    mut socket: S,
    auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    worker_client_pool: Arc<WorkerClientPool>,
    query_router: Arc<QueryRouter>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    config: Arc<PgWireConfig>,
    client_ip: Option<String>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let auth_gateway = auth_service.gateway().cloned();

    let mut buf = [0u8; 4];
    let startup_msg;

    // Read startup message, handling any nested SSL/GSS requests
    loop {
        socket.read_exact(&mut buf).await?;
        let len = u32::from_be_bytes(buf) as usize;
        debug!("Received message, length: {}", len);

        if len < 8 || len > 10000 {
            return Err(anyhow::anyhow!("Invalid message length: {}", len));
        }

        let mut msg = vec![0u8; len - 4];
        socket.read_exact(&mut msg).await?;

        if len == 8 && msg.len() >= 4 {
            let code = u32::from_be_bytes([msg[0], msg[1], msg[2], msg[3]]);
            match code {
                80877103 | 80877104 => {
                    debug!("Nested SSL/GSSAPI request, declining");
                    socket.write_all(&[b'N']).await?;
                    socket.flush().await?;
                    continue;
                }
                80877102 => {
                    warn!("CancelRequest received (query cancellation not fully implemented)");
                    return Ok(());
                }
                _ => {}
            }
        }

        startup_msg = msg;
        break;
    }

    complete_connection(
        &mut socket, startup_msg, auth_gateway, worker_client, worker_client_pool,
        query_router, smart_scaler, query_queue, config, client_ip,
    ).await
}

/// Handle connection with already-parsed startup message
async fn handle_connection_with_startup<S>(
    mut socket: S,
    startup_msg: Vec<u8>,
    auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    worker_client_pool: Arc<WorkerClientPool>,
    query_router: Arc<QueryRouter>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    config: Arc<PgWireConfig>,
    client_ip: Option<String>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let auth_gateway = auth_service.gateway().cloned();

    complete_connection(
        &mut socket, startup_msg, auth_gateway, worker_client, worker_client_pool,
        query_router, smart_scaler, query_queue, config, client_ip,
    ).await
}

/// Complete connection setup: authenticate, send parameters, enter query loop
async fn complete_connection<S>(
    socket: &mut S,
    startup_msg: Vec<u8>,
    auth_gateway: Option<Arc<crate::auth::AuthGateway>>,
    worker_client: Arc<WorkerClient>,
    worker_client_pool: Arc<WorkerClientPool>,
    query_router: Arc<QueryRouter>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    config: Arc<PgWireConfig>,
    client_ip: Option<String>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let user_id = extract_startup_param(&startup_msg, "user")
        .unwrap_or_else(|| "anonymous".to_string());
    info!("PostgreSQL client connected as user: {}", user_id);

    // Authenticate
    let _principal = perform_auth(socket, &user_id, auth_gateway.as_ref(), client_ip).await?;

    // Send server parameters
    send_parameter_status(socket, "server_version", "15.0.0").await?;
    send_parameter_status(socket, "client_encoding", "UTF8").await?;
    send_parameter_status(socket, "server_encoding", "UTF8").await?;
    send_parameter_status(socket, "DateStyle", "ISO, MDY").await?;
    send_parameter_status(socket, "TimeZone", "UTC").await?;
    send_parameter_status(socket, "integer_datetimes", "on").await?;
    send_parameter_status(socket, "standard_conforming_strings", "on").await?;

    // Send BackendKeyData
    let pid = std::process::id();
    let secret = pid.wrapping_mul(1103515245).wrapping_add(12345);
    let mut key_data = vec![b'K', 0, 0, 0, 12];
    key_data.extend_from_slice(&(pid as u32).to_be_bytes());
    key_data.extend_from_slice(&secret.to_be_bytes());
    socket.write_all(&key_data).await?;

    // Send ReadyForQuery
    let ready = [b'Z', 0, 0, 0, 5, b'I'];
    socket.write_all(&ready).await?;
    socket.flush().await?;

    // Enter main query loop
    run_query_loop(
        socket, &worker_client, &worker_client_pool, &query_router,
        &user_id, smart_scaler.as_ref().map(|s| s.as_ref()), &query_queue, &config,
    ).await
}

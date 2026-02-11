//! Connection handling for PostgreSQL wire protocol
//!
//! TLS negotiation, startup message processing, TCP keepalive configuration,
//! and connection lifecycle management.

use crate::auth::AuthService;
use crate::query_queue::QueryQueue;
use crate::query_router::QueryRouter;
use crate::smart_scaler::SmartScaler;
use crate::tls_config::TlsConfig;
use crate::worker_client::{WorkerClient, WorkerClientPool};
use crate::worker_pool::WorkerPoolManager;
use super::auth::{perform_md5_auth, perform_md5_auth_generic};
use super::config::PgWireConfig;
use super::messages::{send_parameter_status, send_parameter_status_generic};
use super::query_loop::{run_query_loop, run_query_loop_generic};
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

/// Check if client connection is still alive
pub(crate) async fn is_client_connected(stream: &tokio::net::TcpStream) -> bool {
    use std::io::ErrorKind;
    
    match stream.peek(&mut [0u8; 0]).await {
        Ok(_) => true,
        Err(e) => {
            match e.kind() {
                ErrorKind::ConnectionReset | ErrorKind::BrokenPipe | ErrorKind::NotConnected => {
                    debug!("Client disconnected: {:?}", e.kind());
                    false
                }
                ErrorKind::WouldBlock => true,
                _ => {
                    debug!("Connection check returned: {:?}", e.kind());
                    true
                }
            }
        }
    }
}

/// Handle connection with optional TLS upgrade
pub(crate) async fn handle_connection_with_tls(
    mut socket: tokio::net::TcpStream,
    auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    worker_client_pool: Arc<WorkerClientPool>,
    query_router: Arc<QueryRouter>,
    pool_manager: Option<Arc<WorkerPoolManager>>,
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
                    
                    return handle_connection_generic(
                        tls_stream, auth_service, worker_client, worker_client_pool,
                        query_router, pool_manager, smart_scaler, query_queue, config, client_ip,
                    ).await;
                } else {
                    debug!("SSL negotiation requested, declining (no TLS config)");
                    socket.write_all(&[b'N']).await?;
                    socket.flush().await?;
                    
                    return handle_connection(
                        socket, auth_service, worker_client, worker_client_pool,
                        query_router, pool_manager, smart_scaler, query_queue, config,
                    ).await;
                }
            }
            80877104 => {
                // GSSENCRequest
                debug!("GSSAPI negotiation requested, declining");
                socket.write_all(&[b'N']).await?;
                socket.flush().await?;
                
                return handle_connection(
                    socket, auth_service, worker_client, worker_client_pool,
                    query_router, pool_manager, smart_scaler, query_queue, config,
                ).await;
            }
            80877102 => {
                // CancelRequest
                let backend_pid = if startup_msg.len() >= 8 {
                    u32::from_be_bytes([startup_msg[4], startup_msg[5], startup_msg[6], startup_msg[7]])
                } else { 0 };
                let _cancel_secret = if startup_msg.len() >= 12 {
                    u32::from_be_bytes([startup_msg[8], startup_msg[9], startup_msg[10], startup_msg[11]])
                } else { 0 };
                
                warn!(
                    backend_pid = backend_pid,
                    "CancelRequest received. Query cancellation not yet fully implemented. \
                     The query will continue running. Use LIMIT clause for large queries."
                );
                
                return Ok(());
            }
            _ => {
                // Normal startup message
            }
        }
    }
    
    handle_connection_with_startup(
        socket, startup_msg, auth_service, worker_client, worker_client_pool,
        query_router, pool_manager, smart_scaler, query_queue, config,
    ).await
}

/// Handle connection after SSL negotiation declined
pub(crate) async fn handle_connection(
    mut socket: tokio::net::TcpStream,
    auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    worker_client_pool: Arc<WorkerClientPool>,
    query_router: Arc<QueryRouter>,
    _pool_manager: Option<Arc<WorkerPoolManager>>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    config: Arc<PgWireConfig>,
) -> anyhow::Result<()> {
    let mut buf = [0u8; 4];
    let mut startup_msg;

    let client_ip = socket.peer_addr().ok().map(|addr| addr.ip().to_string());
    let auth_gateway = auth_service.gateway().cloned();

    loop {
        socket.read_exact(&mut buf).await?;
        let len = u32::from_be_bytes(buf) as usize;
        debug!("Received message, length: {}", len);

        if len < 8 || len > 10000 {
            return Err(anyhow::anyhow!("Invalid message length: {}", len));
        }

        startup_msg = vec![0u8; len - 4];
        socket.read_exact(&mut startup_msg).await?;

        if len == 8 && startup_msg.len() >= 4 {
            let code = u32::from_be_bytes([
                startup_msg[0], startup_msg[1], startup_msg[2], startup_msg[3],
            ]);
            match code {
                80877103 => {
                    debug!("SSL negotiation requested, declining");
                    socket.write_all(&[b'N']).await?;
                    socket.flush().await?;
                    continue;
                }
                80877104 => {
                    debug!("GSSAPI negotiation requested, declining");
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
        break;
    }

    let user_id = extract_startup_param(&startup_msg, "user").unwrap_or_else(|| "anonymous".to_string());
    info!("PostgreSQL client connected as user: {}", user_id);

    let _principal = perform_md5_auth(&mut socket, &user_id, auth_gateway.as_ref(), client_ip).await?;

    send_parameter_status(&mut socket, "server_version", "15.0.0").await?;
    send_parameter_status(&mut socket, "client_encoding", "UTF8").await?;
    send_parameter_status(&mut socket, "server_encoding", "UTF8").await?;
    send_parameter_status(&mut socket, "DateStyle", "ISO, MDY").await?;
    send_parameter_status(&mut socket, "TimeZone", "UTC").await?;
    send_parameter_status(&mut socket, "integer_datetimes", "on").await?;
    send_parameter_status(&mut socket, "standard_conforming_strings", "on").await?;

    let pid = std::process::id();
    let secret = pid.wrapping_mul(1103515245).wrapping_add(12345);
    let mut key_data = vec![b'K', 0, 0, 0, 12];
    key_data.extend_from_slice(&(pid as u32).to_be_bytes());
    key_data.extend_from_slice(&secret.to_be_bytes());
    socket.write_all(&key_data).await?;

    let ready = [b'Z', 0, 0, 0, 5, b'I'];
    socket.write_all(&ready).await?;
    socket.flush().await?;

    run_query_loop(
        &mut socket, &worker_client, &worker_client_pool, &query_router,
        &user_id, smart_scaler.as_ref().map(|s| s.as_ref()), &query_queue, &config,
    ).await
}

/// Handle connection with already-parsed startup message
pub(crate) async fn handle_connection_with_startup(
    mut socket: tokio::net::TcpStream,
    startup_msg: Vec<u8>,
    auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    worker_client_pool: Arc<WorkerClientPool>,
    query_router: Arc<QueryRouter>,
    _pool_manager: Option<Arc<WorkerPoolManager>>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    config: Arc<PgWireConfig>,
) -> anyhow::Result<()> {
    let client_ip = socket.peer_addr().ok().map(|addr| addr.ip().to_string());
    let auth_gateway = auth_service.gateway().cloned();

    let user_id = extract_startup_param(&startup_msg, "user").unwrap_or_else(|| "anonymous".to_string());
    info!("PostgreSQL client connected as user: {}", user_id);

    let _principal = perform_md5_auth(&mut socket, &user_id, auth_gateway.as_ref(), client_ip).await?;

    send_parameter_status(&mut socket, "server_version", "15.0.0").await?;
    send_parameter_status(&mut socket, "client_encoding", "UTF8").await?;
    send_parameter_status(&mut socket, "server_encoding", "UTF8").await?;
    send_parameter_status(&mut socket, "DateStyle", "ISO, MDY").await?;
    send_parameter_status(&mut socket, "TimeZone", "UTC").await?;
    send_parameter_status(&mut socket, "integer_datetimes", "on").await?;
    send_parameter_status(&mut socket, "standard_conforming_strings", "on").await?;

    let pid = std::process::id();
    let secret = pid.wrapping_mul(1103515245).wrapping_add(12345);
    let mut key_data = vec![b'K', 0, 0, 0, 12];
    key_data.extend_from_slice(&(pid as u32).to_be_bytes());
    key_data.extend_from_slice(&secret.to_be_bytes());
    socket.write_all(&key_data).await?;

    let ready = [b'Z', 0, 0, 0, 5, b'I'];
    socket.write_all(&ready).await?;
    socket.flush().await?;

    run_query_loop(&mut socket, &worker_client, &worker_client_pool, &query_router, &user_id, smart_scaler.as_ref().map(|s| s.as_ref()), &query_queue, &config).await
}

/// Generic connection handler for both TLS and non-TLS streams
pub(crate) async fn handle_connection_generic<S>(
    mut socket: S,
    auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    worker_client_pool: Arc<WorkerClientPool>,
    query_router: Arc<QueryRouter>,
    _pool_manager: Option<Arc<WorkerPoolManager>>,
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
    let mut startup_msg;

    loop {
        socket.read_exact(&mut buf).await?;
        let len = u32::from_be_bytes(buf) as usize;
        debug!("Received message (TLS), length: {}", len);

        if len < 8 || len > 10000 {
            return Err(anyhow::anyhow!("Invalid message length: {}", len));
        }

        startup_msg = vec![0u8; len - 4];
        socket.read_exact(&mut startup_msg).await?;

        if len == 8 && startup_msg.len() >= 4 {
            let code = u32::from_be_bytes([
                startup_msg[0], startup_msg[1], startup_msg[2], startup_msg[3],
            ]);
            match code {
                80877103 | 80877104 => {
                    debug!("Nested SSL/GSSAPI request, declining");
                    socket.write_all(&[b'N']).await?;
                    socket.flush().await?;
                    continue;
                }
                80877102 => {
                    warn!("CancelRequest received via TLS (query cancellation not fully implemented)");
                    return Ok(());
                }
                _ => {}
            }
        }
        break;
    }

    let user_id = extract_startup_param(&startup_msg, "user").unwrap_or_else(|| "anonymous".to_string());
    info!("PostgreSQL client connected as user: {} (TLS)", user_id);

    let _principal = perform_md5_auth_generic(&mut socket, &user_id, auth_gateway.as_ref(), client_ip).await?;

    send_parameter_status_generic(&mut socket, "server_version", "15.0.0").await?;
    send_parameter_status_generic(&mut socket, "client_encoding", "UTF8").await?;
    send_parameter_status_generic(&mut socket, "server_encoding", "UTF8").await?;
    send_parameter_status_generic(&mut socket, "DateStyle", "ISO, MDY").await?;
    send_parameter_status_generic(&mut socket, "TimeZone", "UTC").await?;
    send_parameter_status_generic(&mut socket, "integer_datetimes", "on").await?;
    send_parameter_status_generic(&mut socket, "standard_conforming_strings", "on").await?;

    let pid = std::process::id();
    let secret = pid.wrapping_mul(1103515245).wrapping_add(12345);
    let mut key_data = vec![b'K', 0, 0, 0, 12];
    key_data.extend_from_slice(&(pid as u32).to_be_bytes());
    key_data.extend_from_slice(&secret.to_be_bytes());
    socket.write_all(&key_data).await?;

    let ready = [b'Z', 0, 0, 0, 5, b'I'];
    socket.write_all(&ready).await?;
    socket.flush().await?;

    run_query_loop_generic(&mut socket, &worker_client, &worker_client_pool, &query_router, &user_id, smart_scaler.as_ref().map(|s| s.as_ref()), &query_queue, &config).await
}

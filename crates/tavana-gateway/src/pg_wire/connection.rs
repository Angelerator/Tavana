//! Connection utilities for PostgreSQL wire protocol
//!
//! TCP keepalive configuration and client connectivity checks.

use socket2::SockRef;
use std::time::Duration;
use tracing::{debug, warn};

/// Configure TCP keepalive on a socket
/// This helps detect dead connections faster than relying on TCP defaults
pub fn configure_tcp_keepalive(stream: &tokio::net::TcpStream, keepalive_secs: u64) {
    // Set TCP_NODELAY for low latency
    if let Err(e) = stream.set_nodelay(true) {
        warn!("Failed to set TCP_NODELAY: {}", e);
    }

    // Configure TCP keepalive using socket2
    let socket = SockRef::from(stream);

    // Enable keepalive
    if let Err(e) = socket.set_keepalive(true) {
        warn!("Failed to enable TCP keepalive: {}", e);
        return;
    }

    // Configure keepalive timing
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(keepalive_secs))
        .with_interval(Duration::from_secs(keepalive_secs / 2 + 1));

    // Add retries on platforms that support it
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let keepalive = keepalive.with_retries(3);

    if let Err(e) = socket.set_tcp_keepalive(&keepalive) {
        warn!("Failed to configure TCP keepalive timing: {}", e);
    } else {
        debug!("TCP keepalive configured: {}s", keepalive_secs);
    }
}

/// Check if client connection is still alive by attempting a zero-byte peek
/// Returns true if connection is alive, false if disconnected
pub async fn is_client_connected(stream: &tokio::net::TcpStream) -> bool {
    use std::io::ErrorKind;

    // Try to peek at the socket to check connection state
    // If the client has disconnected, this will return an error
    match stream.peek(&mut [0u8; 0]).await {
        Ok(_) => true,
        Err(e) => match e.kind() {
            // Connection reset or broken pipe = definitely disconnected
            ErrorKind::ConnectionReset | ErrorKind::BrokenPipe | ErrorKind::NotConnected => {
                debug!("Client disconnected: {:?}", e.kind());
                false
            }
            // WouldBlock is expected for non-blocking sockets with no data
            ErrorKind::WouldBlock => true,
            // Other errors might be transient
            _ => {
                debug!("Connection check returned: {:?}", e.kind());
                true
            }
        },
    }
}

/// Result of streaming query execution (for metrics tracking)
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct StreamingResult {
    /// Total rows streamed to client
    pub rows: usize,
    /// Total bytes streamed to client
    pub bytes: usize,
}

/// Portal state for Extended Query Protocol cursor streaming
/// Stores buffered rows and offset for resumption after PortalSuspended
#[derive(Debug)]
pub struct PortalState {
    pub rows: Vec<Vec<String>>,
    pub columns: Vec<(String, String)>,
    pub offset: usize,
}

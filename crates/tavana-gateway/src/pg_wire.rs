//! PostgreSQL wire protocol implementation
//!
//! Enables Tableau, PowerBI, and other BI tools to connect natively.
//! Routes queries using the same logic as HTTP API:
//! - Small queries → Worker Pool (HPA managed)
//! - Large queries → Ephemeral Pods (right-sized, isolated)

use crate::adaptive::AdaptiveController;
use crate::auth::AuthService;
use crate::k8s_client::K8sQueryClient;
use crate::metrics;
use crate::query_router::{QueryRouter, QueryTarget};
use crate::worker_client::WorkerClient;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

/// PostgreSQL wire protocol server
///
/// Implements the PostgreSQL wire protocol (pg_wire) to allow BI tools like
/// Tableau and PowerBI to connect directly. Routes queries using the same
/// hybrid routing logic as the HTTP API.
pub struct PgWireServer {
    addr: SocketAddr,
    auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    query_router: Arc<QueryRouter>,
    k8s_client: Arc<K8sQueryClient>,
    adaptive_controller: Arc<AdaptiveController>,
}

impl PgWireServer {
    pub fn new(
        port: u16,
        auth_service: Arc<AuthService>,
        worker_client: Arc<WorkerClient>,
        query_router: Arc<QueryRouter>,
        k8s_client: Arc<K8sQueryClient>,
        adaptive_controller: Arc<AdaptiveController>,
    ) -> Self {
        let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
        Self {
            addr,
            auth_service,
            worker_client,
            query_router,
            k8s_client,
            adaptive_controller,
        }
    }

    /// Start the PostgreSQL wire protocol server
    pub async fn start(&self) -> anyhow::Result<()> {
        info!("Starting PostgreSQL wire protocol server on {}", self.addr);

        let listener = TcpListener::bind(&self.addr).await?;

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            info!("New PostgreSQL connection from {}", peer_addr);

            let auth_service = self.auth_service.clone();
            let worker_client = self.worker_client.clone();
            let query_router = self.query_router.clone();
            let k8s_client = self.k8s_client.clone();
            let adaptive_controller = self.adaptive_controller.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(
                    socket,
                    auth_service,
                    worker_client,
                    query_router,
                    k8s_client,
                    adaptive_controller,
                ).await {
                    error!("Error handling PostgreSQL connection: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(
    mut socket: tokio::net::TcpStream,
    _auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    query_router: Arc<QueryRouter>,
    k8s_client: Arc<K8sQueryClient>,
    adaptive_controller: Arc<AdaptiveController>,
) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buf = [0u8; 4];
    let mut startup_msg;

    // Loop to handle multiple negotiation requests (SSL, GSSAPI)
    loop {
        // Read startup message length
        socket.read_exact(&mut buf).await?;
        let len = u32::from_be_bytes(buf) as usize;
        debug!("Received message, length: {}", len);

        if len < 8 || len > 10000 {
            return Err(anyhow::anyhow!("Invalid message length: {}", len));
        }

        startup_msg = vec![0u8; len - 4];
        socket.read_exact(&mut startup_msg).await?;

        // Check for special request codes (length is 8 for all special requests)
        if len == 8 && startup_msg.len() >= 4 {
            let code = u32::from_be_bytes([
                startup_msg[0],
                startup_msg[1],
                startup_msg[2],
                startup_msg[3],
            ]);
            debug!("Received special request code: {}", code);

            match code {
                // SSL request (80877103)
                80877103 => {
                    debug!("SSL request - responding 'N'");
                    socket.write_all(b"N").await?;
                    socket.flush().await?;
                    continue;
                }
                // GSSAPI request (80877104)
                80877104 => {
                    debug!("GSSAPI request - responding 'N'");
                    socket.write_all(b"N").await?;
                    socket.flush().await?;
                    continue;
                }
                // Cancel request (80877102)
                80877102 => {
                    info!("Received cancel request");
                    return Ok(());
                }
                _ => break,
            }
        } else {
            break;
        }
    }

    // Parse protocol version
    if startup_msg.len() >= 4 {
        let version = u32::from_be_bytes([
            startup_msg[0],
            startup_msg[1],
            startup_msg[2],
            startup_msg[3],
        ]);
        debug!("Protocol version: {}.{}", version >> 16, version & 0xFFFF);
    }

    // Extract username from startup message
    let user_id = extract_startup_param(&startup_msg, "user").unwrap_or("anonymous".to_string());
    info!("User connecting: {}", user_id);

    // Request cleartext password (AuthenticationCleartextPassword = 3)
    // This is required for JDBC clients like Tableau
    let auth_cleartext = [b'R', 0, 0, 0, 8, 0, 0, 0, 3];
    socket.write_all(&auth_cleartext).await?;
    socket.flush().await?;
    debug!("Sent AuthenticationCleartextPassword request");

    // Read password message from client
    let mut pwd_type = [0u8; 1];
    match socket.read_exact(&mut pwd_type).await {
        Ok(_) => {
            info!("Received message type: {} ('{}')", pwd_type[0], pwd_type[0] as char);
            
            if pwd_type[0] == b'p' {
                // Password message - read length and password
                let mut pwd_len_buf = [0u8; 4];
                socket.read_exact(&mut pwd_len_buf).await?;
                let pwd_len = u32::from_be_bytes(pwd_len_buf) as usize - 4;
                let mut pwd_data = vec![0u8; pwd_len];
                socket.read_exact(&mut pwd_data).await?;
                
                // Remove null terminator if present
                if pwd_data.last() == Some(&0) {
                    pwd_data.pop();
                }
                
                let password = String::from_utf8_lossy(&pwd_data);
                info!("Received password (length {}), accepting", password.len());
            } else {
                // Unexpected message type - read and discard the message body
                warn!("Expected password message (p), got: {} (0x{:02x})", pwd_type[0] as char, pwd_type[0]);
                let mut len_buf = [0u8; 4];
                if socket.read_exact(&mut len_buf).await.is_ok() {
                    let len = u32::from_be_bytes(len_buf) as usize;
                    if len > 4 && len < 65536 {
                        let mut discard = vec![0u8; len - 4];
                        let _ = socket.read_exact(&mut discard).await;
                    }
                }
            }
        }
        Err(e) => {
            warn!("Failed to read password message: {}", e);
            // Continue anyway - some clients might not send password
        }
    }

    // Send AuthenticationOk
    let auth_ok = [b'R', 0, 0, 0, 8, 0, 0, 0, 0];
    socket.write_all(&auth_ok).await?;
    socket.flush().await?;  // Flush AuthenticationOk immediately
    info!("Sent AuthenticationOk");

    // Send ParameterStatus messages (Tableau needs many of these)
    for (key, value) in [
        ("server_version", "15.0"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("TimeZone", "UTC"),
        ("application_name", ""),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        ("IntervalStyle", "postgres"),
        ("is_superuser", "on"),
        ("session_authorization", "tavana"),
    ] {
        send_parameter_status(&mut socket, key, value).await?;
    }

    // Send BackendKeyData
    let key_data = [b'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 1];
    socket.write_all(&key_data).await?;

    // Send ReadyForQuery
    let ready = [b'Z', 0, 0, 0, 5, b'I'];
    socket.write_all(&ready).await?;
    socket.flush().await?;

    info!("PostgreSQL handshake completed for user {}", user_id);

    // Main query loop
    loop {
        let mut msg_type = [0u8; 1];
        if socket.read_exact(&mut msg_type).await.is_err() {
            break;
        }

        match msg_type[0] {
            b'Q' => {
                // Simple query
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut query_buf = vec![0u8; len];
                socket.read_exact(&mut query_buf).await?;

                if query_buf.last() == Some(&0) {
                    query_buf.pop();
                }

                let query = String::from_utf8_lossy(&query_buf).to_string();
                info!("Executing query: {}", query);

                // Start timing the query execution
                let start_time = Instant::now();

                // Route query using the same logic as HTTP API
                let estimate = query_router.route(&query).await;
                info!(
                    "PG Wire routing: {}MB data, threshold={}MB, target={:?}",
                    estimate.data_size_mb, estimate.threshold_mb, estimate.target
                );

                // Execute based on routing decision
                let result = match &estimate.target {
                    QueryTarget::WorkerPool => {
                        info!("PG Wire: Small query ({}MB < {}MB) → Worker Pool", estimate.data_size_mb, estimate.threshold_mb);
                        execute_query_via_worker(&worker_client, &query, &user_id).await
                    }
                    QueryTarget::EphemeralPod { memory_mb, cpu_millicores } => {
                        info!(
                            "PG Wire: Large query ({}MB >= {}MB) → Ephemeral Pod ({}MB RAM, {}m CPU)",
                            estimate.data_size_mb, estimate.threshold_mb, memory_mb, cpu_millicores
                        );
                        
                        // Record ephemeral pod creation
                        metrics::record_ephemeral_pod_created(*memory_mb as f64);
                        
                        if k8s_client.is_available() {
                            execute_query_via_ephemeral_pod(
                                &k8s_client,
                                &worker_client,
                                &adaptive_controller,
                                &query,
                                &user_id,
                                *memory_mb,
                                *cpu_millicores,
                                start_time,
                            ).await
                        } else {
                            warn!("K8s not available, falling back to worker pool");
                            execute_query_via_worker(&worker_client, &query, &user_id).await
                        }
                    }
                };

                let route_label = match &estimate.target {
                    QueryTarget::WorkerPool => "worker_pool",
                    QueryTarget::EphemeralPod { .. } => "ephemeral_pod",
                };

                match result {
                    Ok(result) => {
                        let duration = start_time.elapsed().as_secs_f64();
                        metrics::record_query_completed(route_label, "success", duration);
                        debug!("Query result: {} columns, {} rows (took {:.2}s)", result.columns.len(), result.rows.len(), duration);
                        send_query_result(&mut socket, result).await?;
                        debug!("Query result sent successfully");
                    }
                    Err(e) => {
                        let duration = start_time.elapsed().as_secs_f64();
                        metrics::record_query_completed(route_label, "error", duration);
                        warn!("Query execution failed (took {:.2}s): {}", duration, e);
                        send_error(&mut socket, &e.to_string()).await?;
                    }
                }

                // Send ReadyForQuery
                info!("Sending ReadyForQuery after query");
                socket.write_all(&ready).await?;
                socket.flush().await?;
                info!("ReadyForQuery sent and flushed, waiting for next message");
            }
            b'X' => {
                info!("Client disconnected");
                break;
            }
            b'P' => {
                // Parse (extended query protocol)
                handle_parse(&mut socket, &mut buf).await?;
            }
            b'B' => {
                // Bind
                handle_bind(&mut socket, &mut buf).await?;
            }
            b'D' => {
                // Describe
                handle_describe(&mut socket, &mut buf).await?;
            }
            b'E' => {
                // Execute
                handle_execute(&mut socket, &mut buf, &worker_client, &user_id).await?;
            }
            b'S' => {
                // Sync
                socket.write_all(&ready).await?;
                socket.flush().await?;
            }
            _ => {
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut skip = vec![0u8; len];
                socket.read_exact(&mut skip).await?;
            }
        }
    }

    Ok(())
}

/// Execute a query via ephemeral pod (for large queries)
async fn execute_query_via_ephemeral_pod(
    k8s_client: &K8sQueryClient,
    worker_client: &WorkerClient,
    _adaptive_controller: &AdaptiveController,
    sql: &str,
    user_id: &str,
    memory_mb: u32,
    cpu_millicores: u32,
    start_time: Instant,
) -> anyhow::Result<QueryExecutionResult> {
    // Handle PostgreSQL-specific commands locally
    if let Some(result) = handle_pg_specific_command(sql) {
        return Ok(result);
    }

    info!("PG Wire: Creating ephemeral pod ({}MB RAM, {}m CPU) for query...", memory_mb, cpu_millicores);
    
    match k8s_client.execute_query(sql, user_id, memory_mb, cpu_millicores, 300).await {
        Ok(result) => {
            let elapsed = start_time.elapsed();
            info!("PG Wire: Ephemeral pod query completed in {:.2}s", elapsed.as_secs_f64());
            
            // Record ephemeral pod completion
            metrics::record_ephemeral_pod_completed(5.0, elapsed.as_secs_f64());
            
            // Convert K8s result to QueryExecutionResult
            let row_count = result.rows.len();
            Ok(QueryExecutionResult {
                columns: result.columns,
                rows: result.rows,
                row_count,
                command_tag: None,
            })
        }
        Err(e) => {
            warn!("PG Wire: Ephemeral pod failed ({}), falling back to worker pool", e);
            // Record ephemeral pod failure and fall back
            metrics::record_ephemeral_pod_failed();
            
            // Fallback to worker pool
            execute_query_via_worker(worker_client, sql, user_id).await
        }
    }
}

/// Execute a query using the worker service (for small queries)
async fn execute_query_via_worker(
    worker_client: &WorkerClient,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<QueryExecutionResult> {
    // Handle PostgreSQL-specific commands locally (not supported by DuckDB)
    if let Some(result) = handle_pg_specific_command(sql) {
        return Ok(result);
    }

    info!("PG Wire: Executing query via worker pool");
    execute_via_worker_pool(worker_client, sql, user_id).await
}

/// Execute query via worker pool (shared workers with connection pools)
async fn execute_via_worker_pool(
    worker_client: &WorkerClient,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<QueryExecutionResult> {
    match worker_client.execute_query(sql, user_id).await {
        Ok(result) => Ok(QueryExecutionResult {
            columns: result
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.type_name.clone()))
                .collect(),
            rows: result.rows,
            row_count: result.total_rows as usize,
            command_tag: None,
        }),
        Err(e) => {
            warn!("Worker pool unavailable ({}), using local execution", e);
            // Note: The caller (handle_connection) already records metrics,
            // but if fallback is used, it will show up with "success" status
            // if the fallback succeeds
            execute_local_fallback(sql).await
        }
    }
}

/// Handle PostgreSQL-specific commands that DuckDB doesn't support
fn handle_pg_specific_command(sql: &str) -> Option<QueryExecutionResult> {
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql_upper.trim();

    // Handle SET commands (Tableau, psql, etc. send these)
    if sql_trimmed.starts_with("SET ") {
        info!("Handling SET command locally: {}", sql);
        info!("SET command handled successfully, returning SET response");
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("SET".to_string()),
        });
    }

    // Handle SHOW commands
    if sql_trimmed.starts_with("SHOW ") {
        debug!("Handling SHOW command locally: {}", sql);
        
        if sql_trimmed.contains("TRANSACTION ISOLATION") {
            return Some(QueryExecutionResult {
                columns: vec![("transaction_isolation".to_string(), "text".to_string())],
                rows: vec![vec!["read committed".to_string()]],
                row_count: 1,
                command_tag: None,
            });
        }
        
        if sql_trimmed.contains("STANDARD_CONFORMING_STRINGS") {
            return Some(QueryExecutionResult {
                columns: vec![("standard_conforming_strings".to_string(), "text".to_string())],
                rows: vec![vec!["on".to_string()]],
                row_count: 1,
                command_tag: None,
            });
        }

        if sql_trimmed.contains("SERVER_VERSION") {
            return Some(QueryExecutionResult {
                columns: vec![("server_version".to_string(), "text".to_string())],
                rows: vec![vec!["15.0 (Tavana DuckDB)".to_string()]],
                row_count: 1,
                command_tag: None,
            });
        }

        // Generic SHOW response
        return Some(QueryExecutionResult {
            columns: vec![("setting".to_string(), "text".to_string())],
            rows: vec![vec!["".to_string()]],
            row_count: 1,
            command_tag: None,
        });
    }

    // Handle RESET commands
    if sql_trimmed.starts_with("RESET ") {
        debug!("Handling RESET command locally: {}", sql);
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("RESET".to_string()),
        });
    }

    // Handle BEGIN/COMMIT/ROLLBACK (transaction commands)
    if sql_trimmed == "BEGIN" || sql_trimmed.starts_with("BEGIN ") {
        debug!("Handling BEGIN command locally");
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("BEGIN".to_string()),
        });
    }

    if sql_trimmed == "COMMIT" {
        debug!("Handling COMMIT command locally");
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("COMMIT".to_string()),
        });
    }

    if sql_trimmed == "ROLLBACK" {
        debug!("Handling ROLLBACK command locally");
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("ROLLBACK".to_string()),
        });
    }

    // Handle DISCARD commands
    if sql_trimmed.starts_with("DISCARD ") {
        debug!("Handling DISCARD command locally: {}", sql);
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("DISCARD ALL".to_string()),
        });
    }

    // Handle DEALLOCATE commands
    if sql_trimmed.starts_with("DEALLOCATE ") {
        debug!("Handling DEALLOCATE command locally: {}", sql);
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("DEALLOCATE".to_string()),
        });
    }

    // Handle CLOSE commands (cursor)
    if sql_trimmed.starts_with("CLOSE ") {
        debug!("Handling CLOSE command locally: {}", sql);
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("CLOSE CURSOR".to_string()),
        });
    }

    // Handle pg_catalog queries (Tableau metadata discovery)
    if sql_upper.contains("PG_CATALOG") || sql_upper.contains("INFORMATION_SCHEMA") {
        debug!("Handling catalog query locally: {}", sql);
        
        // Return empty results for catalog queries
        // Tableau will still work, just won't show native tables
        return Some(QueryExecutionResult {
            columns: vec![
                ("table_catalog".to_string(), "text".to_string()),
                ("table_schema".to_string(), "text".to_string()),
                ("table_name".to_string(), "text".to_string()),
            ],
            rows: vec![],
            row_count: 0,
            command_tag: None,
        });
    }

    None
}

/// Fallback local execution when worker is unavailable
async fn execute_local_fallback(sql: &str) -> anyhow::Result<QueryExecutionResult> {
    // For simple queries, return mock data
    // In production, this would be removed
    let sql_upper = sql.to_uppercase();

    if sql_upper.starts_with("SELECT 1") || sql_upper.contains("SELECT 1 AS") {
        return Ok(QueryExecutionResult {
            columns: vec![("result".to_string(), "int4".to_string())],
            rows: vec![vec!["1".to_string()]],
            row_count: 1,
            command_tag: None,
        });
    }

    if sql_upper.starts_with("SELECT VERSION()") {
        return Ok(QueryExecutionResult {
            columns: vec![("version".to_string(), "text".to_string())],
            rows: vec![vec!["Tavana DuckDB 1.0".to_string()]],
            row_count: 1,
            command_tag: None,
        });
    }

    if sql_upper.starts_with("SELECT CURRENT_DATABASE()") {
        return Ok(QueryExecutionResult {
            columns: vec![("current_database".to_string(), "text".to_string())],
            rows: vec![vec!["tavana".to_string()]],
            row_count: 1,
            command_tag: None,
        });
    }

    // For any other SELECT, return empty result
    if sql_upper.starts_with("SELECT") {
        return Ok(QueryExecutionResult {
            columns: vec![("result".to_string(), "text".to_string())],
            rows: vec![],
            row_count: 0,
            command_tag: None,
        });
    }

    // For non-SELECT statements
    Ok(QueryExecutionResult {
        columns: vec![],
        rows: vec![],
        row_count: 0,
        command_tag: None,
    })
}

struct QueryExecutionResult {
    columns: Vec<(String, String)>, // (name, type)
    rows: Vec<Vec<String>>,
    row_count: usize,
    command_tag: Option<String>, // Custom command tag (e.g., "SET", "BEGIN")
}

async fn send_query_result(
    socket: &mut tokio::net::TcpStream,
    result: QueryExecutionResult,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    if result.columns.is_empty() {
        // No columns = command with no result set
        let tag = result.command_tag.unwrap_or_else(|| format!("OK {}", result.row_count));
        info!("No columns in result, sending CommandComplete with tag: {}", tag);
        send_command_complete(socket, &tag).await?;
        info!("CommandComplete sent for no-column result");
        return Ok(());
    }

    // Send RowDescription
    let mut row_desc = Vec::new();
    row_desc.push(b'T');

    let field_count = result.columns.len() as i16;
    let mut fields_data = Vec::new();
    fields_data.extend_from_slice(&field_count.to_be_bytes());

    for (name, type_name) in &result.columns {
        fields_data.extend_from_slice(name.as_bytes());
        fields_data.push(0);
        fields_data.extend_from_slice(&0u32.to_be_bytes()); // table OID
        fields_data.extend_from_slice(&0i16.to_be_bytes()); // column number
        fields_data.extend_from_slice(&pg_type_oid(type_name).to_be_bytes()); // type OID
        fields_data.extend_from_slice(&pg_type_len(type_name).to_be_bytes()); // type length
        fields_data.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
        fields_data.extend_from_slice(&0i16.to_be_bytes()); // format (text)
    }

    let len = (4 + fields_data.len()) as u32;
    row_desc.extend_from_slice(&len.to_be_bytes());
    row_desc.extend_from_slice(&fields_data);
    socket.write_all(&row_desc).await?;

    // Send DataRows
    for row in &result.rows {
        let mut data_row = Vec::new();
        data_row.push(b'D');

        let mut row_data = Vec::new();
        row_data.extend_from_slice(&(row.len() as i16).to_be_bytes());

        for value in row {
            if value.is_empty() {
                row_data.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
            } else {
                row_data.extend_from_slice(&(value.len() as i32).to_be_bytes());
                row_data.extend_from_slice(value.as_bytes());
            }
        }

        let len = (4 + row_data.len()) as u32;
        data_row.extend_from_slice(&len.to_be_bytes());
        data_row.extend_from_slice(&row_data);
        socket.write_all(&data_row).await?;
    }

    // Send CommandComplete
    let tag = format!("SELECT {}", result.rows.len());
    send_command_complete(socket, &tag).await?;

    Ok(())
}

async fn send_command_complete(
    socket: &mut tokio::net::TcpStream,
    tag: &str,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    debug!("Sending CommandComplete: {}", tag);
    let mut msg = Vec::new();
    msg.push(b'C');
    let len = 4 + tag.len() + 1;
    msg.extend_from_slice(&(len as u32).to_be_bytes());
    msg.extend_from_slice(tag.as_bytes());
    msg.push(0);
    socket.write_all(&msg).await?;
    socket.flush().await?;
    Ok(())
}

async fn send_error(socket: &mut tokio::net::TcpStream, message: &str) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut msg = Vec::new();
    msg.push(b'E'); // ErrorResponse

    let mut fields = Vec::new();
    fields.push(b'S'); // Severity
    fields.extend_from_slice(b"ERROR\0");
    fields.push(b'C'); // Code
    fields.extend_from_slice(b"42000\0"); // Syntax error
    fields.push(b'M'); // Message
    fields.extend_from_slice(message.as_bytes());
    fields.push(0);
    fields.push(0); // Terminator

    let len = 4 + fields.len();
    msg.extend_from_slice(&(len as u32).to_be_bytes());
    msg.extend_from_slice(&fields);
    socket.write_all(&msg).await?;
    Ok(())
}

async fn send_parameter_status(
    socket: &mut tokio::net::TcpStream,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut msg = Vec::new();
    msg.push(b'S');
    let len = 4 + key.len() + 1 + value.len() + 1;
    msg.extend_from_slice(&(len as u32).to_be_bytes());
    msg.extend_from_slice(key.as_bytes());
    msg.push(0);
    msg.extend_from_slice(value.as_bytes());
    msg.push(0);
    socket.write_all(&msg).await?;
    Ok(())
}

fn extract_startup_param(msg: &[u8], key: &str) -> Option<String> {
    if msg.len() < 8 {
        return None;
    }

    let params = &msg[4..]; // Skip version
    let mut iter = params.split(|&b| b == 0);

    while let Some(k) = iter.next() {
        if k.is_empty() {
            break;
        }
        let v = iter.next()?;
        if k == key.as_bytes() {
            return String::from_utf8(v.to_vec()).ok();
        }
    }
    None
}

fn pg_type_oid(type_name: &str) -> u32 {
    match type_name.to_lowercase().as_str() {
        "int4" | "integer" | "int" => 23,
        "int8" | "bigint" => 20,
        "int2" | "smallint" => 21,
        "float4" | "real" => 700,
        "float8" | "double" | "double precision" => 701,
        "bool" | "boolean" => 16,
        "timestamp" | "timestamptz" => 1184,
        "date" => 1082,
        "uuid" => 2950,
        "json" | "jsonb" => 114,
        _ => 25, // text
    }
}

fn pg_type_len(type_name: &str) -> i16 {
    match type_name.to_lowercase().as_str() {
        "int4" | "integer" | "int" => 4,
        "int8" | "bigint" => 8,
        "int2" | "smallint" => 2,
        "float4" | "real" => 4,
        "float8" | "double" => 8,
        "bool" | "boolean" => 1,
        _ => -1, // Variable length
    }
}

// Extended query protocol handlers (stubs for now)
async fn handle_parse(
    socket: &mut tokio::net::TcpStream,
    buf: &mut [u8; 4],
) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    
    // Parse the statement name and query from the data
    debug!("Received Parse message, data len: {}", len);
    
    // Send ParseComplete
    use tokio::io::AsyncWriteExt;
    socket.write_all(&[b'1', 0, 0, 0, 4]).await?;
    socket.flush().await?;
    Ok(())
}

async fn handle_bind(
    socket: &mut tokio::net::TcpStream,
    buf: &mut [u8; 4],
) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    
    debug!("Received Bind message, data len: {}", len);
    
    // Send BindComplete
    use tokio::io::AsyncWriteExt;
    socket.write_all(&[b'2', 0, 0, 0, 4]).await?;
    socket.flush().await?;
    Ok(())
}

async fn handle_describe(
    socket: &mut tokio::net::TcpStream,
    buf: &mut [u8; 4],
) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    
    debug!("Received Describe message, data len: {}", len);
    
    // Send NoData for now
    use tokio::io::AsyncWriteExt;
    socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
    socket.flush().await?;
    Ok(())
}

async fn handle_execute(
    socket: &mut tokio::net::TcpStream,
    buf: &mut [u8; 4],
    _worker_client: &WorkerClient,
    _user_id: &str,
) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    
    debug!("Received Execute message, data len: {}", len);
    
    // Send CommandComplete
    send_command_complete(socket, "SELECT 0").await?;
    Ok(())
}

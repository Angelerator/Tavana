//! PostgreSQL wire protocol implementation
//!
//! Enables Tableau, PowerBI, DBeaver and other BI tools to connect natively.
//! All queries are routed to the HPA+VPA managed worker pool.

use crate::auth::AuthService;
use crate::metrics;
use crate::query_router::QueryRouter;
use crate::worker_client::WorkerClient;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

/// PostgreSQL wire protocol server
pub struct PgWireServer {
    addr: SocketAddr,
    auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    query_router: Arc<QueryRouter>,
}

impl PgWireServer {
    pub fn new(
        port: u16,
        auth_service: Arc<AuthService>,
        worker_client: Arc<WorkerClient>,
        query_router: Arc<QueryRouter>,
    ) -> Self {
        let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
        Self {
            addr,
            auth_service,
            worker_client,
            query_router,
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

            tokio::spawn(async move {
                if let Err(e) = handle_connection(
                    socket,
                    auth_service,
                    worker_client,
                    query_router,
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
) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buf = [0u8; 4];
    let mut startup_msg;

    // Loop to handle multiple negotiation requests (SSL, GSSAPI)
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
                startup_msg[0],
                startup_msg[1],
                startup_msg[2],
                startup_msg[3],
            ]);
            debug!("Received special request code: {}", code);

            match code {
                80877103 => { // SSL request
                    debug!("SSL request - responding 'N'");
                    socket.write_all(b"N").await?;
                    socket.flush().await?;
                    continue;
                }
                80877104 => { // GSSAPI request
                    debug!("GSSAPI request - responding 'N'");
                    socket.write_all(b"N").await?;
                    socket.flush().await?;
                    continue;
                }
                80877102 => { // Cancel request
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

    let user_id = extract_startup_param(&startup_msg, "user").unwrap_or("anonymous".to_string());
    info!("User connecting: {}", user_id);

    // Request cleartext password
    let auth_cleartext = [b'R', 0, 0, 0, 8, 0, 0, 0, 3];
    socket.write_all(&auth_cleartext).await?;
    socket.flush().await?;

    // Read password message
    let mut pwd_type = [0u8; 1];
    match socket.read_exact(&mut pwd_type).await {
        Ok(_) => {
            if pwd_type[0] == b'p' {
                let mut pwd_len_buf = [0u8; 4];
                socket.read_exact(&mut pwd_len_buf).await?;
                let pwd_len = u32::from_be_bytes(pwd_len_buf) as usize - 4;
                let mut pwd_data = vec![0u8; pwd_len];
                socket.read_exact(&mut pwd_data).await?;
                info!("Password received, accepting");
            } else {
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
        Err(_) => {}
    }

    // Send AuthenticationOk
    let auth_ok = [b'R', 0, 0, 0, 8, 0, 0, 0, 0];
    socket.write_all(&auth_ok).await?;
    socket.flush().await?;

    // Send ParameterStatus messages
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

                let start_time = Instant::now();

                // Route query (all go to worker pool now)
                let estimate = query_router.route(&query).await;
                info!("Query routed: {}MB data → Worker Pool", estimate.data_size_mb);

                // Execute via worker pool
                let result = execute_query_via_worker(&worker_client, &query, &user_id).await;

                match result {
                    Ok(result) => {
                        let duration = start_time.elapsed().as_secs_f64();
                        metrics::record_query_completed("worker_pool", "success", duration);
                        
                        let estimated_bytes = (result.rows.len() * result.columns.len() * 50) as u64;
                        let estimated_mb = estimated_bytes as f64 / (1024.0 * 1024.0);
                        metrics::record_data_scanned(estimated_bytes);
                        metrics::record_actual_query_size(estimated_mb);
                        
                        debug!("Query result: {} columns, {} rows (took {:.2}s)", 
                            result.columns.len(), result.rows.len(), duration);
                        send_query_result(&mut socket, result).await?;
                    }
                    Err(e) => {
                        let duration = start_time.elapsed().as_secs_f64();
                        metrics::record_query_completed("worker_pool", "error", duration);
                        warn!("Query execution failed: {}", e);
                        send_error(&mut socket, &e.to_string()).await?;
                    }
                }

                socket.write_all(&ready).await?;
                socket.flush().await?;
            }
            b'X' => {
                info!("Client disconnected");
                break;
            }
            b'P' => handle_parse(&mut socket, &mut buf).await?,
            b'B' => handle_bind(&mut socket, &mut buf).await?,
            b'D' => handle_describe(&mut socket, &mut buf).await?,
            b'E' => handle_execute(&mut socket, &mut buf, &worker_client, &user_id).await?,
            b'S' => {
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

/// Execute a query using the worker service
async fn execute_query_via_worker(
    worker_client: &WorkerClient,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<QueryExecutionResult> {
    // Handle PostgreSQL-specific commands locally
    if let Some(result) = handle_pg_specific_command(sql) {
        return Ok(result);
    }

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
            warn!("Worker pool error: {}", e);
            execute_local_fallback(sql).await
        }
    }
}

/// Handle PostgreSQL-specific commands that DuckDB doesn't support
fn handle_pg_specific_command(sql: &str) -> Option<QueryExecutionResult> {
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql_upper.trim();

    if sql_trimmed.starts_with("SET ") {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("SET".to_string()),
        });
    }

    if sql_trimmed.starts_with("SHOW ") {
        if sql_trimmed.contains("TRANSACTION ISOLATION") {
            return Some(QueryExecutionResult {
                columns: vec![("transaction_isolation".to_string(), "text".to_string())],
                rows: vec![vec!["read committed".to_string()]],
                row_count: 1,
                command_tag: None,
            });
        }
        return Some(QueryExecutionResult {
            columns: vec![("setting".to_string(), "text".to_string())],
            rows: vec![vec!["".to_string()]],
            row_count: 1,
            command_tag: None,
        });
    }

    if sql_trimmed.starts_with("RESET ") {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("RESET".to_string()),
        });
    }

    if sql_trimmed == "BEGIN" || sql_trimmed.starts_with("BEGIN ") {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("BEGIN".to_string()),
        });
    }

    if sql_trimmed == "COMMIT" {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("COMMIT".to_string()),
        });
    }

    if sql_trimmed == "ROLLBACK" {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("ROLLBACK".to_string()),
        });
    }

    if sql_trimmed.starts_with("DISCARD ") || sql_trimmed.starts_with("DEALLOCATE ") || sql_trimmed.starts_with("CLOSE ") {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("OK".to_string()),
        });
    }

    if sql_upper.contains("PG_CATALOG") || sql_upper.contains("INFORMATION_SCHEMA") {
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

/// Fallback local execution
async fn execute_local_fallback(sql: &str) -> anyhow::Result<QueryExecutionResult> {
    let sql_upper = sql.to_uppercase();

    if sql_upper.starts_with("SELECT 1") {
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

    Ok(QueryExecutionResult {
        columns: vec![("result".to_string(), "text".to_string())],
        rows: vec![],
        row_count: 0,
        command_tag: None,
    })
}

struct QueryExecutionResult {
    columns: Vec<(String, String)>,
    rows: Vec<Vec<String>>,
    row_count: usize,
    command_tag: Option<String>,
}

async fn send_query_result(
    socket: &mut tokio::net::TcpStream,
    result: QueryExecutionResult,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    if result.columns.is_empty() {
        let tag = result.command_tag.unwrap_or_else(|| format!("OK {}", result.row_count));
        send_command_complete(socket, &tag).await?;
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
        fields_data.extend_from_slice(&0u32.to_be_bytes());
        fields_data.extend_from_slice(&0i16.to_be_bytes());
        fields_data.extend_from_slice(&pg_type_oid(type_name).to_be_bytes());
        fields_data.extend_from_slice(&pg_type_len(type_name).to_be_bytes());
        fields_data.extend_from_slice(&(-1i32).to_be_bytes());
        fields_data.extend_from_slice(&0i16.to_be_bytes());
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
                row_data.extend_from_slice(&(-1i32).to_be_bytes());
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

    let tag = format!("SELECT {}", result.rows.len());
    send_command_complete(socket, &tag).await?;

    Ok(())
}

async fn send_command_complete(socket: &mut tokio::net::TcpStream, tag: &str) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
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
    msg.push(b'E');
    let mut fields = Vec::new();
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR\0");
    fields.push(b'C');
    fields.extend_from_slice(b"42000\0");
    fields.push(b'M');
    fields.extend_from_slice(message.as_bytes());
    fields.push(0);
    fields.push(0);
    let len = 4 + fields.len();
    msg.extend_from_slice(&(len as u32).to_be_bytes());
    msg.extend_from_slice(&fields);
    socket.write_all(&msg).await?;
    Ok(())
}

async fn send_parameter_status(socket: &mut tokio::net::TcpStream, key: &str, value: &str) -> anyhow::Result<()> {
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
    if msg.len() < 8 { return None; }
    let params = &msg[4..];
    let mut iter = params.split(|&b| b == 0);
    while let Some(k) = iter.next() {
        if k.is_empty() { break; }
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
        "float8" | "double" => 701,
        "bool" | "boolean" => 16,
        "timestamp" | "timestamptz" => 1184,
        "date" => 1082,
        _ => 25,
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
        _ => -1,
    }
}

async fn handle_parse(socket: &mut tokio::net::TcpStream, buf: &mut [u8; 4]) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    socket.write_all(&[b'1', 0, 0, 0, 4]).await?;
    socket.flush().await?;
    Ok(())
}

async fn handle_bind(socket: &mut tokio::net::TcpStream, buf: &mut [u8; 4]) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    socket.write_all(&[b'2', 0, 0, 0, 4]).await?;
    socket.flush().await?;
    Ok(())
}

async fn handle_describe(socket: &mut tokio::net::TcpStream, buf: &mut [u8; 4]) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
    socket.flush().await?;
    Ok(())
}

async fn handle_execute(socket: &mut tokio::net::TcpStream, buf: &mut [u8; 4], _worker_client: &WorkerClient, _user_id: &str) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    send_command_complete(socket, "SELECT 0").await?;
    Ok(())
}

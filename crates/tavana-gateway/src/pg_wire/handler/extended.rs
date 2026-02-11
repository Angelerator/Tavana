//! Extended Query Protocol handlers (Parse, Bind, Describe, Execute)
//!
//! These handle the non-TLS TcpStream path for extended query protocol.
//! The TLS path handles these inline in run_query_loop_generic.

use crate::pg_compat;
use crate::query_queue::{QueryOutcome, QueryQueue};
use crate::query_router::QueryRouter;
use crate::smart_scaler::SmartScaler;
use crate::worker_client::WorkerClient;
use super::execution::execute_query_streaming_extended;
use super::messages::{
    send_command_complete, send_error, send_row_description_for_describe,
};
use super::utils::handle_pg_specific_command;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, info, warn};

/// Handle Parse message - extract and store the SQL query
pub(crate) async fn handle_parse_extended(
    socket: &mut tokio::net::TcpStream,
    buf: &mut [u8; 4],
) -> anyhow::Result<Option<String>> {
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;

    let stmt_end = data.iter().position(|&b| b == 0).unwrap_or(0);
    let query_start = stmt_end + 1;
    let query_end = data[query_start..]
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(data.len() - query_start)
        + query_start;

    let query = String::from_utf8_lossy(&data[query_start..query_end]).to_string();
    debug!(
        "Extended query protocol - Parse: {}",
        &query[..query.len().min(100)]
    );

    socket.write_all(&[b'1', 0, 0, 0, 4]).await?; // ParseComplete
    socket.flush().await?;

    if query.is_empty() {
        Ok(None)
    } else {
        Ok(Some(query))
    }
}

/// Handle Bind message
pub(crate) async fn handle_bind(socket: &mut tokio::net::TcpStream, buf: &mut [u8; 4]) -> anyhow::Result<()> {
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    
    let mut offset = 0;
    // Skip portal name
    while offset < data.len() && data[offset] != 0 { offset += 1; }
    offset += 1;
    // Skip statement name  
    while offset < data.len() && data[offset] != 0 { offset += 1; }
    offset += 1;
    
    // Skip parameter format codes
    if offset + 2 <= data.len() {
        let param_format_count = i16::from_be_bytes([data[offset], data[offset+1]]) as usize;
        offset += 2 + param_format_count * 2;
    }
    
    // Skip parameter values
    if offset + 2 <= data.len() {
        let param_count = i16::from_be_bytes([data[offset], data[offset+1]]) as usize;
        offset += 2;
        for _ in 0..param_count {
            if offset + 4 <= data.len() {
                let param_len = i32::from_be_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3]]);
                offset += 4;
                if param_len > 0 {
                    offset += param_len as usize;
                }
            }
        }
    }
    
    // Parse result format codes
    if offset + 2 <= data.len() {
        let result_format_count = i16::from_be_bytes([data[offset], data[offset+1]]) as usize;
        offset += 2;
        if result_format_count > 0 && offset + result_format_count * 2 <= data.len() {
            let mut has_binary = false;
            for i in 0..result_format_count {
                let fmt = i16::from_be_bytes([data[offset + i*2], data[offset + i*2 + 1]]);
                if fmt == 1 {
                    has_binary = true;
                }
            }
            if has_binary {
                warn!("Bind: client requested BINARY format for {} result columns! Tavana only supports TEXT format, this may cause client parsing errors.", result_format_count);
            } else {
                debug!("Bind: client requested TEXT format for {} result columns", result_format_count);
            }
        }
    }
    
    socket.write_all(&[b'2', 0, 0, 0, 4]).await?; // BindComplete
    socket.flush().await?;
    Ok(())
}

/// Handle Describe message for extended query protocol
/// Returns (sent_row_description, column_count)
pub(crate) async fn handle_describe(
    socket: &mut tokio::net::TcpStream,
    buf: &mut [u8; 4],
    prepared_query: Option<&str>,
    worker_client: &WorkerClient,
    user_id: &str,
) -> anyhow::Result<(bool, usize)> {
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    
    let describe_type = if data.is_empty() { b'S' } else { data[0] };
    
    debug!("Extended query protocol - Describe type: {}", describe_type as char);
    
    if let Some(sql) = prepared_query {
        if let Some(result) = handle_pg_specific_command(sql) {
            if describe_type == b'S' {
                socket.write_all(&[b't', 0, 0, 0, 6, 0, 0]).await?;
            }
            
            if result.columns.is_empty() {
                socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
                return Ok((false, 0));
            } else {
                let col_count = result.columns.len();
                send_row_description_for_describe(socket, &result.columns).await?;
                return Ok((true, col_count));
            }
        } else {
            let has_parameters = sql.contains("$1") || sql.contains("$2") || sql.contains("$3");
            
            let schema_sql = if has_parameters {
                sql.replace("$1", "NULL")
                    .replace("$2", "NULL")
                    .replace("$3", "NULL")
                    .replace("$4", "NULL")
                    .replace("$5", "NULL")
                    .replace("$6", "NULL")
                    .replace("$7", "NULL")
                    .replace("$8", "NULL")
                    .replace("$9", "NULL")
            } else {
                sql.to_string()
            };
            
            let schema_sql_rewritten = pg_compat::rewrite_pg_to_duckdb(&schema_sql);
            
            let schema_query = format!("SELECT * FROM ({}) AS _schema_query LIMIT 0", 
                schema_sql_rewritten.trim_end_matches(';'));
            
            match worker_client.execute_query(&schema_query, user_id).await {
                Ok(result) => {
                    if describe_type == b'S' {
                        socket.write_all(&[b't', 0, 0, 0, 6, 0, 0]).await?;
                    }
                    
                    let columns: Vec<(String, String)> = result.columns.iter()
                        .map(|c| (c.name.clone(), c.type_name.clone()))
                        .collect();
                    
                    if columns.is_empty() {
                        socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
                        return Ok((false, 0));
                    } else {
                        let col_count = columns.len();
                        info!("Extended Protocol (non-TLS) - Describe: {} columns: {:?}", 
                            col_count, 
                            columns.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>()
                        );
                        send_row_description_for_describe(socket, &columns).await?;
                        return Ok((true, col_count));
                    }
                }
                Err(e) => {
                    warn!("Extended Protocol (non-TLS) - Describe failed: {}, sending NoData", e);
                    if describe_type == b'S' {
                        socket.write_all(&[b't', 0, 0, 0, 6, 0, 0]).await?;
                    }
                    socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
                    return Ok((false, 0));
                }
            }
        }
    } else {
        socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
        socket.flush().await?;
        return Ok((false, 0));
    }
}

/// Handle Execute message for extended query protocol - with QueryQueue
pub(crate) async fn handle_execute_extended(
    socket: &mut tokio::net::TcpStream,
    buf: &mut [u8; 4],
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    smart_scaler: Option<&SmartScaler>,
    query_queue: &Arc<QueryQueue>,
) -> anyhow::Result<()> {
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    
    debug!(
        "Extended query protocol - Execute: {}",
        &sql[..sql.len().min(100)]
    );

    if let Some(result) = handle_pg_specific_command(sql) {
        if !result.rows.is_empty() {
            info!("Extended query protocol - Execute: Intercepted command returns {} rows", result.rows.len());
            for row in &result.rows {
                let mut row_msg = vec![b'D'];
                let field_count = row.len() as i16;
                let mut row_data = Vec::new();
                row_data.extend_from_slice(&field_count.to_be_bytes());
                for value in row {
                    let bytes = value.as_bytes();
                    row_data.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    row_data.extend_from_slice(bytes);
                }
                let row_len = (4 + row_data.len()) as u32;
                row_msg.extend_from_slice(&row_len.to_be_bytes());
                row_msg.extend_from_slice(&row_data);
                socket.write_all(&row_msg).await?;
            }
            let cmd_tag = result.command_tag.unwrap_or_else(|| format!("SELECT {}", result.rows.len()));
            let cmd_bytes = cmd_tag.as_bytes();
            let cmd_len = (4 + cmd_bytes.len() + 1) as u32;
            let mut cmd_msg = vec![b'C'];
            cmd_msg.extend_from_slice(&cmd_len.to_be_bytes());
            cmd_msg.extend_from_slice(cmd_bytes);
            cmd_msg.push(0);
            socket.write_all(&cmd_msg).await?;
            socket.flush().await?;
            return Ok(());
        }
    }

    let query_id = uuid::Uuid::new_v4().to_string();

    let estimate = query_router.route(sql).await;
    let estimated_data_mb = estimate.data_size_mb;

    let enqueue_result = query_queue
        .enqueue(query_id.clone(), estimated_data_mb)
        .await;

    match enqueue_result {
        Ok(query_token) => {
            let start = std::time::Instant::now();

            let result = execute_query_streaming_extended(
                socket,
                worker_client,
                query_router,
                sql,
                user_id,
                smart_scaler,
            )
            .await;

            let duration_ms = start.elapsed().as_millis() as u64;

            let outcome = match &result {
                Ok(row_count) => {
                    info!(
                        rows = row_count,
                        wait_ms = query_token.queue_wait_ms(),
                        exec_ms = duration_ms,
                        "Extended query completed (streaming, no RowDesc)"
                    );
                    QueryOutcome::Success {
                        rows: *row_count as u64,
                        bytes: 0,
                        duration_ms,
                    }
                }
                Err(e) => {
                    error!("Extended query error: {}", e);
                    send_error(socket, &e.to_string()).await?;
                    QueryOutcome::Failure {
                        error: e.to_string(),
                        duration_ms,
                    }
                }
            };

            query_queue.complete(query_token, outcome).await;
        }
        Err(queue_error) => {
            let error_msg = format!("Query queue error: {}", queue_error);
            warn!(query_id = %query_id, "Extended query failed to queue: {}", error_msg);
            send_error(socket, &error_msg).await?;
        }
    }

    Ok(())
}

/// Handle Execute message when there's no prepared statement
pub(crate) async fn handle_execute_empty(
    socket: &mut tokio::net::TcpStream,
    buf: &mut [u8; 4],
) -> anyhow::Result<()> {
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    send_command_complete(socket, "SELECT 0").await?;
    Ok(())
}

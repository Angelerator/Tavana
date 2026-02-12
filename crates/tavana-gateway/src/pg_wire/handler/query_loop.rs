//! Main query processing loop
//!
//! Single generic implementation supporting both Simple and Extended Query
//! protocols, with full features: portal state, parameter binding, schema
//! caching, error recovery, and cursor support.

use crate::cursors::{self, ConnectionCursors};
use crate::pg_compat;
use crate::pg_wire::backpressure::{
    build_data_row_with_formats, build_data_row_from_arrow, is_disconnect_error,
};
use crate::query_queue::{QueryOutcome, QueryQueue};
use crate::query_router::QueryRouter;
use crate::smart_scaler::SmartScaler;
use crate::worker_client::{StreamingBatch, WorkerClient, WorkerClientPool};
use super::config::PgWireConfig;
use super::execution::execute_simple_query;
use super::messages::{
    send_command_complete, send_data_row, send_error,
    send_row_description_with_formats, send_simple_result,
};
use super::portal::{PortalState, StreamingPortal};
use super::utils::{
    extract_copy_inner_query, get_transaction_state_change, handle_pg_specific_command,
    substitute_parameters, TRANSACTION_STATUS_ERROR,
    TRANSACTION_STATUS_IDLE, TRANSACTION_STATUS_IN_TRANSACTION,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error, info, warn};

/// Run the main query loop for any async stream (TLS or non-TLS)
pub(crate) async fn run_query_loop<S>(
    socket: &mut S,
    worker_client: &WorkerClient,
    worker_client_pool: &WorkerClientPool,
    query_router: &QueryRouter,
    user_id: &str,
    smart_scaler: Option<&SmartScaler>,
    query_queue: &Arc<QueryQueue>,
    config: &PgWireConfig,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = [0u8; 4];
    let mut transaction_status: u8 = TRANSACTION_STATUS_IDLE;
    let mut ready = [b'Z', 0, 0, 0, 5, transaction_status];
    let mut prepared_query: Option<String> = None;
    let mut describe_sent_row_description = false;
    let mut describe_column_count: usize = 0;
    let mut cursors = ConnectionCursors::new();
    let mut portal_state: Option<PortalState> = None;
    let mut bound_parameters: Vec<Option<String>> = Vec::new();
    let mut result_format_codes: Vec<i16> = Vec::new();
    let mut cached_column_types: Vec<String> = Vec::new();
    let mut schema_cache: std::collections::HashMap<String, Vec<(String, String)>> = std::collections::HashMap::new();
    let mut _cached_describe_columns: Option<Vec<(String, String)>> = None;
    let mut ignore_till_sync = false;

    macro_rules! update_transaction_status {
        ($new_status:expr) => {{
            transaction_status = $new_status;
            ready[5] = transaction_status;
            debug!("Transaction status changed to: {}",
                match transaction_status {
                    TRANSACTION_STATUS_IDLE => "Idle",
                    TRANSACTION_STATUS_IN_TRANSACTION => "In Transaction",
                    TRANSACTION_STATUS_ERROR => "Error",
                    _ => "Unknown"
                }
            );
        }};
    }

    loop {
        let mut msg_type = [0u8; 1];
        if socket.read_exact(&mut msg_type).await.is_err() {
            debug!("Client disconnected");
            break;
        }

        if ignore_till_sync && msg_type[0] != b'S' && msg_type[0] != b'X' {
            socket.read_exact(&mut buf).await?;
            let len = u32::from_be_bytes(buf) as usize - 4;
            if len > 0 {
                let mut skip_buf = vec![0u8; len];
                socket.read_exact(&mut skip_buf).await?;
            }
            debug!("Skip-till-sync: Skipped message type '{}'", msg_type[0] as char);
            continue;
        }

        match msg_type[0] {
            b'X' => {
                debug!("Client sent Terminate message");
                break;
            }
            // ===== Simple Query Protocol =====
            b'Q' => {
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut query_bytes = vec![0u8; len];
                socket.read_exact(&mut query_bytes).await?;

                let query = String::from_utf8_lossy(&query_bytes)
                    .trim_end_matches('\0')
                    .to_string();

                debug!("Received query: {}", &query[..query.len().min(100)]);

                let query_upper = query.to_uppercase();
                let query_trimmed = query_upper.trim();

                // Transaction state tracking
                let (new_tx_status, is_tx_cmd) = get_transaction_state_change(&query, transaction_status);
                if is_tx_cmd {
                    info!(
                        query = %query_trimmed,
                        old_status = match transaction_status { b'I' => "Idle", b'T' => "InTx", b'E' => "Err", _ => "?" },
                        new_status = match new_tx_status { b'I' => "Idle", b'T' => "InTx", b'E' => "Err", _ => "?" },
                        "Transaction command detected - updating status"
                    );
                    update_transaction_status!(new_tx_status);
                }

                // Cursor commands
                if query_trimmed.starts_with("DECLARE ") && query_trimmed.contains(" CURSOR ") {
                    debug!(query_trimmed = %query_trimmed, "Detected DECLARE CURSOR command");
                    let default_client = worker_client_pool.default_client();
                    if let Some(result) = cursors::handle_declare_cursor(&query, &mut cursors, &default_client, user_id).await {
                        info!(cursor_count = cursors.len(), "DECLARE CURSOR handled successfully");
                        send_simple_result(socket, &[], &[], result.command_tag.as_deref()).await?;
                        socket.write_all(&ready).await?;
                        socket.flush().await?;
                        continue;
                    } else {
                        warn!(query = %query, "DECLARE CURSOR parsing failed, forwarding to worker");
                    }
                }

                if query_trimmed.starts_with("FETCH ") {
                    match cursors::handle_fetch_cursor(&query, &mut cursors, worker_client_pool, user_id).await {
                        Some(result) => {
                            let cols: Vec<(&str, i32)> = result.columns.iter()
                                .map(|(n, _)| (n.as_str(), 25i32)).collect();
                            send_simple_result(socket, &cols, &result.rows, result.command_tag.as_deref()).await?;
                            socket.write_all(&ready).await?;
                            socket.flush().await?;
                            continue;
                        }
                        None => {
                            send_error(socket, "cursor does not exist").await?;
                            socket.write_all(&ready).await?;
                            socket.flush().await?;
                            continue;
                        }
                    }
                }

                if query_trimmed.starts_with("CLOSE ") {
                    if let Some(result) = cursors::handle_close_cursor(&query, &mut cursors, worker_client_pool).await {
                        info!(command_tag = ?result.command_tag, "CLOSE CURSOR handled");
                        send_simple_result(socket, &[], &[], result.command_tag.as_deref()).await?;
                        socket.write_all(&ready).await?;
                        socket.flush().await?;
                        continue;
                    }
                }

                if query_trimmed == "ROLLBACK" {
                    debug!("Executing ROLLBACK in DuckDB to clear transaction state");
                    let _ = worker_client.execute_query("ROLLBACK", user_id).await;
                    send_simple_result(socket, &[], &[], Some("ROLLBACK")).await?;
                    socket.write_all(&ready).await?;
                    socket.flush().await?;
                    continue;
                }

                if query_trimmed.is_empty() {
                    debug!("Empty query received, returning EmptyQueryResponse");
                    socket.write_all(&[b'I', 0, 0, 0, 4]).await?;
                    socket.write_all(&ready).await?;
                    socket.flush().await?;
                    continue;
                }

                // Query queue + execution
                let query_id = uuid::Uuid::new_v4().to_string();
                let estimate = query_router.route(&query).await;
                let estimated_data_mb = estimate.data_size_mb;

                let enqueue_result = query_queue.enqueue(query_id.clone(), estimated_data_mb).await;

                match enqueue_result {
                    Ok(query_token) => {
                        let start = std::time::Instant::now();
                        let timeout_duration = Duration::from_secs(config.query_timeout_secs);

                        let result = tokio::time::timeout(
                            timeout_duration,
                            execute_simple_query(socket, worker_client, query_router, &query, user_id, smart_scaler)
                        ).await;

                        let duration_ms = start.elapsed().as_millis() as u64;
                        let outcome = match result {
                            Ok(Ok(row_count)) => {
                                info!(
                                    query_id = %query_id, rows = row_count,
                                    wait_ms = query_token.queue_wait_ms(), exec_ms = duration_ms,
                                    "Query completed (streaming)"
                                );
                                if row_count > 100_000 {
                                    warn!(query_id = %query_id, rows = row_count,
                                        "Large result set streamed. If client crashes, use LIMIT/OFFSET or DECLARE CURSOR");
                                }
                                QueryOutcome::Success { rows: row_count as u64, bytes: 0, duration_ms }
                            }
                            Ok(Err(e)) => {
                                error!("Query {} error: {}", query_id, e);
                                let error_msg = e.to_string();
                                if error_msg.contains("transaction") && error_msg.contains("aborted") {
                                    warn!("Transaction error detected, automatically rolling back");
                                    let _ = worker_client.execute_query("ROLLBACK", user_id).await;
                                }
                                send_error(socket, &error_msg).await?;
                                QueryOutcome::Failure { error: error_msg, duration_ms }
                            }
                            Err(_elapsed) => {
                                let error_msg = format!(
                                    "Query timeout: exceeded {}s limit. Use LIMIT clause or increase timeout.",
                                    config.query_timeout_secs
                                );
                                error!("Query {} timed out after {}s", query_id, config.query_timeout_secs);
                                send_error(socket, &error_msg).await?;
                                QueryOutcome::Failure { error: error_msg, duration_ms }
                            }
                        };
                        query_queue.complete(query_token, outcome).await;
                    }
                    Err(queue_error) => {
                        let error_msg = format!("Query queue error: {}", queue_error);
                        warn!(query_id = %query_id, "Query failed to queue: {}", error_msg);
                        send_error(socket, &error_msg).await?;
                    }
                }

                socket.write_all(&ready).await?;
                socket.flush().await?;
            }
            // ===== Extended Query Protocol: Parse =====
            b'P' => {
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut data = vec![0u8; len];
                socket.read_exact(&mut data).await?;

                let stmt_end = data.iter().position(|&b| b == 0).unwrap_or(0);
                let query_start = stmt_end + 1;
                let query_end = data[query_start..].iter().position(|&b| b == 0)
                    .unwrap_or(data.len() - query_start) + query_start;
                let query = String::from_utf8_lossy(&data[query_start..query_end]).to_string();

                let actual_query = if let Some(inner) = extract_copy_inner_query(&query) {
                    debug!("Extended Protocol - Converted COPY to SELECT");
                    inner
                } else {
                    query
                };

                debug!("Extended Protocol - Parse: {}", &actual_query[..actual_query.len().min(100)]);

                if !actual_query.is_empty() {
                    prepared_query = Some(actual_query);
                }

                socket.write_all(&[b'1', 0, 0, 0, 4]).await?; // ParseComplete
                socket.flush().await?;
            }
            // ===== Extended Query Protocol: Bind =====
            b'B' => {
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut data = vec![0u8; len];
                socket.read_exact(&mut data).await?;

                let mut offset = 0;
                // Skip portal name
                while offset < data.len() && data[offset] != 0 { offset += 1; }
                offset += 1;
                // Skip statement name
                while offset < data.len() && data[offset] != 0 { offset += 1; }
                offset += 1;

                // Parse parameter format codes
                let mut param_formats: Vec<i16> = Vec::new();
                if offset + 2 <= data.len() {
                    let param_format_count = i16::from_be_bytes([data[offset], data[offset+1]]) as usize;
                    offset += 2;
                    for _ in 0..param_format_count {
                        if offset + 2 <= data.len() {
                            param_formats.push(i16::from_be_bytes([data[offset], data[offset+1]]));
                            offset += 2;
                        }
                    }
                }

                // Parse parameter values
                bound_parameters.clear();
                if offset + 2 <= data.len() {
                    let param_count = i16::from_be_bytes([data[offset], data[offset+1]]) as usize;
                    offset += 2;
                    for i in 0..param_count {
                        if offset + 4 <= data.len() {
                            let param_len = i32::from_be_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3]]);
                            offset += 4;
                            if param_len < 0 {
                                bound_parameters.push(None);
                            } else if param_len == 0 {
                                bound_parameters.push(Some(String::new()));
                            } else if offset + param_len as usize <= data.len() {
                                let is_binary = param_formats.get(i).copied().unwrap_or(
                                    param_formats.first().copied().unwrap_or(0)
                                ) == 1;

                                if is_binary {
                                    let bytes = &data[offset..offset + param_len as usize];
                                    let value = match param_len {
                                        4 => i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string(),
                                        8 => i64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]).to_string(),
                                        _ => String::from_utf8_lossy(bytes).to_string(),
                                    };
                                    bound_parameters.push(Some(value));
                                } else {
                                    let value = String::from_utf8_lossy(&data[offset..offset + param_len as usize]).to_string();
                                    bound_parameters.push(Some(value));
                                }
                                offset += param_len as usize;
                            }
                        }
                    }
                    if !bound_parameters.is_empty() {
                        debug!("Bind: extracted {} parameters: {:?}", bound_parameters.len(), bound_parameters);
                    }
                }

                // Parse result format codes
                result_format_codes.clear();
                if offset + 2 <= data.len() {
                    let result_format_count = i16::from_be_bytes([data[offset], data[offset+1]]) as usize;
                    offset += 2;

                    if result_format_count > 0 && offset + result_format_count * 2 <= data.len() {
                        for i in 0..result_format_count {
                            let fmt = i16::from_be_bytes([data[offset + i*2], data[offset + i*2 + 1]]);
                            result_format_codes.push(fmt);
                        }
                    }
                }

                if result_format_codes.iter().any(|&f| f == 1) {
                    debug!("Bind: client requested BINARY format for {} columns",
                        result_format_codes.iter().filter(|&&f| f == 1).count());
                }

                debug!("Bind: sending BindComplete");
                socket.write_all(&[b'2', 0, 0, 0, 4]).await?;
                socket.flush().await?;
            }
            // ===== Extended Query Protocol: Describe =====
            b'D' => {
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut data = vec![0u8; len];
                socket.read_exact(&mut data).await?;

                let describe_type = if data.is_empty() { b'S' } else { data[0] };
                debug!("Extended Protocol - Describe type: {}", describe_type as char);

                describe_sent_row_description = false;
                describe_column_count = 0;

                if describe_type == b'S' {
                    // ParameterDescription (no parameters)
                    socket.write_all(&[b't', 0, 0, 0, 6, 0, 0]).await?;
                }

                if let Some(ref sql) = prepared_query {
                    if let Some(result) = handle_pg_specific_command(sql) {
                        if result.columns.is_empty() {
                            debug!("Extended Protocol - Describe: PG command returns no data");
                            socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
                        } else {
                            info!("Extended Protocol - Describe: PG command returns {} columns", result.columns.len());
                            describe_column_count = result.columns.len();
                            cached_column_types = result.columns.iter().map(|(_, t)| t.clone()).collect();
                            send_row_description_with_formats(socket, &result.columns, &result_format_codes).await?;
                            describe_sent_row_description = true;
                        }
                    } else {
                        let has_parameters = sql.contains("$1") || sql.contains("$2") || sql.contains("$3");
                        let _sql_upper = sql.to_uppercase();

                        if has_parameters {
                            if let Some(cached_columns) = schema_cache.get(sql) {
                                debug!("Extended Protocol - Describe: using cached schema ({} columns)", cached_columns.len());
                                describe_column_count = cached_columns.len();
                                _cached_describe_columns = Some(cached_columns.clone());
                                cached_column_types = cached_columns.iter().map(|(_, t)| t.clone()).collect();
                                send_row_description_with_formats(socket, cached_columns, &result_format_codes).await?;
                                describe_sent_row_description = true;
                            } else {
                                let schema_sql = sql.replace("$1", "NULL").replace("$2", "NULL").replace("$3", "NULL")
                                    .replace("$4", "NULL").replace("$5", "NULL").replace("$6", "NULL")
                                    .replace("$7", "NULL").replace("$8", "NULL").replace("$9", "NULL");

                                let schema_sql_rewritten = pg_compat::rewrite_pg_to_duckdb(&schema_sql);
                                let schema_query = format!("SELECT * FROM ({}) AS _schema_query LIMIT 0", schema_sql_rewritten.trim_end_matches(';'));

                                match worker_client.execute_query(&schema_query, user_id).await {
                                    Ok(result) => {
                                        let columns: Vec<(String, String)> = result.columns.iter()
                                            .map(|c| (c.name.clone(), c.type_name.clone())).collect();
                                        if columns.is_empty() {
                                            socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
                                        } else {
                                            describe_column_count = columns.len();
                                            schema_cache.insert(sql.clone(), columns.clone());
                                            _cached_describe_columns = Some(columns.clone());
                                            cached_column_types = columns.iter().map(|(_, t)| t.clone()).collect();
                                            send_row_description_with_formats(socket, &columns, &result_format_codes).await?;
                                            describe_sent_row_description = true;
                                        }
                                    }
                                    Err(e) => {
                                        debug!("Extended Protocol - Describe: schema detection failed: {}", e);
                                        socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
                                    }
                                }
                            }
                        } else {
                            let sql_rewritten = pg_compat::rewrite_pg_to_duckdb(sql);
                            let schema_query = format!("SELECT * FROM ({}) AS _schema_query LIMIT 0", sql_rewritten.trim_end_matches(';'));

                            match worker_client.execute_query(&schema_query, user_id).await {
                                Ok(result) => {
                                    let columns: Vec<(String, String)> = result.columns.iter()
                                        .map(|c| (c.name.clone(), c.type_name.clone())).collect();
                                    if columns.is_empty() {
                                        socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
                                    } else {
                                        info!("Extended Protocol - Describe: {} columns: {:?}", columns.len(), columns.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>());
                                        describe_column_count = columns.len();
                                        _cached_describe_columns = Some(columns.clone());
                                        cached_column_types = columns.iter().map(|(_, t)| t.clone()).collect();
                                        send_row_description_with_formats(socket, &columns, &result_format_codes).await?;
                                        describe_sent_row_description = true;
                                    }
                                }
                                Err(e) => {
                                    warn!("Extended Protocol - Describe: schema query failed: {}", e);
                                    socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
                                }
                            }
                        }
                    }
                } else {
                    debug!("Extended Protocol - Describe: no prepared query");
                    socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
                }
                socket.flush().await?;
            }
            // ===== Extended Query Protocol: Execute =====
            b'E' => {
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut data = vec![0u8; len];
                socket.read_exact(&mut data).await?;

                let portal_end = data.iter().position(|&b| b == 0).unwrap_or(0);
                let max_rows = if portal_end + 5 <= data.len() {
                    let max_rows_bytes = &data[portal_end + 1..portal_end + 5];
                    i32::from_be_bytes([max_rows_bytes[0], max_rows_bytes[1], max_rows_bytes[2], max_rows_bytes[3]])
                } else { 0 };

                // Resume existing portal state (JDBC setFetchSize streaming)
                if let Some(mut state) = portal_state.take() {
                    let (should_continue, keep_state) = match &mut state {
                        PortalState::Streaming(portal) => {
                            let max_rows_to_send = if max_rows > 0 { max_rows as usize } else { usize::MAX };
                            let mut rows_sent_this_batch = 0usize;
                            let mut bytes_since_flush = 0usize;
                            let mut client_disconnected = false;
                            let mut stream_exhausted = false;
                            let mut stream_error: Option<String> = None;

                            debug!("Extended Protocol - Execute: streaming resume, max_rows={}", max_rows);

                            // Send pending Arrow batches first (zero-copy)
                            while !portal.pending_arrow.is_empty() && rows_sent_this_batch < max_rows_to_send {
                                let (batch, start) = portal.pending_arrow.remove(0);
                                let mut row_idx = start;
                                while row_idx < batch.num_rows() && rows_sent_this_batch < max_rows_to_send {
                                    let data_row = build_data_row_from_arrow(&batch, row_idx, &cached_column_types, &result_format_codes);
                                    bytes_since_flush += data_row.len();
                                    if let Err(e) = socket.write_all(&data_row).await {
                                        if is_disconnect_error(&e) { client_disconnected = true; break; }
                                        return Err(e.into());
                                    }
                                    rows_sent_this_batch += 1;
                                    portal.rows_sent += 1;
                                    row_idx += 1;
                                    if bytes_since_flush >= config.flush_threshold_bytes {
                                        if let Err(e) = socket.flush().await {
                                            if is_disconnect_error(&e) { client_disconnected = true; break; }
                                            return Err(e.into());
                                        }
                                        bytes_since_flush = 0;
                                    }
                                }
                                // If we didn't finish this batch, put it back
                                if row_idx < batch.num_rows() {
                                    portal.pending_arrow.insert(0, (batch, row_idx));
                                }
                            }

                            // Send pending string rows (legacy fallback)
                            while !portal.pending_rows.is_empty() && rows_sent_this_batch < max_rows_to_send {
                                let row = portal.pending_rows.remove(0);
                                let data_row = build_data_row_with_formats(&row, &cached_column_types, &result_format_codes);
                                bytes_since_flush += data_row.len();
                                if let Err(e) = socket.write_all(&data_row).await {
                                    if is_disconnect_error(&e) { client_disconnected = true; break; }
                                    return Err(e.into());
                                }
                                rows_sent_this_batch += 1;
                                portal.rows_sent += 1;
                                if bytes_since_flush >= config.flush_threshold_bytes {
                                    if let Err(e) = socket.flush().await {
                                        if is_disconnect_error(&e) { client_disconnected = true; break; }
                                        return Err(e.into());
                                    }
                                    bytes_since_flush = 0;
                                }
                            }

                            // Continue from stream
                            while !client_disconnected && rows_sent_this_batch < max_rows_to_send {
                                match portal.stream.next().await {
                                    Some(Ok(StreamingBatch::ArrowBatches(batches))) => {
                                        for batch in batches {
                                            let mut row_idx = 0;
                                            while row_idx < batch.num_rows() {
                                                if rows_sent_this_batch >= max_rows_to_send {
                                                    // Store remaining rows as zero-copy Arrow slice
                                                    let remaining = batch.num_rows() - row_idx;
                                                    portal.pending_arrow.push((batch.slice(row_idx, remaining), 0));
                                                    break;
                                                }
                                                let data_row = build_data_row_from_arrow(&batch, row_idx, &cached_column_types, &result_format_codes);
                                                bytes_since_flush += data_row.len();
                                                if let Err(e) = socket.write_all(&data_row).await {
                                                    if is_disconnect_error(&e) { client_disconnected = true; break; }
                                                    return Err(e.into());
                                                }
                                                rows_sent_this_batch += 1;
                                                portal.rows_sent += 1;
                                                row_idx += 1;
                                                if bytes_since_flush >= config.flush_threshold_bytes {
                                                    if let Err(e) = socket.flush().await {
                                                        if is_disconnect_error(&e) { client_disconnected = true; break; }
                                                        return Err(e.into());
                                                    }
                                                    bytes_since_flush = 0;
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(StreamingBatch::Rows(batch_rows))) => {
                                        for row in batch_rows {
                                            if rows_sent_this_batch >= max_rows_to_send {
                                                portal.pending_rows.push(row);
                                            } else {
                                                let data_row = build_data_row_with_formats(&row, &cached_column_types, &result_format_codes);
                                                bytes_since_flush += data_row.len();
                                                if let Err(e) = socket.write_all(&data_row).await {
                                                    if is_disconnect_error(&e) { client_disconnected = true; break; }
                                                    return Err(e.into());
                                                }
                                                rows_sent_this_batch += 1;
                                                portal.rows_sent += 1;
                                                if bytes_since_flush >= config.flush_threshold_bytes {
                                                    if let Err(e) = socket.flush().await {
                                                        if is_disconnect_error(&e) { client_disconnected = true; break; }
                                                        return Err(e.into());
                                                    }
                                                    bytes_since_flush = 0;
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(StreamingBatch::Metadata { .. })) => continue,
                                    Some(Ok(StreamingBatch::FlightData { .. })) => { /* Flight SQL only, skip in PG wire */ }
                                    Some(Ok(StreamingBatch::Error(msg))) => { stream_error = Some(msg); break; }
                                    Some(Err(e)) => { stream_error = Some(e.to_string()); break; }
                                    None => { stream_exhausted = true; break; }
                                }
                            }

                            if !client_disconnected && stream_error.is_none() { let _ = socket.flush().await; }

                            if client_disconnected {
                                info!("Client disconnected during streaming resume");
                                (false, false)
                            } else if let Some(err) = stream_error {
                                send_error(socket, &err).await?;
                                (true, false)
                            } else if stream_exhausted && portal.pending_arrow.is_empty() && portal.pending_rows.is_empty() {
                                let cmd_tag = format!("SELECT {}", portal.rows_sent);
                                send_command_complete(socket, &cmd_tag).await?;
                                prepared_query = None;
                                describe_sent_row_description = false;
                                describe_column_count = 0;
                                bound_parameters.clear();
                                result_format_codes.clear();
                                _cached_describe_columns = None;
                                cached_column_types.clear();
                                (true, false)
                            } else {
                                debug!("Extended Protocol - Execute: streaming sent {} rows, more available", rows_sent_this_batch);
                                socket.write_all(&[b's', 0, 0, 0, 4]).await?; // PortalSuspended
                                socket.flush().await?;
                                (true, true)
                            }
                        }
                        PortalState::Buffered(portal) => {
                            let remaining = portal.rows.len() - portal.offset;
                            let rows_to_send = if max_rows > 0 && (max_rows as usize) < remaining { max_rows as usize } else { remaining };
                            let end = portal.offset + rows_to_send;
                            debug!("Extended Protocol - Execute: buffered resume from offset {}, sending {} rows", portal.offset, rows_to_send);

                            let mut bytes_since_flush = 0usize;
                            let mut rows_since_flush = 0usize;
                            let mut client_disconnected = false;

                            for row in &portal.rows[portal.offset..end] {
                                let data_row = build_data_row_with_formats(row, &cached_column_types, &result_format_codes);
                                bytes_since_flush += data_row.len();
                                rows_since_flush += 1;
                                socket.write_all(&data_row).await?;
                                if bytes_since_flush >= config.flush_threshold_bytes || rows_since_flush >= config.flush_threshold_rows {
                                    if let Err(e) = socket.flush().await {
                                        if is_disconnect_error(&e) { client_disconnected = true; break; }
                                        return Err(e.into());
                                    }
                                    bytes_since_flush = 0;
                                    rows_since_flush = 0;
                                }
                            }

                            if !client_disconnected { socket.flush().await?; }

                            if client_disconnected {
                                info!("Client disconnected during buffered resume");
                                (false, false)
                            } else {
                                portal.offset = end;
                                if portal.offset >= portal.rows.len() {
                                    let cmd_tag = format!("SELECT {}", portal.rows.len());
                                    send_command_complete(socket, &cmd_tag).await?;
                                    prepared_query = None;
                                    describe_sent_row_description = false;
                                    describe_column_count = 0;
                                    bound_parameters.clear();
                                    result_format_codes.clear();
                                    _cached_describe_columns = None;
                                    cached_column_types.clear();
                                    (true, false)
                                } else {
                                    socket.write_all(&[b's', 0, 0, 0, 4]).await?; // PortalSuspended
                                    socket.flush().await?;
                                    (true, true)
                                }
                            }
                        }
                    };

                    if keep_state { portal_state = Some(state); continue; }
                    if !should_continue { break; }
                } else if let Some(ref sql) = prepared_query {
                    // First Execute for this prepared query
                    let final_sql = if !bound_parameters.is_empty() && sql.contains('$') {
                        let substituted = substitute_parameters(sql, &bound_parameters);
                        debug!("Extended Protocol - Execute: substituted parameters, SQL: {}", &substituted[..substituted.len().min(200)]);
                        substituted
                    } else { sql.clone() };

                    let final_sql = pg_compat::rewrite_pg_to_duckdb(&final_sql);

                    if !describe_sent_row_description {
                        // Describe sent NoData — just send CommandComplete
                        let cmd_tag = if let Some(result) = handle_pg_specific_command(&final_sql) {
                            result.command_tag.unwrap_or_else(|| "SELECT 0".to_string())
                        } else { "SELECT 0".to_string() };
                        debug!("Extended Protocol - Execute: Describe sent NoData, sending CommandComplete: {}", cmd_tag);
                        send_command_complete(socket, &cmd_tag).await?;
                        socket.flush().await?;
                    } else {
                        // Describe sent RowDescription — execute and stream rows
                        if let Some(result) = handle_pg_specific_command(&final_sql) {
                            if !result.rows.is_empty() {
                                info!("Extended Protocol - Execute: Intercepted command returns {} rows", result.rows.len());
                                for row in &result.rows {
                                    send_data_row(socket, row, describe_column_count).await?;
                                }
                                let cmd_tag = result.command_tag.unwrap_or_else(|| format!("SELECT {}", result.rows.len()));
                                send_command_complete(socket, &cmd_tag).await?;
                                socket.flush().await?;
                                prepared_query = None;
                                describe_sent_row_description = false;
                                describe_column_count = 0;
                                bound_parameters.clear();
                                result_format_codes.clear();
                                _cached_describe_columns = None;
                                cached_column_types.clear();
                                continue;
                            }
                        }

                        // TRUE STREAMING mode
                        info!("Extended Protocol - Execute: streaming mode, max_rows={}", max_rows);

                        let stream_result = worker_client.execute_query_streaming(&final_sql, user_id).await;

                        match stream_result {
                            Ok(mut stream) => {
                                let max_rows_to_send = if max_rows > 0 { max_rows as usize } else { usize::MAX };
                                let mut rows_sent = 0usize;
                                let mut bytes_since_flush = 0usize;
                                let mut client_disconnected = false;
                                let mut stream_exhausted = false;
                                let mut stream_error: Option<String> = None;
                                let mut pending_rows: Vec<Vec<String>> = Vec::new();
                                let mut pending_arrow_batches: Vec<(arrow_array::RecordBatch, usize)> = Vec::new();
                                let mut columns: Vec<(String, String)> = Vec::new();
                                let mut local_column_types: Vec<String> = Vec::new();

                                'stream_loop: loop {
                                    if rows_sent >= max_rows_to_send { break; }

                                    match stream.next().await {
                                        Some(Ok(StreamingBatch::Metadata { columns: cols, column_types: types })) => {
                                            local_column_types = types.clone();
                                            columns = cols.into_iter().zip(types.into_iter()).collect();
                                            continue;
                                        }
                                        Some(Ok(StreamingBatch::ArrowBatches(batches))) => {
                                            for batch in batches {
                                                let mut row_idx = 0;
                                                while row_idx < batch.num_rows() {
                                                    if rows_sent >= max_rows_to_send {
                                                        // Store remaining as zero-copy Arrow slice
                                                        let remaining = batch.num_rows() - row_idx;
                                                        pending_arrow_batches.push((batch.slice(row_idx, remaining), 0));
                                                        break;
                                                    }
                                                    let data_row = build_data_row_from_arrow(&batch, row_idx, &local_column_types, &result_format_codes);
                                                    bytes_since_flush += data_row.len();
                                                    if let Err(e) = socket.write_all(&data_row).await {
                                                        if is_disconnect_error(&e) { client_disconnected = true; break 'stream_loop; }
                                                        return Err(e.into());
                                                    }
                                                    rows_sent += 1;
                                                    row_idx += 1;
                                                    if bytes_since_flush >= config.flush_threshold_bytes {
                                                        if let Err(e) = socket.flush().await {
                                                            if is_disconnect_error(&e) { client_disconnected = true; break 'stream_loop; }
                                                            return Err(e.into());
                                                        }
                                                        bytes_since_flush = 0;
                                                    }
                                                }
                                            }
                                        }
                                        Some(Ok(StreamingBatch::Rows(batch_rows))) => {
                                            for row in batch_rows {
                                                if rows_sent >= max_rows_to_send {
                                                    pending_rows.push(row);
                                                } else {
                                                    let data_row = build_data_row_with_formats(&row, &local_column_types, &result_format_codes);
                                                    bytes_since_flush += data_row.len();
                                                    if let Err(e) = socket.write_all(&data_row).await {
                                                        if is_disconnect_error(&e) { client_disconnected = true; break 'stream_loop; }
                                                        return Err(e.into());
                                                    }
                                                    rows_sent += 1;
                                                    if bytes_since_flush >= config.flush_threshold_bytes {
                                                        if let Err(e) = socket.flush().await {
                                                            if is_disconnect_error(&e) { client_disconnected = true; break 'stream_loop; }
                                                            return Err(e.into());
                                                        }
                                                        bytes_since_flush = 0;
                                                    }
                                                }
                                            }
                                        }
                                        Some(Ok(StreamingBatch::FlightData { .. })) => { /* Flight SQL only, skip in PG wire */ }
                                        Some(Ok(StreamingBatch::Error(msg))) => { stream_error = Some(msg); break; }
                                        Some(Err(e)) => { stream_error = Some(e.to_string()); break; }
                                        None => { stream_exhausted = true; break; }
                                    }
                                }

                                if !client_disconnected && stream_error.is_none() { let _ = socket.flush().await; }

                                if client_disconnected {
                                    info!("Client disconnected during streaming Execute after {} rows", rows_sent);
                                    break;
                                } else if let Some(err) = stream_error {
                                    error!("Extended Protocol - Execute: stream error: {}", err);
                                    send_error(socket, &err).await?;
                                    ignore_till_sync = true;
                                    update_transaction_status!(TRANSACTION_STATUS_ERROR);
                                } else if stream_exhausted && pending_arrow_batches.is_empty() && pending_rows.is_empty() {
                                    let cmd_tag = format!("SELECT {}", rows_sent);
                                    send_command_complete(socket, &cmd_tag).await?;
                                    socket.flush().await?;
                                    info!("Extended Protocol - Execute: streaming sent {} rows, CommandComplete", rows_sent);
                                } else {
                                    info!("Extended Protocol - Execute: streaming sent {} rows, storing portal", rows_sent);
                                    portal_state = Some(PortalState::Streaming(StreamingPortal {
                                        stream, columns, rows_sent, column_count: describe_column_count,
                                        pending_arrow: pending_arrow_batches, pending_rows,
                                    }));
                                    socket.write_all(&[b's', 0, 0, 0, 4]).await?; // PortalSuspended
                                    socket.flush().await?;
                                    continue;
                                }
                            }
                            Err(e) => {
                                error!("Extended Protocol - Execute: failed to start stream: {}", e);
                                send_error(socket, &e.to_string()).await?;
                                ignore_till_sync = true;
                                update_transaction_status!(TRANSACTION_STATUS_ERROR);
                            }
                        }
                    }

                    prepared_query = None;
                    describe_sent_row_description = false;
                    describe_column_count = 0;
                    bound_parameters.clear();
                    result_format_codes.clear();
                    _cached_describe_columns = None;
                    cached_column_types.clear();
                } else {
                    debug!("Extended Protocol - Execute: no prepared query");
                    send_command_complete(socket, "SELECT 0").await?;
                }
            }
            // ===== Extended Query Protocol: Close =====
            b'C' => {
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut data = vec![0u8; len];
                socket.read_exact(&mut data).await?;

                let close_type = if data.is_empty() { b'S' } else { data[0] };
                debug!("Extended Protocol - Close type: {}", close_type as char);

                prepared_query = None;
                describe_sent_row_description = false;
                describe_column_count = 0;
                portal_state = None;
                bound_parameters.clear();
                result_format_codes.clear();
                _cached_describe_columns = None;
                cached_column_types.clear();

                socket.write_all(&[b'3', 0, 0, 0, 4]).await?; // CloseComplete
                socket.flush().await?;
            }
            // ===== Extended Query Protocol: Sync =====
            b'S' => {
                socket.read_exact(&mut buf).await?;

                if ignore_till_sync {
                    info!("Extended Protocol - Sync: Error recovery complete, resuming normal operation");
                    ignore_till_sync = false;
                }

                info!("Extended Protocol - Sync: sending ReadyForQuery (status={})",
                    match transaction_status {
                        TRANSACTION_STATUS_IDLE => "Idle",
                        TRANSACTION_STATUS_IN_TRANSACTION => "InTransaction",
                        TRANSACTION_STATUS_ERROR => "Error",
                        _ => "Unknown"
                    }
                );
                socket.write_all(&ready).await?;
                socket.flush().await?;

                if transaction_status == TRANSACTION_STATUS_ERROR {
                    update_transaction_status!(TRANSACTION_STATUS_IDLE);
                }
            }
            // ===== Unknown message type =====
            _ => {
                debug!("Unknown protocol message type: 0x{:02x}", msg_type[0]);
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut skip = vec![0u8; len];
                socket.read_exact(&mut skip).await?;
            }
        }
    }

    Ok(())
}

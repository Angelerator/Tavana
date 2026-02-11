//! Query execution functions
//!
//! All execute_* functions for query processing including streaming,
//! buffered, SmartScaler, and worker routing variants.

use crate::metrics;
use crate::pg_compat;
use crate::pg_wire::backpressure::{
    BackpressureConfig, BackpressureWriter,
    build_row_description, build_command_complete, build_data_row,
    is_disconnect_error,
};
use crate::query_router::{QueryRouter, QueryTarget};
use crate::smart_scaler::SmartScaler;
use crate::worker_client::{StreamingBatch, WorkerClient};
use super::config::PgWireConfig;
use super::messages::{
    send_command_complete_generic, send_copy_data_row_generic,
    send_copy_done_generic, send_copy_out_response_header_generic,
    send_data_row_generic,
    send_query_result_data_only, send_query_result_immediate,
    send_row_description_generic, send_simple_result_generic,
};
use super::utils::{
    build_notice_message, extract_copy_inner_query, handle_pg_specific_command,
    execute_local_fallback, QueryExecutionResult, STREAMING_BATCH_SIZE,
};
use tavana_common::proto;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

/// Execute query for TLS streams with TRUE STREAMING
pub(crate) async fn execute_query_tls<S>(
    socket: &mut S,
    worker_client: &WorkerClient,
    _query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    _smart_scaler: Option<&SmartScaler>,
) -> anyhow::Result<usize>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if let Some(result) = handle_pg_specific_command(sql) {
        let columns: Vec<(&str, i32)> = result.columns.iter().map(|(n, _t)| (n.as_str(), 25i32)).collect();
        send_simple_result_generic(socket, &columns, &result.rows, result.command_tag.as_deref()).await?;
        return Ok(0);
    }
    
    let is_copy_command = sql.trim().to_uppercase().starts_with("COPY");
    
    let actual_sql = if let Some(inner) = extract_copy_inner_query(sql) {
        info!("Rewrote COPY command to inner query: {}", &inner[..inner.len().min(80)]);
        inner
    } else {
        sql.to_string()
    };
    
    let actual_sql = pg_compat::rewrite_pg_to_duckdb(&actual_sql);
    let sql = &actual_sql;

    metrics::query_started();
    let start_time = std::time::Instant::now();

    match worker_client.execute_query_streaming(sql, user_id).await {
        Ok(mut stream) => {
            let mut columns_sent = false;
            let mut total_rows: usize = 0;
            let mut batch_rows: usize = 0;
            let mut column_names: Vec<String> = vec![];
            let mut _column_types: Vec<String> = vec![];

            while let Some(batch) = stream.next().await {
                match batch? {
                    StreamingBatch::Metadata { columns, column_types: types } => {
                        column_names = columns;
                        _column_types = types;
                        
                        if !columns_sent {
                            if is_copy_command {
                                send_copy_out_response_header_generic(socket, column_names.len()).await?;
                            } else {
                                let col_pairs: Vec<(String, String)> = column_names.iter()
                                    .zip(_column_types.iter())
                                    .map(|(n, t)| (n.clone(), t.clone()))
                                    .collect();
                                send_row_description_generic(socket, &col_pairs).await?;
                            }
                            columns_sent = true;
                        }
                    }
                    StreamingBatch::Rows(rows) => {
                        for row in rows {
                            if is_copy_command {
                                send_copy_data_row_generic(socket, &row).await?;
                            } else {
                                send_data_row_generic(socket, &row, column_names.len()).await?;
                            }
                            total_rows += 1;
                            batch_rows += 1;

                            if batch_rows >= STREAMING_BATCH_SIZE {
                                socket.flush().await?;
                                batch_rows = 0;
                            }
                        }
                    }
                    StreamingBatch::Error(msg) => {
                        return Err(anyhow::anyhow!("{}", msg));
                    }
                }
            }

            if is_copy_command {
                send_copy_done_generic(socket).await?;
            }
            
            if !columns_sent && !is_copy_command {
                send_row_description_generic(socket, &[]).await?;
            }
            
            let tag = format!("SELECT {}", total_rows);
            send_command_complete_generic(socket, &tag).await?;
            socket.flush().await?;

            metrics::query_ended();
            debug!(
                rows = total_rows,
                elapsed_ms = start_time.elapsed().as_millis(),
                "Query completed (TRUE STREAMING - OOM-proof)"
            );
            
            Ok(total_rows)
        }
        Err(e) => {
            error!("Streaming query failed: {}", e);
            Err(e)
        }
    }
}

/// Execute query for Extended Query Protocol (buffered)
#[allow(dead_code)]
pub(crate) async fn execute_query_extended_protocol<S>(
    socket: &mut S,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    _smart_scaler: Option<&SmartScaler>,
    expected_column_count: usize,
    max_rows: i32,
) -> anyhow::Result<(usize, bool)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::query_router::QueryTarget;
    
    if let Some(result) = handle_pg_specific_command(sql) {
        send_data_rows_only_generic(socket, &result.rows, result.rows.len(), result.command_tag.as_deref(), expected_column_count).await?;
        return Ok((result.rows.len(), false));
    }
    
    let actual_sql = if let Some(inner) = extract_copy_inner_query(sql) {
        debug!("Extended Protocol Execute - Converted COPY to SELECT");
        inner
    } else {
        sql.to_string()
    };
    
    let actual_sql = pg_compat::rewrite_pg_to_duckdb(&actual_sql);
    let sql = &actual_sql;

    metrics::query_started();

    let estimate = query_router.route(sql).await;
    debug!(
        data_mb = estimate.data_size_mb,
        target = ?estimate.target,
        "Extended Protocol - Query routed"
    );

    let (_columns, rows) = match &estimate.target {
        QueryTarget::PreSizedWorker { address, worker_name } => {
            debug!("Extended Protocol - Executing on pre-sized worker: {}", worker_name);
            execute_query_on_worker_buffered(address, sql, user_id).await?
        }
        QueryTarget::TenantPool { service_addr, tenant_id } => {
            debug!("Extended Protocol - Executing on tenant pool: {}", tenant_id);
            execute_query_on_worker_buffered(service_addr, sql, user_id).await?
        }
        QueryTarget::WorkerPool => {
            let result = worker_client.execute_query(sql, user_id).await?;
            let cols: Vec<(String, String)> = result.columns.iter()
                .map(|c| (c.name.clone(), c.type_name.clone()))
                .collect();
            (cols, result.rows)
        }
    };
    
    let total_rows = rows.len();
    let (rows_to_send, more_available) = (&rows[..], false);
    
    if max_rows > 0 {
        debug!(
            max_rows = max_rows,
            total_rows = total_rows,
            "Extended Protocol - max_rows requested but cursor streaming not yet implemented, sending all rows"
        );
    }
    
    let row_count = rows_to_send.len();
    send_data_rows_only_generic(socket, rows_to_send, row_count, None, expected_column_count).await?;
    
    debug!("Extended Protocol Execute completed: {} rows (all sent)", row_count);
    
    Ok((row_count, more_available))
}

// Re-export for use in the function above
use super::messages::send_data_rows_only_generic;

/// Execute query and return rows only (for cursor streaming)
pub(crate) async fn execute_query_get_rows(
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    _smart_scaler: Option<&SmartScaler>,
) -> anyhow::Result<Vec<Vec<String>>> {
    if let Some(result) = handle_pg_specific_command(sql) {
        return Ok(result.rows);
    }
    
    let actual_sql = if let Some(inner) = extract_copy_inner_query(sql) {
        debug!("execute_query_get_rows - Converted COPY to SELECT");
        inner
    } else {
        sql.to_string()
    };
    
    let actual_sql = pg_compat::rewrite_pg_to_duckdb(&actual_sql);
    let sql = &actual_sql;

    let estimate = query_router.route(sql).await;
    
    let (_columns, rows) = match &estimate.target {
        QueryTarget::PreSizedWorker { address, worker_name } => {
            debug!("execute_query_get_rows - Executing on pre-sized worker: {}", worker_name);
            execute_query_on_worker_buffered(address, sql, user_id).await?
        }
        QueryTarget::TenantPool { service_addr, tenant_id } => {
            debug!("execute_query_get_rows - Executing on tenant pool: {}", tenant_id);
            execute_query_on_worker_buffered(service_addr, sql, user_id).await?
        }
        QueryTarget::WorkerPool => {
            let result = worker_client.execute_query(sql, user_id).await?;
            let cols: Vec<(String, String)> = result.columns.iter()
                .map(|c| (c.name.clone(), c.type_name.clone()))
                .collect();
            (cols, result.rows)
        }
    };
    
    Ok(rows)
}

/// Execute query on a specific worker address and return buffered results
pub(crate) async fn execute_query_on_worker_buffered(
    worker_addr: &str,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<(Vec<(String, String)>, Vec<Vec<String>>)> {
    const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024;

    let channel = Channel::from_shared(worker_addr.to_string())?
        .timeout(std::time::Duration::from_secs(1800))
        .connect_timeout(std::time::Duration::from_secs(30))
        .tcp_keepalive(Some(std::time::Duration::from_secs(10)))
        .connect()
        .await?;

    let mut client = proto::query_service_client::QueryServiceClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);

    let query_id = uuid::Uuid::new_v4().to_string();

    let request = proto::ExecuteQueryRequest {
        query_id: query_id.clone(),
        sql: sql.to_string(),
        user: Some(proto::UserIdentity {
            user_id: user_id.to_string(),
            tenant_id: "default".to_string(),
            scopes: vec!["query:execute".to_string()],
            claims: Default::default(),
        }),
        options: Some(proto::QueryOptions {
            timeout_seconds: 1800,
            max_rows: 0,
            max_bytes: 0,
            enable_profiling: false,
            session_params: Default::default(),
        }),
        allocated_resources: None,
    };

    let response = client.execute_query(request).await?;
    let mut stream = response.into_inner();

    let mut columns: Vec<(String, String)> = Vec::new();
    let mut rows: Vec<Vec<String>> = Vec::new();

    while let Some(batch) = stream.message().await? {
        match batch.result {
            Some(proto::query_result_batch::Result::Metadata(meta)) => {
                columns = meta.columns.iter()
                    .zip(meta.column_types.iter())
                    .map(|(name, type_name)| (name.clone(), type_name.clone()))
                    .collect();
            }
            Some(proto::query_result_batch::Result::RecordBatch(batch_data)) => {
                if !batch_data.data.is_empty() {
                    if let Ok(batch_rows) = serde_json::from_slice::<Vec<Vec<String>>>(&batch_data.data) {
                        rows.extend(batch_rows);
                    }
                }
            }
            Some(proto::query_result_batch::Result::Error(err)) => {
                return Err(anyhow::anyhow!("{}: {}", err.code, err.message));
            }
            _ => {}
        }
    }

    Ok((columns, rows))
}

// ===== Streaming Execution (TcpStream) =====

/// Execute query with TRUE STREAMING (TcpStream)
#[allow(dead_code)]
pub(crate) async fn execute_query_streaming(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<usize> {
    execute_query_streaming_with_scaler(socket, worker_client, query_router, sql, user_id, None).await
}

/// Execute query with SmartScaler lifecycle
#[allow(dead_code)]
pub(crate) async fn execute_query_streaming_with_scaler(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    smart_scaler: Option<&SmartScaler>,
) -> anyhow::Result<usize> {
    execute_query_streaming_impl(socket, worker_client, query_router, sql, user_id, smart_scaler, false).await
}

/// Execute query for Extended Query Protocol (TcpStream)
#[allow(dead_code)]
pub(crate) async fn execute_query_streaming_extended(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    smart_scaler: Option<&SmartScaler>,
) -> anyhow::Result<usize> {
    execute_query_streaming_impl(socket, worker_client, query_router, sql, user_id, smart_scaler, true).await
}

/// Internal streaming implementation with skip_row_description flag
#[allow(dead_code)]
pub(crate) async fn execute_query_streaming_impl(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    smart_scaler: Option<&SmartScaler>,
    skip_row_description: bool,
) -> anyhow::Result<usize> {
    let sql_trimmed = sql.trim().to_uppercase();
    if sql_trimmed == "ROLLBACK" {
        debug!("Executing ROLLBACK in DuckDB to clear transaction state (streaming)");
        let _ = worker_client.execute_query("ROLLBACK", user_id).await;
        let result = QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("ROLLBACK".to_string()),
        };
        if skip_row_description {
            send_query_result_data_only(socket, result).await?;
        } else {
            send_query_result_immediate(socket, result).await?;
        }
        return Ok(0);
    }

    if let Some(result) = handle_pg_specific_command(sql) {
        if skip_row_description {
            send_query_result_data_only(socket, result).await?;
        } else {
            send_query_result_immediate(socket, result).await?;
        }
        return Ok(0);
    }

    let rewritten_sql = pg_compat::rewrite_pg_to_duckdb(sql);
    let sql = rewritten_sql.as_str();

    metrics::query_started();

    let estimate = query_router.route(sql).await;

    info!(
        data_mb = estimate.data_size_mb,
        target = ?estimate.target,
        skip_row_desc = skip_row_description,
        "Query routed for streaming execution"
    );

    if let Some(scaler) = smart_scaler {
        return execute_with_smart_scaler_impl(
            socket, worker_client, query_router, scaler, sql, user_id, &estimate, skip_row_description,
        ).await;
    }

    let (result, worker_name) = match estimate.target {
        QueryTarget::PreSizedWorker { address, worker_name } => {
            info!("Using pre-sized worker {} at {}", worker_name, address);
            metrics::update_worker_active_queries(&worker_name, 1);
            let result = execute_query_streaming_to_worker_impl(socket, &address, sql, user_id, skip_row_description).await;
            metrics::update_worker_active_queries(&worker_name, 0);
            (result, Some(worker_name))
        }
        QueryTarget::TenantPool { service_addr, tenant_id } => {
            info!("Using tenant pool {} at {}", tenant_id, service_addr);
            let result = execute_query_streaming_to_worker_impl(socket, &service_addr, sql, user_id, skip_row_description).await;
            (result, None)
        }
        QueryTarget::WorkerPool => {
            let result = execute_query_streaming_default_impl(socket, worker_client, sql, user_id, skip_row_description).await;
            (result, None)
        }
    };

    if let Some(ref name) = worker_name {
        query_router.release_worker(name, None).await;
        debug!("Released worker {} after streaming query", name);
    }

    metrics::query_ended();

    result
}

/// Execute with SmartScaler (public)
#[allow(dead_code)]
pub(crate) async fn execute_with_smart_scaler(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    scaler: &SmartScaler,
    sql: &str,
    user_id: &str,
    estimate: &crate::query_router::QueryEstimate,
) -> anyhow::Result<usize> {
    execute_with_smart_scaler_impl(socket, worker_client, query_router, scaler, sql, user_id, estimate, false).await
}

/// Execute with SmartScaler - internal implementation
#[allow(dead_code)]
pub(crate) async fn execute_with_smart_scaler_impl(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    scaler: &SmartScaler,
    sql: &str,
    user_id: &str,
    estimate: &crate::query_router::QueryEstimate,
    skip_row_description: bool,
) -> anyhow::Result<usize> {
    let query_id = uuid::Uuid::new_v4().to_string();

    let selection = match scaler.select_worker(&query_id, estimate.data_size_mb).await {
        Some(s) => s,
        None => {
            let pending = scaler.get_pending_demand_mb().await + estimate.data_size_mb;
            let decision = scaler.evaluate_hpa_scaling(pending).await;
            if let Err(e) = scaler.apply_hpa_scaling(&decision).await {
                warn!("Failed to apply HPA scaling: {}", e);
            }

            scaler.enqueue_query(&query_id, estimate.data_size_mb).await;

            for _ in 0..30 {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if let Some(_s) = scaler.select_worker(&query_id, estimate.data_size_mb).await {
                    scaler.dequeue_query(&query_id).await;
                    break;
                }
            }

            warn!("No SmartScaler worker available after waiting, falling back to pool");
            metrics::query_ended();
            return execute_query_streaming_default_impl(socket, worker_client, sql, user_id, skip_row_description).await;
        }
    };

    info!(
        "SmartScaler selected worker {} (needs_resize={}, limit={}MB->{}MB)",
        selection.worker_name, selection.needs_resize, selection.current_limit_mb, selection.new_limit_mb
    );

    if selection.needs_resize {
        if let Err(e) = scaler.presize_worker(&selection.worker_name, selection.new_limit_mb).await {
            warn!("Failed to pre-size worker: {}", e);
        }
    }

    scaler.query_started(&selection.worker_name, &query_id, estimate.data_size_mb, selection.new_limit_mb).await;

    metrics::update_worker_active_queries(&selection.worker_name, 1);

    let elastic_handle = scaler.start_elastic_monitoring(selection.worker_name.clone());

    let result = execute_query_streaming_to_worker_impl(socket, &selection.worker_address, sql, user_id, skip_row_description).await;

    elastic_handle.abort();

    scaler.query_completed(&selection.worker_name, &query_id).await;
    query_router.release_worker(&selection.worker_name, None).await;

    metrics::update_worker_active_queries(&selection.worker_name, 0);
    metrics::query_ended();

    info!("SmartScaler query {} completed on worker {}", query_id, selection.worker_name);

    result
}

/// Stream query to specific worker
#[allow(dead_code)]
pub(crate) async fn execute_query_streaming_to_worker(
    socket: &mut tokio::net::TcpStream,
    worker_addr: &str,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<usize> {
    execute_query_streaming_to_worker_impl(socket, worker_addr, sql, user_id, false).await
}

/// Stream query to specific worker - internal implementation
#[allow(dead_code)]
pub(crate) async fn execute_query_streaming_to_worker_impl(
    socket: &mut tokio::net::TcpStream,
    worker_addr: &str,
    sql: &str,
    user_id: &str,
    skip_row_description: bool,
) -> anyhow::Result<usize> {
    info!("Streaming query to worker: {}", worker_addr);

    let config = PgWireConfig::default();
    let bp_config = BackpressureConfig {
        flush_threshold_bytes: config.flush_threshold_bytes,
        flush_threshold_rows: config.flush_threshold_rows,
        flush_timeout_secs: config.flush_timeout_secs,
        connection_check_interval_rows: config.connection_check_interval_rows,
        write_buffer_size: config.write_buffer_size,
    };

    const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024;

    let channel = Channel::from_shared(worker_addr.to_string())?
        .timeout(std::time::Duration::from_secs(1800))
        .connect_timeout(std::time::Duration::from_secs(30))
        .tcp_keepalive(Some(std::time::Duration::from_secs(10)))
        .http2_keep_alive_interval(std::time::Duration::from_secs(10))
        .keep_alive_timeout(std::time::Duration::from_secs(20))
        .keep_alive_while_idle(true)
        .connect()
        .await?;

    let mut client = proto::query_service_client::QueryServiceClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);

    let query_id = uuid::Uuid::new_v4().to_string();

    let request = proto::ExecuteQueryRequest {
        query_id: query_id.clone(),
        sql: sql.to_string(),
        user: Some(proto::UserIdentity {
            user_id: user_id.to_string(),
            tenant_id: "default".to_string(),
            scopes: vec!["query:execute".to_string()],
            claims: Default::default(),
        }),
        options: Some(proto::QueryOptions {
            timeout_seconds: config.query_timeout_secs as u32,
            max_rows: 0,
            max_bytes: 0,
            enable_profiling: false,
            session_params: Default::default(),
        }),
        allocated_resources: None,
    };

    let response = client.execute_query(request).await?;
    let mut stream = response.into_inner();

    let mut writer = BackpressureWriter::new(&mut *socket, bp_config);
    let mut columns_sent = false;

    while let Some(batch) = stream.message().await? {
        match batch.result {
            Some(proto::query_result_batch::Result::Metadata(meta)) => {
                if !columns_sent && !skip_row_description {
                    let row_desc = build_row_description(&meta.columns, &meta.column_types);
                    writer.write_bytes(&row_desc).await?;
                }
                columns_sent = true;
            }
            Some(proto::query_result_batch::Result::RecordBatch(batch)) => {
                if !batch.data.is_empty() {
                    if let Ok(rows) = serde_json::from_slice::<Vec<Vec<String>>>(&batch.data) {
                        for row in rows {
                            let data_row = build_data_row(&row);
                            
                            match writer.write_bytes(&data_row).await {
                                Ok(_) => {}
                                Err(e) if is_disconnect_error(&e) => {
                                    let stats = writer.stats();
                                    warn!("Client disconnected during streaming after {} rows, {} bytes", stats.rows_sent, stats.bytes_sent);
                                    return Err(anyhow::anyhow!("Client disconnected"));
                                }
                                Err(e) => return Err(e.into()),
                            }
                            writer.row_sent();

                            if writer.should_flush() {
                                match writer.flush_with_backpressure().await {
                                    Ok(true) => {}
                                    Ok(false) => {
                                        let stats = writer.stats();
                                        debug!("Slow client detected after {} rows", stats.rows_sent);
                                    }
                                    Err(e) if is_disconnect_error(&e) => {
                                        let stats = writer.stats();
                                        warn!("Client disconnected during flush after {} rows", stats.rows_sent);
                                        return Err(anyhow::anyhow!("Client disconnected"));
                                    }
                                    Err(e) => {
                                        warn!("Flush failed: {} - client may be overwhelmed", e);
                                        return Err(anyhow::anyhow!("Client too slow: {}", e));
                                    }
                                }
                            }

                            if writer.should_check_connection() {
                                if !writer.is_connected().await {
                                    let stats = writer.stats();
                                    warn!("Client disconnected during streaming after {} rows", stats.rows_sent);
                                    return Err(anyhow::anyhow!("Client disconnected"));
                                }
                                
                                let stats = writer.stats();
                                debug!("Streaming progress: {} rows, {} bytes, {} flushes", stats.rows_sent, stats.bytes_sent, stats.flush_count);
                            }

                            if let Some(current_gb) = writer.stats_mut().should_warn_large_transfer() {
                                let notice_msg = format!(
                                    "Large data transfer: {}GB sent. Consider using LIMIT or CURSOR for very large results.",
                                    current_gb
                                );
                                warn!("Large transfer warning: {}GB sent to client", current_gb);
                                let notice_bytes = build_notice_message(&notice_msg);
                                if let Err(e) = writer.write_bytes(&notice_bytes).await {
                                    debug!("Failed to send large transfer notice: {}", e);
                                }
                            }

                            let stats = writer.stats();
                            if stats.rows_sent % 1_000_000 == 0 && stats.rows_sent > 0 {
                                info!("Streaming progress: {} rows, {} MB sent", stats.rows_sent, stats.bytes_sent / (1024 * 1024));
                            }
                        }
                    }
                }
            }
            Some(proto::query_result_batch::Result::Error(err)) => {
                return Err(anyhow::anyhow!("{}: {}", err.code, err.message));
            }
            _ => {}
        }
    }

    if !columns_sent && !skip_row_description {
        let row_desc = build_row_description(&[], &[]);
        writer.write_bytes(&row_desc).await?;
    }

    let stats = writer.stats();
    let tag = format!("SELECT {}", stats.rows_sent);
    let cmd_complete = build_command_complete(&tag);
    writer.write_bytes(&cmd_complete).await?;

    writer.force_flush().await?;

    let final_stats = writer.stats();
    info!(
        "Streaming complete: {} rows, {} bytes, {} flushes ({} slow)",
        final_stats.rows_sent, final_stats.bytes_sent, final_stats.flush_count, final_stats.slow_flush_count
    );

    Ok(final_stats.rows_sent)
}

/// Stream using default worker client
#[allow(dead_code)]
pub(crate) async fn execute_query_streaming_default(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<usize> {
    execute_query_streaming_default_impl(socket, worker_client, sql, user_id, false).await
}

/// Stream using default worker client - internal implementation
#[allow(dead_code)]
pub(crate) async fn execute_query_streaming_default_impl(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    sql: &str,
    user_id: &str,
    skip_row_description: bool,
) -> anyhow::Result<usize> {
    let config = PgWireConfig::default();
    let bp_config = BackpressureConfig {
        flush_threshold_bytes: config.flush_threshold_bytes,
        flush_threshold_rows: config.flush_threshold_rows,
        flush_timeout_secs: config.flush_timeout_secs,
        connection_check_interval_rows: config.connection_check_interval_rows,
        write_buffer_size: config.write_buffer_size,
    };

    match worker_client.execute_query_streaming(sql, user_id).await {
        Ok(mut stream) => {
            let mut writer = BackpressureWriter::new(&mut *socket, bp_config);
            let mut columns_sent = false;

            while let Some(batch) = stream.next().await {
                match batch? {
                    StreamingBatch::Metadata { columns, column_types } => {
                        if !columns_sent && !skip_row_description {
                            let row_desc = build_row_description(&columns, &column_types);
                            writer.write_bytes(&row_desc).await?;
                        }
                        columns_sent = true;
                    }
                    StreamingBatch::Rows(rows) => {
                        for row in rows {
                            let data_row = build_data_row(&row);
                            
                            match writer.write_bytes(&data_row).await {
                                Ok(_) => {}
                                Err(e) if is_disconnect_error(&e) => {
                                    let stats = writer.stats();
                                    warn!("Client disconnected during streaming after {} rows", stats.rows_sent);
                                    return Err(anyhow::anyhow!("Client disconnected"));
                                }
                                Err(e) => return Err(e.into()),
                            }
                            writer.row_sent();

                            if writer.should_flush() {
                                match writer.flush_with_backpressure().await {
                                    Ok(true) => {}
                                    Ok(false) => {
                                        let stats = writer.stats();
                                        debug!("Slow client detected after {} rows", stats.rows_sent);
                                    }
                                    Err(e) if is_disconnect_error(&e) => {
                                        let stats = writer.stats();
                                        warn!("Client disconnected during flush after {} rows", stats.rows_sent);
                                        return Err(anyhow::anyhow!("Client disconnected"));
                                    }
                                    Err(e) => {
                                        warn!("Flush failed: {} - client may be overwhelmed", e);
                                        return Err(anyhow::anyhow!("Client too slow: {}", e));
                                    }
                                }
                            }

                            if writer.should_check_connection() {
                                if !writer.is_connected().await {
                                    let stats = writer.stats();
                                    warn!("Client disconnected during streaming after {} rows", stats.rows_sent);
                                    return Err(anyhow::anyhow!("Client disconnected"));
                                }
                            }

                            if let Some(current_gb) = writer.stats_mut().should_warn_large_transfer() {
                                let notice_msg = format!(
                                    "Large data transfer: {}GB sent. Consider using LIMIT or CURSOR for very large results.",
                                    current_gb
                                );
                                warn!("Large transfer warning: {}GB sent to client", current_gb);
                                let notice_bytes = build_notice_message(&notice_msg);
                                if let Err(e) = writer.write_bytes(&notice_bytes).await {
                                    debug!("Failed to send large transfer notice: {}", e);
                                }
                            }
                        }
                    }
                    StreamingBatch::Error(msg) => {
                        return Err(anyhow::anyhow!("{}", msg));
                    }
                }
            }

            if !columns_sent && !skip_row_description {
                let row_desc = build_row_description(&[], &[]);
                writer.write_bytes(&row_desc).await?;
            }

            let stats = writer.stats();
            let tag = format!("SELECT {}", stats.rows_sent);
            let cmd_complete = build_command_complete(&tag);
            writer.write_bytes(&cmd_complete).await?;

            writer.force_flush().await?;

            let final_stats = writer.stats();
            debug!(
                "Streaming complete: {} rows, {} bytes, {} flushes",
                final_stats.rows_sent, final_stats.bytes_sent, final_stats.flush_count
            );

            Ok(final_stats.rows_sent)
        }
        Err(e) => {
            warn!("Streaming not available, falling back to buffered: {}", e);
            let result = execute_query_buffered(worker_client, sql, user_id).await?;
            if skip_row_description {
                send_query_result_data_only(socket, result).await
            } else {
                send_query_result_immediate(socket, result).await
            }
        }
    }
}

/// Buffered execution fallback
pub(crate) async fn execute_query_buffered(
    worker_client: &WorkerClient,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<QueryExecutionResult> {
    match worker_client.execute_query(sql, user_id).await {
        Ok(result) => Ok(QueryExecutionResult {
            columns: result.columns.iter().map(|c| (c.name.clone(), c.type_name.clone())).collect(),
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

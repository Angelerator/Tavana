//! Query execution functions
//!
//! All execution functions are generic over async streams, supporting both
//! TLS and non-TLS connections. Includes streaming, backpressure, COPY support,
//! QueryRouter routing, and SmartScaler integration.

use crate::metrics;
use crate::pg_compat;
use crate::pg_wire::backpressure::{
    BackpressureConfig, BackpressureWriter,
    build_row_description_with_formats,
    build_command_complete, build_data_row_with_formats,
    is_disconnect_error,
};
use crate::query_router::{QueryRouter, QueryTarget};
use crate::smart_scaler::SmartScaler;
use crate::worker_client::{StreamingBatch, WorkerClient};
use super::config::PgWireConfig;
use super::messages::{
    send_command_complete, send_copy_data_row,
    send_copy_done, send_copy_out_response_header,
    send_query_result_data_only, send_query_result_immediate,
    send_simple_result,
};
use super::utils::{
    build_notice_message, extract_copy_inner_query, handle_pg_specific_command,
    execute_local_fallback, QueryExecutionResult, STREAMING_BATCH_SIZE,
};
use tavana_common::proto;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

/// Execute a simple query (handles PG commands, COPY, routing)
///
/// This is the main entry point for simple query execution from the query loop.
pub(crate) async fn execute_simple_query<S>(
    socket: &mut S,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    smart_scaler: Option<&SmartScaler>,
) -> anyhow::Result<usize>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Intercept PostgreSQL-specific commands locally
    if let Some(result) = handle_pg_specific_command(sql) {
        let columns: Vec<(&str, i32)> = result.columns.iter().map(|(n, _t)| (n.as_str(), 25i32)).collect();
        send_simple_result(socket, &columns, &result.rows, result.command_tag.as_deref()).await?;
        return Ok(0);
    }

    // Handle COPY commands by extracting inner query
    let is_copy_command = sql.trim().to_uppercase().starts_with("COPY");
    let actual_sql = if let Some(inner) = extract_copy_inner_query(sql) {
        info!("Rewrote COPY command to inner query: {}", &inner[..inner.len().min(80)]);
        inner
    } else {
        sql.to_string()
    };

    let actual_sql = pg_compat::rewrite_pg_to_duckdb(&actual_sql);

    if is_copy_command {
        // COPY uses direct streaming (no routing needed)
        return execute_copy_query(socket, worker_client, &actual_sql, user_id).await;
    }

    // Regular query: route via QueryRouter with optional SmartScaler
    execute_routed_query(socket, worker_client, query_router, &actual_sql, user_id, smart_scaler, false, &[], &[]).await
}

/// Execute a COPY query via direct streaming
async fn execute_copy_query<S>(
    socket: &mut S,
    worker_client: &WorkerClient,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<usize>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    metrics::query_started();
    let start_time = std::time::Instant::now();

    match worker_client.execute_query_streaming(sql, user_id).await {
        Ok(mut stream) => {
            let mut columns_sent = false;
            let mut total_rows: usize = 0;
            let mut batch_rows: usize = 0;
            let mut column_names: Vec<String> = vec![];

            while let Some(batch) = stream.next().await {
                match batch? {
                    StreamingBatch::Metadata { columns, column_types: _ } => {
                        column_names = columns;
                        if !columns_sent {
                            send_copy_out_response_header(socket, column_names.len()).await?;
                            columns_sent = true;
                        }
                    }
                    StreamingBatch::Rows(rows) => {
                        for row in rows {
                            send_copy_data_row(socket, &row).await?;
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

            send_copy_done(socket).await?;
            let tag = format!("SELECT {}", total_rows);
            send_command_complete(socket, &tag).await?;
            socket.flush().await?;

            metrics::query_ended();
            debug!(
                rows = total_rows,
                elapsed_ms = start_time.elapsed().as_millis(),
                "COPY query completed"
            );

            Ok(total_rows)
        }
        Err(e) => {
            error!("COPY streaming query failed: {}", e);
            Err(e)
        }
    }
}

/// Route and execute a query with backpressure
pub(crate) async fn execute_routed_query<S>(
    socket: &mut S,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    smart_scaler: Option<&SmartScaler>,
    skip_row_description: bool,
    format_codes: &[i16],
    column_types_hint: &[String],
) -> anyhow::Result<usize>
where
    S: AsyncWrite + Unpin,
{
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
        return execute_with_smart_scaler(
            socket, worker_client, query_router, scaler, sql, user_id, &estimate,
            skip_row_description, format_codes, column_types_hint,
        ).await;
    }

    let (result, worker_name) = match estimate.target {
        QueryTarget::PreSizedWorker { address, worker_name } => {
            info!("Using pre-sized worker {} at {}", worker_name, address);
            metrics::update_worker_active_queries(&worker_name, 1);
            let result = execute_to_worker(socket, &address, sql, user_id, skip_row_description, format_codes, column_types_hint).await;
            metrics::update_worker_active_queries(&worker_name, 0);
            (result, Some(worker_name))
        }
        QueryTarget::TenantPool { service_addr, tenant_id } => {
            info!("Using tenant pool {} at {}", tenant_id, service_addr);
            let result = execute_to_worker(socket, &service_addr, sql, user_id, skip_row_description, format_codes, column_types_hint).await;
            (result, None)
        }
        QueryTarget::WorkerPool => {
            let result = execute_default(socket, worker_client, sql, user_id, skip_row_description, format_codes, column_types_hint).await;
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

/// Execute with SmartScaler worker selection
async fn execute_with_smart_scaler<S>(
    socket: &mut S,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    scaler: &SmartScaler,
    sql: &str,
    user_id: &str,
    estimate: &crate::query_router::QueryEstimate,
    skip_row_description: bool,
    format_codes: &[i16],
    column_types_hint: &[String],
) -> anyhow::Result<usize>
where
    S: AsyncWrite + Unpin,
{
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
            return execute_default(socket, worker_client, sql, user_id, skip_row_description, format_codes, column_types_hint).await;
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

    let result = execute_to_worker(socket, &selection.worker_address, sql, user_id, skip_row_description, format_codes, column_types_hint).await;

    elastic_handle.abort();

    scaler.query_completed(&selection.worker_name, &query_id).await;
    query_router.release_worker(&selection.worker_name, None).await;

    metrics::update_worker_active_queries(&selection.worker_name, 0);
    metrics::query_ended();

    info!("SmartScaler query {} completed on worker {}", query_id, selection.worker_name);

    result
}

/// Stream query to a specific worker via gRPC
async fn execute_to_worker<S>(
    socket: &mut S,
    worker_addr: &str,
    sql: &str,
    user_id: &str,
    skip_row_description: bool,
    format_codes: &[i16],
    column_types_hint: &[String],
) -> anyhow::Result<usize>
where
    S: AsyncWrite + Unpin,
{
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
    let mut streaming_column_types: Vec<String> = column_types_hint.to_vec();

    while let Some(batch) = stream.message().await? {
        match batch.result {
            Some(proto::query_result_batch::Result::Metadata(meta)) => {
                if !columns_sent && !skip_row_description {
                    let row_desc = build_row_description_with_formats(&meta.columns, &meta.column_types, format_codes);
                    writer.write_bytes(&row_desc).await?;
                }
                if streaming_column_types.is_empty() {
                    streaming_column_types = meta.column_types.clone();
                }
                columns_sent = true;
            }
            Some(proto::query_result_batch::Result::RecordBatch(batch)) => {
                if !batch.data.is_empty() {
                    if let Ok(rows) = serde_json::from_slice::<Vec<Vec<String>>>(&batch.data) {
                        for row in rows {
                            let data_row = build_data_row_with_formats(&row, &streaming_column_types, format_codes);

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
        let row_desc = build_row_description_with_formats(&[], &[], format_codes);
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
async fn execute_default<S>(
    socket: &mut S,
    worker_client: &WorkerClient,
    sql: &str,
    user_id: &str,
    skip_row_description: bool,
    format_codes: &[i16],
    column_types_hint: &[String],
) -> anyhow::Result<usize>
where
    S: AsyncWrite + Unpin,
{
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
            let mut streaming_column_types: Vec<String> = column_types_hint.to_vec();

            while let Some(batch) = stream.next().await {
                match batch? {
                    StreamingBatch::Metadata { columns, column_types } => {
                        if !columns_sent && !skip_row_description {
                            let row_desc = build_row_description_with_formats(&columns, &column_types, format_codes);
                            writer.write_bytes(&row_desc).await?;
                        }
                        if streaming_column_types.is_empty() {
                            streaming_column_types = column_types;
                        }
                        columns_sent = true;
                    }
                    StreamingBatch::Rows(rows) => {
                        for row in rows {
                            let data_row = build_data_row_with_formats(&row, &streaming_column_types, format_codes);

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
                let row_desc = build_row_description_with_formats(&[], &[], format_codes);
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

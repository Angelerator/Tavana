//! PostgreSQL wire protocol message sending functions
//!
//! Functions for constructing and sending protocol messages to clients.
//! Includes both TcpStream-specific and generic (TLS-compatible) variants.

use crate::errors::classify_error;
use crate::pg_wire::protocol::types::{pg_type_len, pg_type_oid};
use super::utils::{QueryExecutionResult, STREAMING_BATCH_SIZE};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::debug;

// ===== Generic Stream Functions (TLS-compatible) =====

/// Send parameter status for generic streams
pub(crate) async fn send_parameter_status_generic<S>(socket: &mut S, name: &str, value: &str) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let total_len = 4 + name.len() + 1 + value.len() + 1;
    let mut msg = vec![b'S'];
    msg.extend_from_slice(&(total_len as u32).to_be_bytes());
    msg.extend_from_slice(name.as_bytes());
    msg.push(0);
    msg.extend_from_slice(value.as_bytes());
    msg.push(0);
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send error message for generic streams
pub(crate) async fn send_error_generic<S>(socket: &mut S, message: &str) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    send_classified_error_generic(socket, message).await
}

/// Send a classified error with proper SQLSTATE code, message, hint, and detail
pub(crate) async fn send_classified_error_generic<S>(socket: &mut S, raw_message: &str) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let classified = classify_error(raw_message);
    
    debug!(
        category = %classified.category,
        sqlstate = %classified.sqlstate,
        message = %classified.message,
        raw = %classified.raw_error,
        "Sending classified error to client"
    );
    
    let mut fields = Vec::new();
    
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR");
    fields.push(0);
    
    fields.push(b'V');
    fields.extend_from_slice(b"ERROR");
    fields.push(0);
    
    fields.push(b'C');
    fields.extend_from_slice(classified.sqlstate.as_bytes());
    fields.push(0);
    
    fields.push(b'M');
    fields.extend_from_slice(classified.message.as_bytes());
    fields.push(0);
    
    if let Some(ref detail) = classified.detail {
        if !detail.is_empty() {
            fields.push(b'D');
            fields.extend_from_slice(detail.as_bytes());
            fields.push(0);
        }
    }
    
    if let Some(ref hint) = classified.hint {
        fields.push(b'H');
        fields.extend_from_slice(hint.as_bytes());
        fields.push(0);
    }
    
    fields.push(0);
    
    let total_len = 4 + fields.len();
    let mut error_msg = vec![b'E'];
    error_msg.extend_from_slice(&(total_len as u32).to_be_bytes());
    error_msg.extend_from_slice(&fields);
    
    socket.write_all(&error_msg).await?;
    socket.flush().await?;

    Ok(())
}

/// Send simple result for generic streams
pub(crate) async fn send_simple_result_generic<S>(socket: &mut S, columns: &[(&str, i32)], rows: &[Vec<String>], command_tag: Option<&str>) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    if !columns.is_empty() {
        let mut row_desc = vec![b'T'];
        let num_fields = columns.len() as i16;
        let mut fields_data = Vec::new();
        fields_data.extend_from_slice(&num_fields.to_be_bytes());

        for (col_name, _type_oid) in columns {
            fields_data.extend_from_slice(col_name.as_bytes());
            fields_data.push(0);
            fields_data.extend_from_slice(&0i32.to_be_bytes());
            fields_data.extend_from_slice(&0i16.to_be_bytes());
            fields_data.extend_from_slice(&25i32.to_be_bytes());
            fields_data.extend_from_slice(&(-1i16).to_be_bytes());
            fields_data.extend_from_slice(&(-1i32).to_be_bytes());
            fields_data.extend_from_slice(&0i16.to_be_bytes());
        }

        let row_desc_len = (4 + fields_data.len()) as u32;
        row_desc.extend_from_slice(&row_desc_len.to_be_bytes());
        row_desc.extend_from_slice(&fields_data);
        socket.write_all(&row_desc).await?;

        for row in rows {
            let mut data_row = vec![b'D'];
            let mut row_data = Vec::new();
            row_data.extend_from_slice(&(row.len() as i16).to_be_bytes());

            for val in row {
                let bytes = val.as_bytes();
                row_data.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                row_data.extend_from_slice(bytes);
            }

            let row_len = (4 + row_data.len()) as u32;
            data_row.extend_from_slice(&row_len.to_be_bytes());
            data_row.extend_from_slice(&row_data);
            socket.write_all(&data_row).await?;
        }
    }

    let cmd = match command_tag {
        Some(tag) => tag.to_string(),
        None => format!("SELECT {}", rows.len()),
    };
    let cmd_bytes = cmd.as_bytes();
    let mut complete = vec![b'C'];
    complete.extend_from_slice(&((4 + cmd_bytes.len() + 1) as u32).to_be_bytes());
    complete.extend_from_slice(cmd_bytes);
    complete.push(0);
    socket.write_all(&complete).await?;
    socket.flush().await?;

    Ok(())
}

/// Send only DataRows for Extended Query Protocol Execute phase
#[allow(dead_code)]
pub(crate) async fn send_data_rows_only_generic<S>(socket: &mut S, rows: &[Vec<String>], row_count: usize, command_tag: Option<&str>, expected_column_count: usize) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    for row in rows {
        let mut data_row = vec![b'D'];
        let mut row_data = Vec::new();
        
        let actual_cols = row.len();
        let cols_to_send = if expected_column_count > 0 { expected_column_count } else { actual_cols };
        
        row_data.extend_from_slice(&(cols_to_send as i16).to_be_bytes());

        for i in 0..cols_to_send {
            if i < actual_cols {
                let bytes = row[i].as_bytes();
                row_data.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                row_data.extend_from_slice(bytes);
            } else {
                row_data.extend_from_slice(&(-1i32).to_be_bytes());
            }
        }

        let row_len = (4 + row_data.len()) as u32;
        data_row.extend_from_slice(&row_len.to_be_bytes());
        data_row.extend_from_slice(&row_data);
        socket.write_all(&data_row).await?;
    }

    let cmd = match command_tag {
        Some(tag) => tag.to_string(),
        None => format!("SELECT {}", row_count),
    };
    let cmd_bytes = cmd.as_bytes();
    let mut complete = vec![b'C'];
    complete.extend_from_slice(&((4 + cmd_bytes.len() + 1) as u32).to_be_bytes());
    complete.extend_from_slice(cmd_bytes);
    complete.push(0);
    socket.write_all(&complete).await?;
    socket.flush().await?;

    Ok(())
}

/// Send COPY OUT protocol response
#[allow(dead_code)]
pub(crate) async fn send_copy_out_response<S>(
    socket: &mut S,
    columns: &[(String, String)],
    rows: &[Vec<String>],
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let num_cols = columns.len() as i16;
    let msg_len = 4 + 1 + 2 + (2 * columns.len() as i32);
    let mut copy_out_resp = vec![b'H'];
    copy_out_resp.extend_from_slice(&(msg_len as u32).to_be_bytes());
    copy_out_resp.push(0);
    copy_out_resp.extend_from_slice(&num_cols.to_be_bytes());
    for _ in 0..columns.len() {
        copy_out_resp.extend_from_slice(&0i16.to_be_bytes());
    }
    socket.write_all(&copy_out_resp).await?;
    
    for row in rows {
        let row_text = row.join("\t") + "\n";
        let row_bytes = row_text.as_bytes();
        
        let mut copy_data = vec![b'd'];
        copy_data.extend_from_slice(&((4 + row_bytes.len()) as u32).to_be_bytes());
        copy_data.extend_from_slice(row_bytes);
        socket.write_all(&copy_data).await?;
    }
    
    socket.write_all(&[b'c', 0, 0, 0, 4]).await?;
    
    let cmd = format!("COPY {}", rows.len());
    let cmd_bytes = cmd.as_bytes();
    let mut complete = vec![b'C'];
    complete.extend_from_slice(&((4 + cmd_bytes.len() + 1) as u32).to_be_bytes());
    complete.extend_from_slice(cmd_bytes);
    complete.push(0);
    socket.write_all(&complete).await?;
    
    socket.flush().await?;
    
    debug!("Sent COPY OUT text response: {} rows", rows.len());
    Ok(())
}

/// Send RowDescription for generic streams (TLS)
pub(crate) async fn send_row_description_generic<S>(
    socket: &mut S,
    columns: &[(String, String)],
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    send_row_description_generic_with_formats(socket, columns, &[]).await
}

/// Send RowDescription with format codes for binary support
pub(crate) async fn send_row_description_generic_with_formats<S>(
    socket: &mut S,
    columns: &[(String, String)],
    format_codes: &[i16],
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut msg = Vec::new();
    msg.push(b'T');
    msg.extend_from_slice(&[0, 0, 0, 0]);
    msg.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    
    for (i, (name, type_name)) in columns.iter().enumerate() {
        let format = match format_codes.len() {
            0 => 0i16,
            1 => format_codes[0],
            _ => format_codes.get(i).copied().unwrap_or(0),
        };
        
        msg.extend_from_slice(name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(&0u32.to_be_bytes());
        msg.extend_from_slice(&0i16.to_be_bytes());
        msg.extend_from_slice(&pg_type_oid(type_name).to_be_bytes());
        msg.extend_from_slice(&pg_type_len(type_name).to_be_bytes());
        msg.extend_from_slice(&(-1i32).to_be_bytes());
        msg.extend_from_slice(&format.to_be_bytes());
    }
    
    let len = (msg.len() - 1) as u32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());
    
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send CommandComplete for generic streams (TLS)
pub(crate) async fn send_command_complete_generic<S>(socket: &mut S, tag: &str) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut msg = Vec::new();
    msg.push(b'C');
    let tag_bytes = tag.as_bytes();
    let len = (4 + tag_bytes.len() + 1) as u32;
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(tag_bytes);
    msg.push(0);
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send CopyOutResponse header for generic streams (TLS)
pub(crate) async fn send_copy_out_response_header_generic<S>(socket: &mut S, column_count: usize) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut msg = Vec::new();
    msg.push(b'H');
    let body_len = 4 + 1 + 2 + (column_count * 2);
    msg.extend_from_slice(&(body_len as u32).to_be_bytes());
    msg.push(0);
    msg.extend_from_slice(&(column_count as i16).to_be_bytes());
    for _ in 0..column_count {
        msg.extend_from_slice(&0i16.to_be_bytes());
    }
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send CopyData row for generic streams (TLS)
pub(crate) async fn send_copy_data_row_generic<S>(socket: &mut S, row: &[String]) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let row_text = row.join("\t") + "\n";
    let row_bytes = row_text.as_bytes();
    
    let mut msg = Vec::new();
    msg.push(b'd');
    let len = (4 + row_bytes.len()) as u32;
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(row_bytes);
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send CopyDone for generic streams (TLS)
pub(crate) async fn send_copy_done_generic<S>(socket: &mut S) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    socket.write_all(&[b'c', 0, 0, 0, 4]).await?;
    Ok(())
}

/// Send a single DataRow for generic streams (TLS)
pub(crate) async fn send_data_row_generic<S>(socket: &mut S, row: &[String], expected_cols: usize) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut data_row = Vec::with_capacity(5 + row.len() * 20);
    data_row.push(b'D');

    let mut row_data = Vec::with_capacity(2 + row.len() * 20);
    let cols_to_send = if expected_cols > 0 { expected_cols } else { row.len() };
    row_data.extend_from_slice(&(cols_to_send as i16).to_be_bytes());

    for i in 0..cols_to_send {
        if i < row.len() {
            let value = &row[i];
            if value == "NULL" {
                row_data.extend_from_slice(&(-1i32).to_be_bytes());
            } else if value.is_empty() {
                row_data.extend_from_slice(&0i32.to_be_bytes());
            } else {
                let sanitized: String = value.chars().filter(|&c| c != '\0').collect();
                row_data.extend_from_slice(&(sanitized.len() as i32).to_be_bytes());
                row_data.extend_from_slice(sanitized.as_bytes());
            }
        } else {
            row_data.extend_from_slice(&(-1i32).to_be_bytes());
        }
    }

    let len = (4 + row_data.len()) as u32;
    data_row.extend_from_slice(&len.to_be_bytes());
    data_row.extend_from_slice(&row_data);

    socket.write_all(&data_row).await?;
    Ok(())
}

// ===== TcpStream-Specific Functions =====

/// Send RowDescription message (TcpStream)
pub(crate) async fn send_row_description(
    socket: &mut tokio::net::TcpStream,
    columns: &[String],
    column_types: &[String],
) -> anyhow::Result<()> {
    let mut row_desc = Vec::new();
    row_desc.push(b'T');

    let field_count = columns.len() as i16;
    let mut fields_data = Vec::new();
    fields_data.extend_from_slice(&field_count.to_be_bytes());

    for (i, name) in columns.iter().enumerate() {
        let type_name = column_types.get(i).map(|s| s.as_str()).unwrap_or("text");

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

    Ok(())
}

/// Send a single DataRow message (TcpStream)
pub(crate) async fn send_data_row(socket: &mut tokio::net::TcpStream, row: &[String]) -> anyhow::Result<()> {
    let mut data_row = Vec::with_capacity(5 + row.len() * 20);
    data_row.push(b'D');

    let mut row_data = Vec::with_capacity(2 + row.len() * 20);
    row_data.extend_from_slice(&(row.len() as i16).to_be_bytes());

    for value in row {
        if value.is_empty() || value == "null" || value == "NULL" {
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

    Ok(())
}

/// Send query result immediately (TcpStream)
pub(crate) async fn send_query_result_immediate(
    socket: &mut tokio::net::TcpStream,
    result: QueryExecutionResult,
) -> anyhow::Result<usize> {
    if result.columns.is_empty() {
        let tag = result
            .command_tag
            .unwrap_or_else(|| format!("OK {}", result.row_count));
        send_command_complete(socket, &tag).await?;
        return Ok(0);
    }

    let columns: Vec<String> = result.columns.iter().map(|(n, _)| n.clone()).collect();
    let types: Vec<String> = result.columns.iter().map(|(_, t)| t.clone()).collect();
    send_row_description(socket, &columns, &types).await?;

    let mut count = 0;
    for row in &result.rows {
        send_data_row(socket, row).await?;
        count += 1;

        if count % STREAMING_BATCH_SIZE == 0 {
            socket.flush().await?;
        }
    }

    let tag = result.command_tag.unwrap_or_else(|| format!("SELECT {}", result.rows.len()));
    send_command_complete(socket, &tag).await?;

    Ok(result.rows.len())
}

/// Send query result without RowDescription (TcpStream, Extended Protocol)
pub(crate) async fn send_query_result_data_only(
    socket: &mut tokio::net::TcpStream,
    result: QueryExecutionResult,
) -> anyhow::Result<usize> {
    if result.columns.is_empty() {
        let tag = result
            .command_tag
            .unwrap_or_else(|| format!("OK {}", result.row_count));
        send_command_complete(socket, &tag).await?;
        return Ok(0);
    }

    let mut count = 0;
    for row in &result.rows {
        send_data_row(socket, row).await?;
        count += 1;

        if count % STREAMING_BATCH_SIZE == 0 {
            socket.flush().await?;
        }
    }

    let tag = format!("SELECT {}", result.rows.len());
    send_command_complete(socket, &tag).await?;

    Ok(result.rows.len())
}

/// Send CommandComplete (TcpStream)
pub(crate) async fn send_command_complete(
    socket: &mut tokio::net::TcpStream,
    tag: &str,
) -> anyhow::Result<()> {
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

/// Send error (TcpStream)
pub(crate) async fn send_error(socket: &mut tokio::net::TcpStream, message: &str) -> anyhow::Result<()> {
    send_classified_error_tcp(socket, message).await
}

/// Send a classified error to a TcpStream
pub(crate) async fn send_classified_error_tcp(socket: &mut tokio::net::TcpStream, raw_message: &str) -> anyhow::Result<()> {
    let classified = classify_error(raw_message);
    
    debug!(
        category = %classified.category,
        sqlstate = %classified.sqlstate,
        message = %classified.message,
        "Sending classified error to client (TCP)"
    );
    
    let mut fields = Vec::new();
    
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR");
    fields.push(0);
    
    fields.push(b'V');
    fields.extend_from_slice(b"ERROR");
    fields.push(0);
    
    fields.push(b'C');
    fields.extend_from_slice(classified.sqlstate.as_bytes());
    fields.push(0);
    
    fields.push(b'M');
    fields.extend_from_slice(classified.message.as_bytes());
    fields.push(0);
    
    if let Some(ref detail) = classified.detail {
        if !detail.is_empty() {
            fields.push(b'D');
            fields.extend_from_slice(detail.as_bytes());
            fields.push(0);
        }
    }
    
    if let Some(ref hint) = classified.hint {
        fields.push(b'H');
        fields.extend_from_slice(hint.as_bytes());
        fields.push(0);
    }
    
    fields.push(0);
    
    let mut msg = Vec::new();
    msg.push(b'E');
    let len = 4 + fields.len();
    msg.extend_from_slice(&(len as u32).to_be_bytes());
    msg.extend_from_slice(&fields);
    socket.write_all(&msg).await?;
    socket.flush().await?;
    Ok(())
}

/// Send error response with specific SQLSTATE code (TcpStream)
#[allow(dead_code)]
pub(crate) async fn send_error_response(socket: &mut tokio::net::TcpStream, code: &str, message: &str) -> anyhow::Result<()> {
    let code_bytes = format!("{}\0", code);

    let mut msg = Vec::new();
    msg.push(b'E');
    let mut fields = Vec::new();
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR\0");
    fields.push(b'C');
    fields.extend_from_slice(code_bytes.as_bytes());
    fields.push(b'M');
    fields.extend_from_slice(message.as_bytes());
    fields.push(0);
    fields.push(0);
    let len = 4 + fields.len();
    msg.extend_from_slice(&(len as u32).to_be_bytes());
    msg.extend_from_slice(&fields);
    socket.write_all(&msg).await?;
    socket.flush().await?;
    Ok(())
}

/// Send notice (TcpStream)
#[allow(dead_code)]
pub(crate) async fn send_notice(socket: &mut tokio::net::TcpStream, message: &str) -> anyhow::Result<()> {
    let mut msg = Vec::new();
    msg.push(b'N');
    let mut fields = Vec::new();
    fields.push(b'S');
    fields.extend_from_slice(b"WARNING\0");
    fields.push(b'C');
    fields.extend_from_slice(b"01000\0");
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

/// Send parameter status (TcpStream)
pub(crate) async fn send_parameter_status(
    socket: &mut tokio::net::TcpStream,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
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

/// Send RowDescription for Describe response (TcpStream)
pub(crate) async fn send_row_description_for_describe(
    socket: &mut tokio::net::TcpStream,
    columns: &[(String, String)],
) -> anyhow::Result<()> {
    let mut msg = Vec::new();
    msg.push(b'T');
    msg.extend_from_slice(&[0, 0, 0, 0]);
    msg.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    
    for (name, type_name) in columns {
        msg.extend_from_slice(name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(&0u32.to_be_bytes());
        msg.extend_from_slice(&0i16.to_be_bytes());
        msg.extend_from_slice(&pg_type_oid(type_name).to_be_bytes());
        msg.extend_from_slice(&pg_type_len(type_name).to_be_bytes());
        msg.extend_from_slice(&(-1i32).to_be_bytes());
        msg.extend_from_slice(&0i16.to_be_bytes());
    }
    
    let len = (msg.len() - 1) as u32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());
    
    socket.write_all(&msg).await?;
    Ok(())
}

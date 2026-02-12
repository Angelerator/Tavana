//! PostgreSQL wire protocol message builders
//!
//! Functions for constructing and sending protocol messages to clients.

use super::types::{pg_type_len, pg_type_oid};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::debug;

/// Send a PostgreSQL NOTICE message to inform client about something
pub async fn send_notice_message<S: AsyncWrite + Unpin>(
    socket: &mut S,
    message: &str,
) -> std::io::Result<()> {
    // NoticeResponse message format:
    // 'N' + length (4 bytes) + fields (severity, message, etc.) + terminator (0)
    let mut buf = Vec::new();
    buf.push(b'N');
    
    // We'll fill in length after building the message
    buf.extend_from_slice(&[0, 0, 0, 0]); // Placeholder for length
    
    // Severity field (S)
    buf.push(b'S');
    buf.extend_from_slice(b"NOTICE");
    buf.push(0);
    
    // SQLSTATE code (C) - use 00000 for success/notice
    buf.push(b'C');
    buf.extend_from_slice(b"00000");
    buf.push(0);
    
    // Message field (M)
    buf.push(b'M');
    buf.extend_from_slice(message.as_bytes());
    buf.push(0);
    
    // Terminator
    buf.push(0);
    
    // Fill in length (message length minus the type byte)
    let len = (buf.len() - 1) as u32;
    buf[1..5].copy_from_slice(&len.to_be_bytes());
    
    socket.write_all(&buf).await
}

/// Send ParameterStatus message
pub async fn send_parameter_status<S>(socket: &mut S, name: &str, value: &str) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut msg = Vec::new();
    msg.push(b'S'); // ParameterStatus
    // Length placeholder
    msg.extend_from_slice(&[0, 0, 0, 0]);
    // Parameter name (null-terminated)
    msg.extend_from_slice(name.as_bytes());
    msg.push(0);
    // Parameter value (null-terminated)
    msg.extend_from_slice(value.as_bytes());
    msg.push(0);
    // Fill in length
    let len = (msg.len() - 1) as u32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send ErrorResponse message
pub async fn send_error<S>(socket: &mut S, message: &str) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut buf = Vec::new();
    buf.push(b'E'); // ErrorResponse

    // Error fields
    let mut fields = Vec::new();
    // Severity
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR");
    fields.push(0);
    // SQLSTATE (generic internal error)
    fields.push(b'C');
    fields.extend_from_slice(b"XX000");
    fields.push(0);
    // Message
    fields.push(b'M');
    fields.extend_from_slice(message.as_bytes());
    fields.push(0);
    // Terminator
    fields.push(0);

    // Length = 4 + fields length
    let len = (4 + fields.len()) as u32;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&fields);

    socket.write_all(&buf).await?;
    Ok(())
}

/// Send ErrorResponse with custom SQLSTATE code
pub async fn send_error_with_code<S>(
    socket: &mut S,
    code: &str,
    message: &str,
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut buf = Vec::new();
    buf.push(b'E'); // ErrorResponse

    // Error fields
    let mut fields = Vec::new();
    // Severity
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR");
    fields.push(0);
    // SQLSTATE
    fields.push(b'C');
    fields.extend_from_slice(code.as_bytes());
    fields.push(0);
    // Message
    fields.push(b'M');
    fields.extend_from_slice(message.as_bytes());
    fields.push(0);
    // Terminator
    fields.push(0);

    // Length = 4 + fields length
    let len = (4 + fields.len()) as u32;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&fields);

    socket.write_all(&buf).await?;
    Ok(())
}

/// Send a rich ErrorResponse with SQLSTATE code, message, hint, and detail
/// This follows the PostgreSQL wire protocol ErrorResponse format:
/// - 'S' Severity: ERROR
/// - 'V' Severity (non-localized): ERROR  
/// - 'C' SQLSTATE code
/// - 'M' Message (primary human-readable message)
/// - 'D' Detail (optional secondary message)
/// - 'H' Hint (optional suggestion)
pub async fn send_classified_error<S>(
    socket: &mut S,
    error: &crate::errors::ClassifiedError,
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut buf = Vec::new();
    buf.push(b'E'); // ErrorResponse

    let mut fields = Vec::new();
    
    // Severity (localized)
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR");
    fields.push(0);
    
    // Severity (non-localized, for programmatic use)
    fields.push(b'V');
    fields.extend_from_slice(b"ERROR");
    fields.push(0);
    
    // SQLSTATE code
    fields.push(b'C');
    fields.extend_from_slice(error.sqlstate.as_bytes());
    fields.push(0);
    
    // Primary message
    fields.push(b'M');
    fields.extend_from_slice(error.message.as_bytes());
    fields.push(0);
    
    // Detail (if present)
    if let Some(ref detail) = error.detail {
        if !detail.is_empty() {
            fields.push(b'D');
            fields.extend_from_slice(detail.as_bytes());
            fields.push(0);
        }
    }
    
    // Hint (if present)
    if let Some(ref hint) = error.hint {
        fields.push(b'H');
        fields.extend_from_slice(hint.as_bytes());
        fields.push(0);
    }
    
    // Terminator
    fields.push(0);

    let len = (4 + fields.len()) as u32;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&fields);

    socket.write_all(&buf).await?;
    Ok(())
}

/// Send RowDescription message
pub async fn send_row_description<S>(
    socket: &mut S,
    columns: &[(String, String)],
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut msg = Vec::new();
    msg.push(b'T'); // RowDescription
    msg.extend_from_slice(&[0, 0, 0, 0]); // Length placeholder
    msg.extend_from_slice(&(columns.len() as i16).to_be_bytes()); // Field count

    for (name, type_name) in columns {
        msg.extend_from_slice(name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(&0u32.to_be_bytes()); // table OID
        msg.extend_from_slice(&0i16.to_be_bytes()); // column attr
        msg.extend_from_slice(&pg_type_oid(type_name).to_be_bytes());
        msg.extend_from_slice(&pg_type_len(type_name).to_be_bytes());
        msg.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
        msg.extend_from_slice(&0i16.to_be_bytes()); // format code
    }

    let len = (msg.len() - 1) as u32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());

    socket.write_all(&msg).await?;
    Ok(())
}

/// Send RowDescription for TcpStream with column info as tuples
pub async fn send_row_description_tcpstream(
    socket: &mut tokio::net::TcpStream,
    columns: &[(&str, i32)],
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    
    let mut msg = Vec::new();
    msg.push(b'T'); // RowDescription
    msg.extend_from_slice(&[0, 0, 0, 0]); // Length placeholder
    msg.extend_from_slice(&(columns.len() as i16).to_be_bytes());

    for (name, type_oid) in columns {
        msg.extend_from_slice(name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(&0u32.to_be_bytes()); // table OID
        msg.extend_from_slice(&0i16.to_be_bytes()); // column attr
        msg.extend_from_slice(&(*type_oid as u32).to_be_bytes());
        msg.extend_from_slice(&(-1i16).to_be_bytes()); // type size
        msg.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
        msg.extend_from_slice(&0i16.to_be_bytes()); // format code
    }

    let len = (msg.len() - 1) as u32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());

    socket.write_all(&msg).await?;
    Ok(())
}

/// Send RowDescription for Describe phase
pub async fn send_row_description_for_describe(
    socket: &mut tokio::net::TcpStream,
    columns: &[(String, String)],
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    
    let mut msg = Vec::new();
    msg.push(b'T'); // RowDescription
    msg.extend_from_slice(&[0, 0, 0, 0]); // Length placeholder
    msg.extend_from_slice(&(columns.len() as i16).to_be_bytes());

    for (name, type_name) in columns {
        msg.extend_from_slice(name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(&0u32.to_be_bytes()); // table OID
        msg.extend_from_slice(&0i16.to_be_bytes()); // column attr number
        msg.extend_from_slice(&pg_type_oid(type_name).to_be_bytes());
        msg.extend_from_slice(&pg_type_len(type_name).to_be_bytes());
        msg.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
        // Format code (0 = text)
        msg.extend_from_slice(&0i16.to_be_bytes());
    }

    // Fill in the length
    let len = (msg.len() - 1) as u32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());

    socket.write_all(&msg).await?;
    Ok(())
}

/// Send CommandComplete message
pub async fn send_command_complete<S>(socket: &mut S, tag: &str) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut msg = Vec::new();
    msg.push(b'C'); // CommandComplete
    let tag_bytes = tag.as_bytes();
    let len = (4 + tag_bytes.len() + 1) as u32;
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(tag_bytes);
    msg.push(0); // Null terminator
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send a single DataRow
pub async fn send_data_row<S>(
    socket: &mut S,
    row: &[String],
    expected_cols: usize,
) -> anyhow::Result<()>
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
            // Treat explicit "NULL" strings as NULL, but preserve empty strings
            if value == "NULL" {
                row_data.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
            } else if value.is_empty() {
                // Empty string: length 0, no bytes
                row_data.extend_from_slice(&0i32.to_be_bytes());
            } else {
                // CRITICAL: PostgreSQL text format should not contain NULL bytes
                // Remove any embedded NULL bytes to prevent parsing issues
                let sanitized: String = value.chars().filter(|&c| c != '\0').collect();
                row_data.extend_from_slice(&(sanitized.len() as i32).to_be_bytes());
                row_data.extend_from_slice(sanitized.as_bytes());
            }
        } else {
            row_data.extend_from_slice(&(-1i32).to_be_bytes()); // NULL for missing columns
        }
    }

    let len = (4 + row_data.len()) as u32;
    data_row.extend_from_slice(&len.to_be_bytes());
    data_row.extend_from_slice(&row_data);

    socket.write_all(&data_row).await?;
    Ok(())
}

/// Send DataRow for TcpStream
pub async fn send_data_row_tcpstream(
    socket: &mut tokio::net::TcpStream,
    row: &[String],
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    
    let mut buf = Vec::new();
    buf.push(b'D'); // DataRow
    buf.extend_from_slice(&[0, 0, 0, 0]); // Length placeholder
    buf.extend_from_slice(&(row.len() as i16).to_be_bytes());

    for val in row {
        if val == "NULL" {
            buf.extend_from_slice(&(-1i32).to_be_bytes());
        } else {
            buf.extend_from_slice(&(val.len() as i32).to_be_bytes());
            buf.extend_from_slice(val.as_bytes());
        }
    }

    let len = (buf.len() - 1) as u32;
    buf[1..5].copy_from_slice(&len.to_be_bytes());

    socket.write_all(&buf).await?;
    Ok(())
}

/// Send ReadyForQuery message with transaction status
pub async fn send_ready_for_query<S>(socket: &mut S, status: u8) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    socket.write_all(&[b'Z', 0, 0, 0, 5, status]).await?;
    Ok(())
}

/// Send CopyOutResponse header
pub async fn send_copy_out_response_header<S>(
    socket: &mut S,
    column_count: usize,
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // CopyOutResponse: 'H' + length + format (0=text) + column count + format per column
    let mut msg = Vec::new();
    msg.push(b'H'); // CopyOutResponse
    let body_len = 4 + 1 + 2 + (column_count * 2); // length + format + col count + format per col
    msg.extend_from_slice(&(body_len as u32).to_be_bytes());
    msg.push(0); // Overall format: 0 = text
    msg.extend_from_slice(&(column_count as i16).to_be_bytes());
    for _ in 0..column_count {
        msg.extend_from_slice(&0i16.to_be_bytes()); // Per-column format: 0 = text
    }
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send CopyData row
pub async fn send_copy_data_row<S>(socket: &mut S, row: &[String]) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Build tab-separated row with newline
    let row_text = row.join("\t") + "\n";
    let row_bytes = row_text.as_bytes();

    // CopyData: 'd' + length + data
    let mut msg = Vec::new();
    msg.push(b'd'); // CopyData
    let len = (4 + row_bytes.len()) as u32;
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(row_bytes);
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send CopyDone
pub async fn send_copy_done<S>(socket: &mut S) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // CopyDone: 'c' + length (4)
    socket.write_all(&[b'c', 0, 0, 0, 4]).await?;
    Ok(())
}

/// Send simple result set (RowDescription + DataRows + CommandComplete)
pub async fn send_simple_result<S>(
    socket: &mut S,
    columns: &[(&str, i32)],
    rows: &[Vec<String>],
    command_tag: Option<&str>,
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Build and send RowDescription
    let mut msg = Vec::new();
    msg.push(b'T'); // RowDescription
    msg.extend_from_slice(&[0, 0, 0, 0]); // Length placeholder
    msg.extend_from_slice(&(columns.len() as i16).to_be_bytes());

    for (name, type_oid) in columns {
        msg.extend_from_slice(name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(&0u32.to_be_bytes()); // table OID
        msg.extend_from_slice(&0i16.to_be_bytes()); // column attr
        msg.extend_from_slice(&(*type_oid as u32).to_be_bytes());
        msg.extend_from_slice(&(-1i16).to_be_bytes()); // type size
        msg.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
        msg.extend_from_slice(&0i16.to_be_bytes()); // format code (text)
    }

    let len = (msg.len() - 1) as u32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());
    socket.write_all(&msg).await?;

    // Send DataRows
    for row in rows {
        let mut row_msg = Vec::new();
        row_msg.push(b'D'); // DataRow
        row_msg.extend_from_slice(&[0, 0, 0, 0]); // Length placeholder
        row_msg.extend_from_slice(&(row.len() as i16).to_be_bytes());

        for val in row {
            if val == "NULL" {
                row_msg.extend_from_slice(&(-1i32).to_be_bytes());
            } else {
                row_msg.extend_from_slice(&(val.len() as i32).to_be_bytes());
                row_msg.extend_from_slice(val.as_bytes());
            }
        }

        let len = (row_msg.len() - 1) as u32;
        row_msg[1..5].copy_from_slice(&len.to_be_bytes());
        socket.write_all(&row_msg).await?;
    }

    // Send CommandComplete
    let tag = command_tag.unwrap_or_else(|| {
        if rows.is_empty() {
            "SELECT 0"
        } else {
            "SELECT"
        }
    });
    let tag_with_count = if tag == "SELECT" {
        format!("SELECT {}", rows.len())
    } else {
        tag.to_string()
    };

    let mut cmd_msg = Vec::new();
    cmd_msg.push(b'C'); // CommandComplete
    let tag_bytes = tag_with_count.as_bytes();
    let len = (4 + tag_bytes.len() + 1) as u32;
    cmd_msg.extend_from_slice(&len.to_be_bytes());
    cmd_msg.extend_from_slice(tag_bytes);
    cmd_msg.push(0);
    socket.write_all(&cmd_msg).await?;

    debug!("Sent simple result: {} rows", rows.len());
    Ok(())
}

/// Send data rows only (without RowDescription, for Execute after Describe)
pub async fn send_data_rows_only<S>(
    socket: &mut S,
    rows: &[Vec<String>],
    _row_count: usize,
    command_tag: Option<&str>,
    expected_column_count: usize,
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Send DataRows
    for row in rows {
        send_data_row(socket, row, expected_column_count).await?;
    }

    // Send CommandComplete
    let tag = command_tag.unwrap_or("SELECT");
    let tag_with_count = format!("{} {}", tag, rows.len());
    send_command_complete(socket, &tag_with_count).await?;

    debug!("Sent data rows only: {} rows", rows.len());
    Ok(())
}

/// Send PortalSuspended message (for cursor-based streaming)
pub async fn send_portal_suspended<S>(socket: &mut S) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    socket.write_all(&[b's', 0, 0, 0, 4]).await?;
    Ok(())
}

/// Send NoData message
pub async fn send_no_data<S>(socket: &mut S) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
    Ok(())
}

/// Send ParseComplete message
pub async fn send_parse_complete<S>(socket: &mut S) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    socket.write_all(&[b'1', 0, 0, 0, 4]).await?;
    Ok(())
}

/// Send BindComplete message
pub async fn send_bind_complete<S>(socket: &mut S) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    socket.write_all(&[b'2', 0, 0, 0, 4]).await?;
    Ok(())
}

/// Send CloseComplete message
pub async fn send_close_complete<S>(socket: &mut S) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    socket.write_all(&[b'3', 0, 0, 0, 4]).await?;
    Ok(())
}

/// Send ParameterDescription (0 parameters)
pub async fn send_empty_parameter_description<S>(socket: &mut S) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    socket.write_all(&[b't', 0, 0, 0, 6, 0, 0]).await?;
    Ok(())
}

/// Send AuthenticationOk message
pub async fn send_authentication_ok<S>(socket: &mut S) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    socket.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await?;
    Ok(())
}

/// Send AuthenticationCleartextPassword request
pub async fn send_auth_cleartext_request<S>(socket: &mut S) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    socket.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 3]).await?;
    Ok(())
}

/// Send BackendKeyData message
pub async fn send_backend_key_data<S>(socket: &mut S, pid: i32, secret: i32) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut msg = vec![b'K', 0, 0, 0, 12];
    msg.extend_from_slice(&pid.to_be_bytes());
    msg.extend_from_slice(&secret.to_be_bytes());
    socket.write_all(&msg).await?;
    Ok(())
}

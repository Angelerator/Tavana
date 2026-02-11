//! Direct Arrow → PostgreSQL DataRow Encoder (ZERO-COPY OPTIMIZED)
//!
//! This module encodes Arrow RecordBatches directly to PostgreSQL wire format
//! WITHOUT intermediate string conversion, eliminating a major performance bottleneck.
//!
//! ## Performance Gains
//!
//! Traditional path: Arrow → Vec<Vec<String>> → DataRow bytes
//! - Allocates String per cell
//! - Copies data twice (Arrow → String → bytes)
//!
//! Optimized path: Arrow → DataRow bytes
//! - Zero allocation for strings (uses Arrow's string slice directly)
//! - Single copy for numeric types (itoa/ryu write directly to buffer)
//! - Uses thread-local buffers for reuse
//!
//! ## Supported Types
//!
//! | Arrow Type      | Text Format    | Binary Format |
//! |-----------------|----------------|---------------|
//! | Int8-Int64      | itoa (fast)    | BE bytes      |
//! | UInt8-UInt64    | itoa (fast)    | BE bytes      |
//! | Float32/Float64 | ryu (fast)     | BE bytes      |
//! | Boolean         | "t"/"f"        | 1 byte        |
//! | Utf8/LargeUtf8  | zero-copy      | UTF-8 bytes   |
//! | Binary          | hex escape     | raw bytes     |
//! | Date32          | YYYY-MM-DD     | days as i32   |
//! | Timestamp       | ISO 8601       | microseconds  |
//! | Null            | -1 length      | -1 length     |
//!
//! ## References
//!
//! - pgpq library: https://github.com/adriangb/pgpq
//! - PostgreSQL wire protocol: https://www.postgresql.org/docs/current/protocol-message-formats.html
//! - itoa: https://github.com/dtolnay/itoa
//! - ryu: https://github.com/dtolnay/ryu

use arrow_array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::DataType;
use bytes::{BufMut, Bytes, BytesMut};
use std::cell::RefCell;

/// Thread-local encoder for batch processing
/// Each thread gets its own encoder to avoid synchronization overhead
thread_local! {
    static ARROW_ENCODER: RefCell<ArrowDataRowEncoder> = RefCell::new(ArrowDataRowEncoder::new());
}

/// DataRow header size: 'D' (1) + length (4) + field count (2) = 7 bytes
const DATAROW_HEADER_SIZE: usize = 7;

/// PostgreSQL epoch offset: days from 1970-01-01 to 2000-01-01
const PG_EPOCH_DAYS: i32 = 10_957;

/// PostgreSQL epoch offset in microseconds
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// Encoder that directly writes Arrow data to PostgreSQL DataRow format
pub struct ArrowDataRowEncoder {
    /// Buffer for the current row
    buffer: BytesMut,
    /// Reusable buffer for itoa formatting
    itoa_buffer: itoa::Buffer,
    /// Reusable buffer for ryu formatting
    ryu_buffer: ryu::Buffer,
}

impl ArrowDataRowEncoder {
    fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(8192),
            itoa_buffer: itoa::Buffer::new(),
            ryu_buffer: ryu::Buffer::new(),
        }
    }

    /// Prepare buffer for a new row by reserving header space
    #[inline]
    fn prepare(&mut self) {
        self.buffer.clear();
        self.buffer.resize(DATAROW_HEADER_SIZE, 0);
    }

    /// Take the encoded row using split() - TRUE zero copy!
    #[inline]
    fn take_row(&mut self, field_count: i16) -> Bytes {
        let data_len = self.buffer.len() - DATAROW_HEADER_SIZE;
        let length_value = (4 + 2 + data_len) as u32;

        self.buffer[0] = b'D';
        self.buffer[1..5].copy_from_slice(&length_value.to_be_bytes());
        self.buffer[5..7].copy_from_slice(&field_count.to_be_bytes());

        self.buffer.split().freeze()
    }
}

/// Encode a single Arrow RecordBatch row directly to DataRow bytes
///
/// # Arguments
/// * `batch` - The Arrow RecordBatch
/// * `row_idx` - The row index to encode
/// * `format_codes` - Format codes for each column (0=text, 1=binary)
///
/// # Returns
/// A `Bytes` containing the complete DataRow message
#[inline]
pub fn encode_arrow_row(batch: &RecordBatch, row_idx: usize, format_codes: &[i16]) -> Bytes {
    ARROW_ENCODER.with(|encoder| {
        let mut encoder = encoder.borrow_mut();
        encoder.prepare();

        let num_cols = batch.num_columns();

        for col_idx in 0..num_cols {
            let format = get_format_for_column(col_idx, format_codes);
            let column = batch.column(col_idx);

            encode_arrow_value(&mut encoder, column.as_ref(), row_idx, format);
        }

        encoder.take_row(num_cols as i16)
    })
}

/// Encode all rows from an Arrow RecordBatch
///
/// Returns an iterator of DataRow bytes for efficient streaming
pub fn encode_arrow_batch<'a>(
    batch: &'a RecordBatch,
    format_codes: &'a [i16],
) -> impl Iterator<Item = Bytes> + 'a {
    (0..batch.num_rows()).map(move |row_idx| encode_arrow_row(batch, row_idx, format_codes))
}

/// Get the format code for a specific column index
#[inline]
fn get_format_for_column(column_index: usize, format_codes: &[i16]) -> i16 {
    match format_codes.len() {
        0 => 0,
        1 => format_codes[0],
        _ => format_codes.get(column_index).copied().unwrap_or(0),
    }
}

/// Encode a single Arrow value to the buffer
#[inline]
fn encode_arrow_value(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, format: i16) {
    if array.is_null(row_idx) {
        // NULL: length = -1
        encoder.buffer.put_i32(-1);
        return;
    }

    let data_type = array.data_type();

    match data_type {
        // Integer types
        DataType::Int8 => encode_int8(encoder, array, row_idx, format),
        DataType::Int16 => encode_int16(encoder, array, row_idx, format),
        DataType::Int32 => encode_int32(encoder, array, row_idx, format),
        DataType::Int64 => encode_int64(encoder, array, row_idx, format),
        DataType::UInt8 => encode_uint8(encoder, array, row_idx, format),
        DataType::UInt16 => encode_uint16(encoder, array, row_idx, format),
        DataType::UInt32 => encode_uint32(encoder, array, row_idx, format),
        DataType::UInt64 => encode_uint64(encoder, array, row_idx, format),

        // Float types
        DataType::Float32 => encode_float32(encoder, array, row_idx, format),
        DataType::Float64 => encode_float64(encoder, array, row_idx, format),

        // Boolean
        DataType::Boolean => encode_boolean(encoder, array, row_idx, format),

        // String types (ZERO-COPY for text format!)
        DataType::Utf8 => encode_utf8(encoder, array, row_idx),
        DataType::LargeUtf8 => encode_large_utf8(encoder, array, row_idx),
        DataType::Utf8View => encode_utf8_view(encoder, array, row_idx),

        // Date/Time
        DataType::Date32 => encode_date32(encoder, array, row_idx, format),
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
            encode_timestamp_micros(encoder, array, row_idx, format)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
            encode_timestamp_millis(encoder, array, row_idx, format)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
            encode_timestamp_secs(encoder, array, row_idx, format)
        }
        DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
            encode_time64_micros(encoder, array, row_idx, format)
        }

        // Decimal (common for monetary data)
        DataType::Decimal128(_, scale) => encode_decimal128(encoder, array, row_idx, *scale),

        // Binary
        DataType::Binary | DataType::LargeBinary => encode_binary(encoder, array, row_idx, format),

        // Null type
        DataType::Null => {
            encoder.buffer.put_i32(-1);
        }

        // Fallback: use Arrow's display formatter (allocates, but handles all types)
        _ => encode_fallback(encoder, array, row_idx),
    }
}

// ============= Integer Encoders =============

#[inline]
fn encode_int8(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, format: i16) {
    let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
    let value = arr.value(row_idx);

    if format == 1 {
        // Binary: PostgreSQL uses int2 (2 bytes) for int8 values
        encoder.buffer.put_i32(2);
        encoder.buffer.put_i16(value as i16);
    } else {
        // Text: use itoa for fast formatting
        let s = encoder.itoa_buffer.format(value);
        encoder.buffer.put_i32(s.len() as i32);
        encoder.buffer.extend_from_slice(s.as_bytes());
    }
}

#[inline]
fn encode_int16(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, format: i16) {
    let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
    let value = arr.value(row_idx);

    if format == 1 {
        encoder.buffer.put_i32(2);
        encoder.buffer.put_i16(value);
    } else {
        let s = encoder.itoa_buffer.format(value);
        encoder.buffer.put_i32(s.len() as i32);
        encoder.buffer.extend_from_slice(s.as_bytes());
    }
}

#[inline]
fn encode_int32(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, format: i16) {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    let value = arr.value(row_idx);

    if format == 1 {
        encoder.buffer.put_i32(4);
        encoder.buffer.put_i32(value);
    } else {
        let s = encoder.itoa_buffer.format(value);
        encoder.buffer.put_i32(s.len() as i32);
        encoder.buffer.extend_from_slice(s.as_bytes());
    }
}

#[inline]
fn encode_int64(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, format: i16) {
    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
    let value = arr.value(row_idx);

    if format == 1 {
        encoder.buffer.put_i32(8);
        encoder.buffer.put_i64(value);
    } else {
        let s = encoder.itoa_buffer.format(value);
        encoder.buffer.put_i32(s.len() as i32);
        encoder.buffer.extend_from_slice(s.as_bytes());
    }
}

#[inline]
fn encode_uint8(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, format: i16) {
    let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
    let value = arr.value(row_idx);

    if format == 1 {
        encoder.buffer.put_i32(2);
        encoder.buffer.put_i16(value as i16);
    } else {
        let s = encoder.itoa_buffer.format(value);
        encoder.buffer.put_i32(s.len() as i32);
        encoder.buffer.extend_from_slice(s.as_bytes());
    }
}

#[inline]
fn encode_uint16(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, format: i16) {
    let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
    let value = arr.value(row_idx);

    if format == 1 {
        encoder.buffer.put_i32(4);
        encoder.buffer.put_i32(value as i32);
    } else {
        let s = encoder.itoa_buffer.format(value);
        encoder.buffer.put_i32(s.len() as i32);
        encoder.buffer.extend_from_slice(s.as_bytes());
    }
}

#[inline]
fn encode_uint32(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, format: i16) {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    let value = arr.value(row_idx);

    if format == 1 {
        encoder.buffer.put_i32(8);
        encoder.buffer.put_i64(value as i64);
    } else {
        let s = encoder.itoa_buffer.format(value);
        encoder.buffer.put_i32(s.len() as i32);
        encoder.buffer.extend_from_slice(s.as_bytes());
    }
}

#[inline]
fn encode_uint64(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, format: i16) {
    let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
    let value = arr.value(row_idx);

    // PostgreSQL doesn't have unsigned 64-bit, use text representation
    // or encode as numeric (complex). For simplicity, always use text.
    let s = encoder.itoa_buffer.format(value);
    encoder.buffer.put_i32(s.len() as i32);
    encoder.buffer.extend_from_slice(s.as_bytes());
}

// ============= Float Encoders =============

#[inline]
fn encode_float32(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, format: i16) {
    let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
    let value = arr.value(row_idx);

    if format == 1 {
        encoder.buffer.put_i32(4);
        encoder.buffer.put_f32(value);
    } else {
        // ryu for fast float-to-string (no allocation!)
        let s = encoder.ryu_buffer.format(value);
        encoder.buffer.put_i32(s.len() as i32);
        encoder.buffer.extend_from_slice(s.as_bytes());
    }
}

#[inline]
fn encode_float64(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, format: i16) {
    let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
    let value = arr.value(row_idx);

    if format == 1 {
        encoder.buffer.put_i32(8);
        encoder.buffer.put_f64(value);
    } else {
        let s = encoder.ryu_buffer.format(value);
        encoder.buffer.put_i32(s.len() as i32);
        encoder.buffer.extend_from_slice(s.as_bytes());
    }
}

// ============= Boolean Encoder =============

#[inline]
fn encode_boolean(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, format: i16) {
    let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
    let value = arr.value(row_idx);

    if format == 1 {
        encoder.buffer.put_i32(1);
        encoder.buffer.put_u8(if value { 1 } else { 0 });
    } else {
        encoder.buffer.put_i32(1);
        encoder.buffer.put_u8(if value { b't' } else { b'f' });
    }
}

// ============= String Encoders =============

/// Encode a string value to the PG wire protocol buffer.
/// 
/// Ensures valid UTF-8 output for the PostgreSQL wire protocol:
/// - Source data from Parquet/Delta may contain invalid UTF-8 (e.g., Latin-1 encoded
///   patent titles with accented characters like "Système D'antenne")
/// - PostgreSQL wire protocol requires valid UTF-8 in text mode
/// - Invalid bytes are replaced with U+FFFD (Unicode replacement character)
/// - NULL bytes (0x00) are stripped (PG wire protocol uses null-terminated strings)
///
/// Fast path: if bytes are valid UTF-8 and contain no NULLs, zero-copy.
/// Slow path: only allocates when sanitization is needed.
#[inline]
fn encode_string_bytes(encoder: &mut ArrowDataRowEncoder, bytes: &[u8]) {
    // Fast path: check if bytes are clean (valid UTF-8, no NULL bytes)
    let needs_null_filter = bytes.contains(&0);
    let is_valid_utf8 = std::str::from_utf8(bytes).is_ok();
    
    if is_valid_utf8 && !needs_null_filter {
        // Fast path: direct copy (zero allocation)
        encoder.buffer.put_i32(bytes.len() as i32);
        encoder.buffer.extend_from_slice(bytes);
    } else {
        // Slow path: sanitize invalid UTF-8 and/or NULL bytes
        let sanitized = String::from_utf8_lossy(bytes); // replaces invalid bytes with U+FFFD
        let clean: Vec<u8> = sanitized.as_bytes().iter().copied().filter(|&b| b != 0).collect();
        encoder.buffer.put_i32(clean.len() as i32);
        encoder.buffer.extend_from_slice(&clean);
    }
}

#[inline]
fn encode_utf8(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize) {
    let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
    // Access raw bytes to handle potentially invalid UTF-8 from source data
    let start = arr.value_offsets()[row_idx] as usize;
    let end = arr.value_offsets()[row_idx + 1] as usize;
    let bytes = &arr.value_data()[start..end];
    encode_string_bytes(encoder, bytes);
}

#[inline]
fn encode_large_utf8(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize) {
    let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
    let start = arr.value_offsets()[row_idx] as usize;
    let end = arr.value_offsets()[row_idx + 1] as usize;
    let bytes = &arr.value_data()[start..end];
    encode_string_bytes(encoder, bytes);
}

// ============= Date/Time Encoders =============

#[inline]
fn encode_date32(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, format: i16) {
    let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
    let days_since_unix = arr.value(row_idx);

    if format == 1 {
        // Binary: days since PostgreSQL epoch (2000-01-01)
        let pg_days = days_since_unix - PG_EPOCH_DAYS;
        encoder.buffer.put_i32(4);
        encoder.buffer.put_i32(pg_days);
    } else {
        // Text: YYYY-MM-DD format
        let date = chrono::NaiveDate::from_num_days_from_ce_opt(days_since_unix + 719_163)
            .unwrap_or(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
        let s = date.format("%Y-%m-%d").to_string();
        encoder.buffer.put_i32(s.len() as i32);
        encoder.buffer.extend_from_slice(s.as_bytes());
    }
}

#[inline]
fn encode_timestamp_micros(
    encoder: &mut ArrowDataRowEncoder,
    array: &dyn Array,
    row_idx: usize,
    format: i16,
) {
    let arr = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let micros = arr.value(row_idx);

    if format == 1 {
        // Binary: microseconds since PostgreSQL epoch
        let pg_micros = micros - PG_EPOCH_MICROS;
        encoder.buffer.put_i32(8);
        encoder.buffer.put_i64(pg_micros);
    } else {
        // Text: ISO 8601 format
        let dt = chrono::DateTime::from_timestamp_micros(micros)
            .unwrap_or_else(|| chrono::DateTime::UNIX_EPOCH);
        let s = dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        encoder.buffer.put_i32(s.len() as i32);
        encoder.buffer.extend_from_slice(s.as_bytes());
    }
}

#[inline]
fn encode_timestamp_millis(
    encoder: &mut ArrowDataRowEncoder,
    array: &dyn Array,
    row_idx: usize,
    format: i16,
) {
    let arr = array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let millis = arr.value(row_idx);

    if format == 1 {
        // Binary: convert to microseconds, then subtract epoch
        let micros = millis * 1000;
        let pg_micros = micros - PG_EPOCH_MICROS;
        encoder.buffer.put_i32(8);
        encoder.buffer.put_i64(pg_micros);
    } else {
        let dt = chrono::DateTime::from_timestamp_millis(millis)
            .unwrap_or_else(|| chrono::DateTime::UNIX_EPOCH);
        let s = dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        encoder.buffer.put_i32(s.len() as i32);
        encoder.buffer.extend_from_slice(s.as_bytes());
    }
}

#[inline]
fn encode_timestamp_secs(
    encoder: &mut ArrowDataRowEncoder,
    array: &dyn Array,
    row_idx: usize,
    format: i16,
) {
    let arr = array
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .unwrap();
    let secs = arr.value(row_idx);

    if format == 1 {
        // Binary: convert to microseconds, then subtract epoch
        let micros = secs * 1_000_000;
        let pg_micros = micros - PG_EPOCH_MICROS;
        encoder.buffer.put_i32(8);
        encoder.buffer.put_i64(pg_micros);
    } else {
        let dt = chrono::DateTime::from_timestamp(secs, 0)
            .unwrap_or_else(|| chrono::DateTime::UNIX_EPOCH);
        let s = dt.format("%Y-%m-%d %H:%M:%S").to_string();
        encoder.buffer.put_i32(s.len() as i32);
        encoder.buffer.extend_from_slice(s.as_bytes());
    }
}

#[inline]
fn encode_time64_micros(
    encoder: &mut ArrowDataRowEncoder,
    array: &dyn Array,
    row_idx: usize,
    format: i16,
) {
    let arr = array
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap();
    let micros = arr.value(row_idx);

    if format == 1 {
        // Binary: PostgreSQL TIME is 8 bytes microseconds since midnight
        encoder.buffer.put_i32(8);
        encoder.buffer.put_i64(micros);
    } else {
        // Text: HH:MM:SS.ffffff
        let total_secs = micros / 1_000_000;
        let remaining_micros = micros % 1_000_000;
        let hours = total_secs / 3600;
        let minutes = (total_secs % 3600) / 60;
        let seconds = total_secs % 60;
        let s = format!("{:02}:{:02}:{:02}.{:06}", hours, minutes, seconds, remaining_micros);
        encoder.buffer.put_i32(s.len() as i32);
        encoder.buffer.extend_from_slice(s.as_bytes());
    }
}

// ============= Decimal128 Encoder =============

#[inline]
fn encode_decimal128(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, scale: i8) {
    let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
    let value = arr.value(row_idx);

    // Convert to string representation with proper decimal placement
    // This is simpler and more reliable than PostgreSQL's complex NUMERIC binary format
    let abs_value = value.abs();
    let is_negative = value < 0;
    
    let int_str = abs_value.to_string();
    let scale = scale as usize;
    
    let s = if scale == 0 {
        if is_negative {
            format!("-{}", int_str)
        } else {
            int_str
        }
    } else if int_str.len() <= scale {
        // Value is less than 1, need to pad with zeros
        let zeros = "0".repeat(scale - int_str.len());
        if is_negative {
            format!("-0.{}{}", zeros, int_str)
        } else {
            format!("0.{}{}", zeros, int_str)
        }
    } else {
        // Insert decimal point
        let decimal_pos = int_str.len() - scale;
        let (integer_part, decimal_part) = int_str.split_at(decimal_pos);
        if is_negative {
            format!("-{}.{}", integer_part, decimal_part)
        } else {
            format!("{}.{}", integer_part, decimal_part)
        }
    };

    encoder.buffer.put_i32(s.len() as i32);
    encoder.buffer.extend_from_slice(s.as_bytes());
}

// ============= StringView Encoder (DuckDB StringView) =============

#[inline]
fn encode_utf8_view(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize) {
    let arr = array.as_any().downcast_ref::<StringViewArray>().unwrap();
    let value = arr.value(row_idx);
    encode_string_bytes(encoder, value.as_bytes());
}

// ============= Binary Encoder =============

#[inline]
fn encode_binary(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize, format: i16) {
    use arrow_array::BinaryArray;

    let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
    let value = arr.value(row_idx);

    if format == 1 {
        // Binary format: raw bytes
        encoder.buffer.put_i32(value.len() as i32);
        encoder.buffer.extend_from_slice(value);
    } else {
        // Text format: hex escape (\\x prefix)
        let hex_len = 2 + value.len() * 2; // "\\x" + 2 hex chars per byte
        encoder.buffer.put_i32(hex_len as i32);
        encoder.buffer.extend_from_slice(b"\\x");
        for byte in value {
            encoder.buffer.extend_from_slice(
                format!("{:02x}", byte).as_bytes()
            );
        }
    }
}

// ============= Fallback Encoder =============

/// Fallback for types not directly supported
/// Uses Arrow's display formatter (allocates a String)
#[inline]
fn encode_fallback(encoder: &mut ArrowDataRowEncoder, array: &dyn Array, row_idx: usize) {
    use arrow::util::display::ArrayFormatter;

    let s = match ArrayFormatter::try_new(array, &Default::default()) {
        Ok(formatter) => formatter.value(row_idx).to_string(),
        Err(_) => format!("<{:?}>", array.data_type()),
    };

    encode_string_bytes(encoder, s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int32Builder, StringBuilder};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_encode_int32_text() {
        let mut builder = Int32Builder::new();
        builder.append_value(42);
        builder.append_value(-123);
        builder.append_null();
        let array = builder.finish();

        let schema = Schema::new(vec![Field::new("num", DataType::Int32, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        // Text format
        let row0 = encode_arrow_row(&batch, 0, &[0]);
        assert!(row0.len() > 0);

        // Row with NULL
        let row2 = encode_arrow_row(&batch, 2, &[0]);
        assert!(row2.len() > 0);
    }

    #[test]
    fn test_encode_string_zerocopy() {
        let mut builder = StringBuilder::new();
        builder.append_value("hello");
        builder.append_value("world");
        let array = builder.finish();

        let schema = Schema::new(vec![Field::new("text", DataType::Utf8, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        let row0 = encode_arrow_row(&batch, 0, &[0]);
        // Check that "hello" (5 bytes) is encoded correctly
        // DataRow: D (1) + len (4) + field_count (2) + value_len (4) + "hello" (5) = 16 bytes
        assert_eq!(row0.len(), 16);
    }
}

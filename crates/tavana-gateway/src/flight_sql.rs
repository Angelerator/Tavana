//! Arrow Flight SQL Server Implementation (ADBC Compatible)
//!
//! Provides high-performance Arrow-native database connectivity for analytics clients.
//! Implements the full Flight SQL protocol for ADBC driver compatibility.
//!
//! Protocol: gRPC over HTTP/2
//! Port: 443 (default, configurable via FLIGHT_SQL_PORT)
//!
//! Clients can use:
//! - Python: `adbc_driver_flightsql` with `adbc_driver_flightsql.dbapi.connect()`
//! - Go: `github.com/apache/arrow-adbc/go/adbc/driver/flightsql`
//! - Java: `org.apache.arrow.adbc:adbc-driver-flight-sql`
//! - JDBC: `jdbc:arrow-flight-sql://host:443`
//! - pyarrow: `pyarrow.flight.connect('grpc://host:443')`

use arrow_array::{ArrayRef, RecordBatch, StringArray, builder::StringBuilder};
use arrow_flight::{
    Action, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, Ticket,
    encode::FlightDataEncoderBuilder,
    flight_service_server::FlightService,
    sql::{
        ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
        ActionCreatePreparedStatementResult, Any, CommandGetCatalogs, CommandGetDbSchemas,
        CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
        CommandPreparedStatementQuery, CommandStatementQuery, CommandStatementUpdate,
        ProstMessageExt, SqlInfo, TicketStatementQuery,
        metadata::{SqlInfoData, SqlInfoDataBuilder},
        server::{FlightSqlService, PeekableFlightDataStream},
        DoPutPreparedStatementResult,
    },
    error::FlightError,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use base64::{Engine, prelude::BASE64_STANDARD};
use bytes::Bytes;
use dashmap::DashMap;
use futures::{Stream, StreamExt, stream};
use once_cell::sync::Lazy;
use prost::Message;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::worker_client::{WorkerClient, StreamingResult};

/// Build IPC write options with LZ4 compression for Flight SQL output.
///
/// LZ4 reduces wire bytes by 2-5x with <1ms/MB overhead, significantly
/// speeding up Arrow Flight transfers to ADBC clients.
/// Configurable via TAVANA_FLIGHT_COMPRESSION env var (lz4/zstd/none).
fn flight_ipc_write_options() -> IpcWriteOptions {
    let compression = match std::env::var("TAVANA_FLIGHT_COMPRESSION")
        .unwrap_or_else(|_| "lz4".to_string())
        .to_lowercase()
        .as_str()
    {
        "none" | "off" | "false" => None,
        "zstd" => Some(arrow_ipc::CompressionType::ZSTD),
        _ => Some(arrow_ipc::CompressionType::LZ4_FRAME),
    };
    IpcWriteOptions::try_new(8, false, arrow_ipc::MetadataVersion::V5)
        .and_then(|opts| opts.try_with_compression(compression))
        .unwrap_or_default()
}

/// SQL Info metadata for the Tavana server
static TAVANA_SQL_INFO: Lazy<SqlInfoData> = Lazy::new(|| {
    let mut builder = SqlInfoDataBuilder::new();
    
    // Server information
    builder.append(SqlInfo::FlightSqlServerName, "Tavana Gateway");
    builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    builder.append(SqlInfo::FlightSqlServerReadOnly, false);
    builder.append(SqlInfo::FlightSqlServerSql, true);
    builder.append(SqlInfo::FlightSqlServerSubstrait, false);
    builder.append(SqlInfo::FlightSqlServerTransaction, false);
    builder.append(SqlInfo::FlightSqlServerCancel, false);
    builder.append(SqlInfo::FlightSqlServerStatementTimeout, 0i64);
    builder.append(SqlInfo::FlightSqlServerTransactionTimeout, 0i64);
    
    // SQL syntax information
    builder.append(SqlInfo::SqlIdentifierCase, 1i64); // Case insensitive
    builder.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
    builder.append(SqlInfo::SqlQuotedIdentifierCase, 3i64); // Sensitive if quoted
    
    // DuckDB-specific keywords (subset)
    builder.append(SqlInfo::SqlKeywords, vec![
        "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "LIMIT", "OFFSET",
        "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "NATURAL",
        "UNION", "INTERSECT", "EXCEPT", "WITH", "AS", "DISTINCT",
        "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE",
        "VIEW", "INDEX", "SCHEMA", "DATABASE", "IF", "EXISTS", "NOT",
        "NULL", "AND", "OR", "IN", "BETWEEN", "LIKE", "HAVING", "CASE",
        "WHEN", "THEN", "ELSE", "END", "CAST", "COALESCE", "NULLIF",
    ].into_iter().map(String::from).collect::<Vec<_>>());
    
    builder.append(SqlInfo::SqlNumericFunctions, vec![
        "ABS", "CEIL", "FLOOR", "ROUND", "SQRT", "POWER", "LOG", "EXP",
        "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "RANDOM",
    ].into_iter().map(String::from).collect::<Vec<_>>());
    
    builder.append(SqlInfo::SqlStringFunctions, vec![
        "CONCAT", "LENGTH", "LOWER", "UPPER", "TRIM", "LTRIM", "RTRIM",
        "SUBSTRING", "REPLACE", "REVERSE", "SPLIT_PART", "LEFT", "RIGHT",
    ].into_iter().map(String::from).collect::<Vec<_>>());
    
    builder.append(SqlInfo::SqlSupportsConvert, false);
    builder.append(SqlInfo::SqlSupportsColumnAliasing, true);
    builder.append(SqlInfo::SqlNullOrdering, 0i64); // Nulls first
    builder.append(SqlInfo::SqlSupportedGroupBy, 3i64); // Unrelated
    builder.append(SqlInfo::SqlSupportsLikeEscapeClause, true);
    builder.append(SqlInfo::SqlSupportsNonNullableColumns, true);
    builder.append(SqlInfo::SqlSupportsIntegrityEnhancementFacility, false);
    builder.append(SqlInfo::SqlCorrelatedSubqueriesSupported, true);
    builder.append(SqlInfo::SqlSupportedPositionedCommands, 0i64);
    builder.append(SqlInfo::SqlSelectForUpdateSupported, false);
    builder.append(SqlInfo::SqlStoredProceduresSupported, false);
    builder.append(SqlInfo::SqlMaxBinaryLiteralLength, 0i64); // No limit
    builder.append(SqlInfo::SqlMaxCharLiteralLength, 0i64); // No limit
    builder.append(SqlInfo::SqlMaxColumnNameLength, 0i64); // No limit
    builder.append(SqlInfo::SqlMaxColumnsInGroupBy, 0i64); // No limit
    builder.append(SqlInfo::SqlMaxColumnsInIndex, 0i64); // No limit
    builder.append(SqlInfo::SqlMaxColumnsInOrderBy, 0i64); // No limit
    builder.append(SqlInfo::SqlMaxColumnsInSelect, 0i64); // No limit
    builder.append(SqlInfo::SqlMaxColumnsInTable, 0i64); // No limit
    builder.append(SqlInfo::SqlMaxConnections, 0i64); // No limit
    builder.append(SqlInfo::SqlMaxCursorNameLength, 0i64); // No limit
    builder.append(SqlInfo::SqlMaxIndexLength, 0i64); // No limit
    // Note: SqlDbSchemaSupport and SqlCatalogSupport are not available in all versions
    builder.append(SqlInfo::SqlBatchUpdatesSupported, false);
    builder.append(SqlInfo::SqlSavepointsSupported, false);
    builder.append(SqlInfo::SqlLocatorsUpdateCopy, false);
    
    builder.build().expect("Failed to build SQL info")
});

/// Information about a prepared statement
#[derive(Clone)]
struct PreparedStatementInfo {
    sql: String,
    schema: SchemaRef,
    created_at: Instant,
}

/// Information about an active query (for statement execution)
struct StatementInfo {
    sql: String,
    #[allow(dead_code)]
    created_at: Instant,
}

/// Tavana Flight SQL Service
/// 
/// Implements the full Arrow Flight SQL protocol for ADBC client compatibility.
/// This enables high-performance, Arrow-native database access from any ADBC client.
#[derive(Clone)]
pub struct TavanaFlightSqlService {
    worker_client: Arc<WorkerClient>,
    /// Active statements (query handle -> statement info)
    statements: Arc<DashMap<String, StatementInfo>>,
    /// Prepared statements (handle -> prepared statement info)
    prepared_statements: Arc<DashMap<String, PreparedStatementInfo>>,
}

impl TavanaFlightSqlService {
    /// Create a new Flight SQL service
    pub fn new(worker_addr: String) -> Self {
        info!("Creating ADBC-compatible Arrow Flight SQL service");
        info!("  Worker address: {}", worker_addr);
        info!("  Supported clients: ADBC (Python/Go/Java/R), JDBC, pyarrow.flight");
        
        Self {
            worker_client: Arc::new(WorkerClient::new(worker_addr)),
            statements: Arc::new(DashMap::new()),
            prepared_statements: Arc::new(DashMap::new()),
        }
    }

    /// Generate a unique handle
    fn generate_handle() -> String {
        Uuid::new_v4().to_string()
    }

    /// Extract user from request metadata
    fn extract_user<T>(&self, request: &Request<T>) -> String {
        request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|auth| {
                if auth.starts_with("Bearer ") {
                    Some(auth[7..].to_string())
                } else if auth.starts_with("Basic ") {
                    // Decode base64 and extract username
                    BASE64_STANDARD.decode(&auth[6..]).ok()
                        .and_then(|bytes| String::from_utf8(bytes).ok())
                        .and_then(|s| s.split(':').next().map(String::from))
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "anonymous".to_string())
    }

    /// Execute a query and return RecordBatches
    async fn execute_query(&self, sql: &str, user: &str) -> Result<Vec<RecordBatch>, Status> {
        let result = self
            .worker_client
            .execute_query(sql, user)
            .await
            .map_err(|e| Status::internal(format!("Query failed: {}", e)))?;

        // Build Arrow schema from columns
        let fields: Vec<Field> = result.columns.iter()
            .map(|c| Field::new(&c.name, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Convert rows to Arrow RecordBatch
        let arrays: Vec<ArrayRef> = (0..result.columns.len())
            .map(|col_idx| {
                let values: Vec<Option<&str>> = result.rows.iter()
                    .map(|row| {
                        if col_idx < row.len() {
                            let v = &row[col_idx];
                            if v == "NULL" { None } else { Some(v.as_str()) }
                        } else {
                            None
                        }
                    })
                    .collect();
                Arc::new(StringArray::from(values)) as ArrayRef
            })
            .collect();

        if arrays.is_empty() {
            // Empty result set
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }

        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| Status::internal(format!("Failed to create RecordBatch: {}", e)))?;

        Ok(vec![batch])
    }

    /// Execute a query and return native Arrow RecordBatches (zero conversion)
    /// 
    /// Uses streaming to collect Arrow IPC batches directly from the worker,
    /// preserving original DuckDB types instead of flattening to Utf8.
    async fn execute_query_arrow(&self, sql: &str, user: &str) -> Result<(Vec<RecordBatch>, Vec<(String, String)>), Status> {
        use crate::worker_client::StreamingBatch;
        
        let mut stream = self
            .worker_client
            .execute_query_streaming(sql, user)
            .await
            .map_err(|e| Status::internal(format!("Query failed: {}", e)))?;

        let mut batches = Vec::new();
        let mut columns_meta = Vec::new();

        while let Some(result) = stream.next().await {
            match result.map_err(|e| Status::internal(format!("Stream error: {}", e)))? {
                StreamingBatch::ArrowBatches(arrow_batches) => {
                    batches.extend(arrow_batches);
                }
                StreamingBatch::Metadata { columns, column_types } => {
                    columns_meta = columns.into_iter().zip(column_types.into_iter()).collect();
                }
                StreamingBatch::Rows(rows) => {
                    // Legacy path: convert string rows to Arrow (same as old execute_query)
                    if !rows.is_empty() && !columns_meta.is_empty() {
                        let fields: Vec<Field> = columns_meta.iter()
                            .map(|(name, _)| Field::new(name, DataType::Utf8, true))
                            .collect();
                        let schema = Arc::new(Schema::new(fields));
                        let arrays: Vec<ArrayRef> = (0..columns_meta.len())
                            .map(|col_idx| {
                                let values: Vec<Option<&str>> = rows.iter()
                                    .map(|row| {
                                        if col_idx < row.len() {
                                            let v = &row[col_idx];
                                            if v == "NULL" { None } else { Some(v.as_str()) }
                                        } else { None }
                                    }).collect();
                                Arc::new(StringArray::from(values)) as ArrayRef
                            }).collect();
                        if let Ok(batch) = RecordBatch::try_new(schema, arrays) {
                            batches.push(batch);
                        }
                    }
                }
                StreamingBatch::FlightData { .. } => { /* Handled in Flight SQL streaming path */ }
                StreamingBatch::Error(msg) => {
                    return Err(Status::internal(msg));
                }
            }
        }

        Ok((batches, columns_meta))
    }

    /// Create a Flight stream from record batches (with LZ4 compression)
    fn batches_to_stream(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> {
        let batch_stream = stream::iter(batches.into_iter().map(Ok));
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_options(flight_ipc_write_options())
            .build(batch_stream)
            .map(|result| result.map_err(|e| Status::internal(format!("Encoding error: {}", e))));
        Box::pin(flight_data_stream)
    }

    /// Build a Flight SQL response stream from a worker stream.
    ///
    /// **Zero-copy passthrough**: If the worker provides pre-encoded FlightData
    /// (ipc_header + ipc_body), forward those raw bytes directly — no deserialize,
    /// no re-serialize. This eliminates 2 serialization steps per batch.
    ///
    /// IMPORTANT: The schema for the FlightData header must come from the actual
    /// ArrowBatch (not from Metadata type mapping) to ensure the schema matches
    /// the batch data types exactly. `map_duckdb_type_to_arrow` is lossy (e.g.,
    /// Timestamp → Utf8) which would cause decode errors on the client.
    ///
    /// **Fallback**: If no passthrough data is available, uses FlightDataEncoderBuilder
    /// to re-serialize RecordBatches (current behavior, still works for cursors etc.).
    fn build_flight_stream(
        batch_schema: SchemaRef, // must come from actual ArrowBatch, not Metadata
        worker_stream: StreamingResult,
        first_flight_data: std::collections::VecDeque<FlightData>,
        first_batches: std::collections::VecDeque<RecordBatch>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>, Status> {
        use crate::worker_client::StreamingBatch;

        // Zero-copy passthrough is available when the worker sends pre-encoded
        // ipc_header+ipc_body. Currently gated behind an env var because the
        // DuckDB-bundled IpcDataGenerator may produce bytes that differ from what
        // some ADBC clients expect. Enable with TAVANA_FLIGHT_ZERO_COPY=true.
        let zero_copy_enabled = std::env::var("TAVANA_FLIGHT_ZERO_COPY")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        if zero_copy_enabled && !first_flight_data.is_empty() {
            // ── Zero-copy passthrough path ──
            // Forward raw ipc_header+ipc_body from worker as FlightData.
            // Only the schema message is constructed here; batch data is byte-forwarded.
            let arrow_flight::IpcMessage(schema_bytes) =
                arrow_flight::SchemaAsIpc::new(&batch_schema, &IpcWriteOptions::default())
                    .try_into()
                    .map_err(|e: arrow_schema::ArrowError| Status::internal(format!("Schema encode error: {e}")))?;

            let schema_fd = FlightData {
                data_header: schema_bytes,
                ..Default::default()
            };

            let flight_stream = futures::stream::try_unfold(
                (worker_stream, Some(schema_fd), first_flight_data),
                |(mut ws, pending_schema, mut pending)| async move {
                    // 1. Emit schema message first
                    if let Some(sfd) = pending_schema {
                        return Ok(Some((sfd, (ws, None, pending))));
                    }
                    // 2. Drain buffered FlightData from the first batch
                    if let Some(fd) = pending.pop_front() {
                        return Ok(Some((fd, (ws, None, pending))));
                    }
                    // 3. Stream remaining FlightData from worker (skip ArrowBatches)
                    loop {
                        match ws.next().await {
                            Some(Ok(StreamingBatch::FlightData { ipc_header, ipc_body })) => {
                                let fd = FlightData {
                                    data_header: ipc_header,
                                    data_body: ipc_body,
                                    ..Default::default()
                                };
                                return Ok(Some((fd, (ws, None, pending))));
                            }
                            // ArrowBatches are redundant when we have FlightData passthrough
                            Some(Ok(StreamingBatch::ArrowBatches(_))) => continue,
                            Some(Ok(StreamingBatch::Metadata { .. })) | Some(Ok(StreamingBatch::Rows(_))) => continue,
                            Some(Ok(StreamingBatch::Error(msg))) => return Err(Status::internal(msg)),
                            Some(Err(e)) => return Err(Status::internal(e.to_string())),
                            None => return Ok(None),
                        }
                    }
                },
            );

            Ok(Box::pin(flight_stream))
        } else {
            // ── Fallback: FlightDataEncoderBuilder (re-serialize RecordBatches) ──
            let batch_stream = futures::stream::try_unfold(
                (worker_stream, first_batches),
                |(mut ws, mut pending)| async move {
                    if let Some(batch) = pending.pop_front() {
                        return Ok(Some((batch, (ws, pending))));
                    }
                    loop {
                        match ws.next().await {
                            Some(Ok(StreamingBatch::ArrowBatches(batches))) => {
                                let mut iter = batches.into_iter().collect::<std::collections::VecDeque<_>>();
                                if let Some(first) = iter.pop_front() {
                                    return Ok(Some((first, (ws, iter))));
                                }
                            }
                            Some(Ok(StreamingBatch::FlightData { .. })) => continue,
                            Some(Ok(StreamingBatch::Metadata { .. })) | Some(Ok(StreamingBatch::Rows(_))) => continue,
                            Some(Ok(StreamingBatch::Error(msg))) => return Err(FlightError::ExternalError(msg.into())),
                            Some(Err(e)) => return Err(FlightError::ExternalError(e.into())),
                            None => return Ok(None),
                        }
                    }
                },
            );

            let flight_data_stream = FlightDataEncoderBuilder::new()
                .with_schema(batch_schema)
                .with_options(flight_ipc_write_options())
                .build(batch_stream)
                .map(|r| r.map_err(|e| Status::internal(format!("Encoding error: {e}"))));

            Ok(Box::pin(flight_data_stream))
        }
    }

    /// Get the schema of a query by executing it with LIMIT 0
    /// 
    /// This is the recommended approach per the Arrow Flight SQL specification
    /// to determine the result schema without fetching actual data.
    async fn get_query_schema(&self, sql: &str) -> Result<SchemaRef, Status> {
        // Wrap the query with LIMIT 0 to get schema without data
        // Use a subquery to handle all SQL types correctly
        let schema_query = format!("SELECT * FROM ({}) AS _schema_query LIMIT 0", sql.trim_end_matches(';'));
        
        debug!("Determining schema with query: {}", &schema_query[..schema_query.len().min(100)]);
        
        let result = self
            .worker_client
            .execute_query(&schema_query, "schema-detection")
            .await
            .map_err(|e| Status::internal(format!("Schema detection failed: {}", e)))?;

        // Build Arrow schema from columns
        let fields: Vec<Field> = result.columns.iter()
            .map(|c| Field::new(&c.name, DataType::Utf8, true))
            .collect();
        
        Ok(Arc::new(Schema::new(fields)))
    }
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightSqlService for TavanaFlightSqlService {
    type FlightService = TavanaFlightSqlService;

    /// Handle authentication handshake
    /// 
    /// Supports Basic auth (username:password) and Bearer tokens.
    /// Returns a Bearer token for subsequent requests.
    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>, Status> {
        let mut stream = request.into_inner();
        
        // Read handshake request
        let handshake = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("No handshake request received"))??;
        
        let auth_payload = String::from_utf8_lossy(&handshake.payload);
        debug!("Flight SQL handshake received: {} bytes", handshake.payload.len());
        
        // For now, accept all connections and return a token
        // In production, integrate with Separ or other auth providers
        let token = Self::generate_handle();
        
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: token.clone().into_bytes().into(),
        };
        
        let output = stream::once(async { Ok(response) });
        let mut resp = Response::new(Box::pin(output) as Pin<Box<dyn Stream<Item = _> + Send>>);
        
        // Set authorization header for client to use in subsequent requests
        if let Ok(value) = format!("Bearer {}", token).parse() {
            resp.metadata_mut().insert("authorization", value);
        }
        
        info!("Flight SQL handshake completed, issued token");
        Ok(resp)
    }

    /// Get flight info for a SQL statement query
    #[instrument(skip(self, request))]
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = query.query.clone();
        info!("Flight SQL get_flight_info_statement: {}", &sql[..sql.len().min(100)]);

        // Generate handle and store query for later execution
        let handle = Self::generate_handle();
        self.statements.insert(
            handle.clone(),
            StatementInfo {
                sql: sql.clone(),
                created_at: Instant::now(),
            },
        );

        // Create ticket using TicketStatementQuery
        let ticket_query = TicketStatementQuery {
            statement_handle: handle.into_bytes().into(),
        };
        let ticket = Ticket {
            ticket: ticket_query.as_any().encode_to_vec().into(),
        };
        
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let descriptor = request.into_inner();

        let flight_info = FlightInfo::new()
            .with_descriptor(descriptor)
            .with_endpoint(endpoint);

        Ok(Response::new(flight_info))
    }

    /// Execute statement and stream results directly from worker (true streaming).
    ///
    /// **Zero-copy passthrough**: Forwards raw ipc_header+ipc_body from the worker
    /// directly as FlightData — no deserialize, no re-serialize. Eliminates 2
    /// serialization steps per batch compared to the old FlightDataEncoder path.
    #[instrument(skip(self, request))]
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let handle = String::from_utf8_lossy(&ticket.statement_handle).to_string();
        let user = self.extract_user(&request);
        
        let sql = self.statements
            .get(&handle)
            .map(|s| s.sql.clone())
            .ok_or_else(|| Status::not_found(format!("Statement {} not found", handle)))?;

        info!("Flight SQL do_get_statement: streaming query for handle {}", handle);
        self.statements.remove(&handle);

        let mut worker_stream = self.worker_client
            .execute_query_streaming(&sql, &user)
            .await
            .map_err(|e| Status::internal(format!("Query failed: {}", e)))?;

        // Phase 1: Collect schema + first FlightData/ArrowBatches.
        // Schema MUST come from ArrowBatch (not Metadata) to match actual batch data types.
        let mut batch_schema: Option<SchemaRef> = None;
        let mut first_flight_data: std::collections::VecDeque<FlightData> = std::collections::VecDeque::new();
        let mut first_batches: std::collections::VecDeque<RecordBatch> = std::collections::VecDeque::new();

        while let Some(result) = worker_stream.next().await {
            use crate::worker_client::StreamingBatch;
            match result.map_err(|e| Status::internal(e.to_string()))? {
                StreamingBatch::Metadata { .. } => {
                    // Skip Metadata for schema — its type mapping is lossy.
                    // We'll get the exact schema from the first ArrowBatch.
                }
                StreamingBatch::FlightData { ipc_header, ipc_body } => {
                    first_flight_data.push_back(FlightData {
                        data_header: ipc_header,
                        data_body: ipc_body,
                        ..Default::default()
                    });
                }
                StreamingBatch::ArrowBatches(batches) => {
                    if batch_schema.is_none() && !batches.is_empty() {
                        batch_schema = Some(batches[0].schema());
                    }
                    first_batches.extend(batches);
                    break;
                }
                StreamingBatch::Error(msg) => return Err(Status::internal(msg)),
                _ => {}
            }
        }

        let schema = batch_schema.unwrap_or_else(|| Arc::new(Schema::empty()));

        // Phase 2: Build stream (zero-copy passthrough or FlightDataEncoder fallback)
        let stream = Self::build_flight_stream(schema, worker_stream, first_flight_data, first_batches)?;
        Ok(Response::new(stream))
    }

    /// Handle fallback for unrecognized ticket types
    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        warn!("Flight SQL do_get_fallback called with type_url: {}", message.type_url);
        
        // Try to decode as a simple query handle (backwards compatibility)
        let handle = String::from_utf8_lossy(&request.into_inner().ticket).to_string();
        
        if let Some(statement) = self.statements.get(&handle) {
            let sql = statement.sql.clone();
            drop(statement);
            
            let (batches, _) = self.execute_query_arrow(&sql, "fallback-user").await?;
            let schema = if batches.is_empty() {
                Arc::new(Schema::empty())
            } else {
                batches[0].schema()
            };
            
            self.statements.remove(&handle);
            return Ok(Response::new(Self::batches_to_stream(schema, batches)));
        }
        
        Err(Status::invalid_argument(format!(
            "Unsupported ticket type: {}",
            message.type_url
        )))
    }

    /// Get SQL server metadata
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("Flight SQL get_flight_info_sql_info");
        
        let descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(query.into_builder(&TAVANA_SQL_INFO).schema().as_ref())
            .map_err(|e| Status::internal(format!("Unable to encode schema: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);

        Ok(Response::new(flight_info))
    }

    /// Get SQL server metadata data
    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("Flight SQL do_get_sql_info");
        
        let builder = query.into_builder(&TAVANA_SQL_INFO);
        let schema = builder.schema();
        let batch = builder.build();
        
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::once(async { batch }))
            .map(|r| r.map_err(|e: FlightError| Status::internal(e.to_string())));
        
        Ok(Response::new(Box::pin(stream)))
    }

    /// Get catalogs info
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("Flight SQL get_flight_info_catalogs");
        
        let descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| Status::internal(format!("Unable to encode schema: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);

        Ok(Response::new(flight_info))
    }

    /// Get catalogs data - DuckDB has a single "main" catalog
    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("Flight SQL do_get_catalogs");
        
        let mut builder = query.into_builder();
        builder.append("memory"); // DuckDB in-memory database
        
        let schema = builder.schema();
        let batch = builder.build();
        
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::once(async { batch }))
            .map(|r| r.map_err(|e: FlightError| Status::internal(e.to_string())));
        
        Ok(Response::new(Box::pin(stream)))
    }

    /// Get schemas info
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("Flight SQL get_flight_info_schemas");
        
        let descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| Status::internal(format!("Unable to encode schema: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);

        Ok(Response::new(flight_info))
    }

    /// Get schemas data - DuckDB has "main" and "pg_catalog" schemas
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("Flight SQL do_get_schemas");
        
        let mut builder = query.into_builder();
        builder.append("memory", "main");
        builder.append("memory", "pg_catalog");
        builder.append("memory", "information_schema");
        
        let schema = builder.schema();
        let batch = builder.build();
        
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::once(async { batch }))
            .map(|r| r.map_err(|e: FlightError| Status::internal(e.to_string())));
        
        Ok(Response::new(Box::pin(stream)))
    }

    /// Get tables info
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("Flight SQL get_flight_info_tables");
        
        let descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| Status::internal(format!("Unable to encode schema: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);

        Ok(Response::new(flight_info))
    }

    /// Get tables data
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("Flight SQL do_get_tables");
        
        // Return empty table list for now - tables are accessed via delta_scan()
        let builder = query.into_builder();
        let schema = builder.schema();
        let batch = builder.build();
        
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::once(async { batch }))
            .map(|r| r.map_err(|e: FlightError| Status::internal(e.to_string())));
        
        Ok(Response::new(Box::pin(stream)))
    }

    /// Get table types
    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("Flight SQL get_flight_info_table_types");
        
        let descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        // Table types schema
        let schema = Schema::new(vec![
            Field::new("table_type", DataType::Utf8, false),
        ]);

        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Unable to encode schema: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);

        Ok(Response::new(flight_info))
    }

    /// Get table types data
    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("Flight SQL do_get_table_types");
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_type", DataType::Utf8, false),
        ]));
        
        let mut builder = StringBuilder::new();
        builder.append_value("TABLE");
        builder.append_value("VIEW");
        
        let arrays: Vec<ArrayRef> = vec![Arc::new(builder.finish())];
        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| Status::internal(format!("Failed to create batch: {}", e)))?;
        
        Ok(Response::new(Self::batches_to_stream(schema, vec![batch])))
    }

    /// Create prepared statement
    /// 
    /// Per the Arrow Flight SQL spec, the dataset_schema is optional but if provided,
    /// should represent the schema of the result set. We execute the query with LIMIT 0
    /// to determine the actual schema without fetching data.
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let sql = query.query.clone();
        info!("Flight SQL create_prepared_statement: {}", &sql[..sql.len().min(100)]);

        let handle = Self::generate_handle();
        
        // Determine the actual schema by executing the query with LIMIT 0
        // This follows the Flight SQL spec which states the schema should be accurate
        // See: https://arrow.apache.org/docs/format/FlightSql.html
        let schema = self.get_query_schema(&sql).await.unwrap_or_else(|e| {
            warn!("Failed to determine schema for prepared statement, using empty schema: {}", e);
            Arc::new(Schema::empty())
        });
        
        self.prepared_statements.insert(
            handle.clone(),
            PreparedStatementInfo {
                sql,
                schema: schema.clone(),
                created_at: Instant::now(),
            },
        );

        // Serialize schema for the response (empty if we couldn't determine it)
        let schema_bytes = if schema.fields().is_empty() {
            Bytes::new()
        } else {
            let message = arrow_flight::SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
                .try_into()
                .map_err(|e: arrow_schema::ArrowError| Status::internal(format!("Unable to serialize schema: {}", e)))?;
            let arrow_flight::IpcMessage(bytes) = message;
            bytes
        };

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into_bytes().into(),
            dataset_schema: schema_bytes,
            parameter_schema: Bytes::new(), // No parameters for now
        })
    }

    /// Close prepared statement
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        let handle = String::from_utf8_lossy(&query.prepared_statement_handle).to_string();
        debug!("Flight SQL close_prepared_statement: {}", handle);
        
        self.prepared_statements.remove(&handle);
        Ok(())
    }

    /// Get flight info for prepared statement query
    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let handle = String::from_utf8_lossy(&cmd.prepared_statement_handle).to_string();
        debug!("Flight SQL get_flight_info_prepared_statement: {}", handle);

        let ps_info = self.prepared_statements
            .get(&handle)
            .ok_or_else(|| Status::not_found(format!("Prepared statement {} not found", handle)))?;

        let ticket = Ticket::new(cmd.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let descriptor = request.into_inner();

        let flight_info = FlightInfo::new()
            .try_with_schema(&ps_info.schema)
            .map_err(|e| Status::internal(format!("Unable to encode schema: {}", e)))?
            .with_descriptor(descriptor)
            .with_endpoint(endpoint);

        Ok(Response::new(flight_info))
    }

    /// Execute prepared statement and stream results directly from worker.
    ///
    /// Uses zero-copy passthrough when available (same as do_get_statement).
    async fn do_get_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let handle = String::from_utf8_lossy(&cmd.prepared_statement_handle).to_string();
        let user = self.extract_user(&request);
        
        let (sql, stored_schema) = self.prepared_statements
            .get(&handle)
            .map(|ps| (ps.sql.clone(), ps.schema.clone()))
            .ok_or_else(|| Status::not_found(format!("Prepared statement {} not found", handle)))?;

        info!("Flight SQL do_get_prepared_statement: streaming {}", handle);

        let mut worker_stream = self.worker_client
            .execute_query_streaming(&sql, &user)
            .await
            .map_err(|e| Status::internal(format!("Query failed: {}", e)))?;

        // Phase 1: Collect schema + first FlightData/ArrowBatches.
        // Schema MUST come from ArrowBatch to match actual batch data types.
        let mut batch_schema: Option<SchemaRef> = None;
        let mut first_flight_data: std::collections::VecDeque<FlightData> = std::collections::VecDeque::new();
        let mut first_batches: std::collections::VecDeque<RecordBatch> = std::collections::VecDeque::new();

        while let Some(result) = worker_stream.next().await {
            use crate::worker_client::StreamingBatch;
            match result.map_err(|e| Status::internal(e.to_string()))? {
                StreamingBatch::Metadata { .. } => {}
                StreamingBatch::FlightData { ipc_header, ipc_body } => {
                    first_flight_data.push_back(FlightData {
                        data_header: ipc_header,
                        data_body: ipc_body,
                        ..Default::default()
                    });
                }
                StreamingBatch::ArrowBatches(batches) => {
                    if batch_schema.is_none() && !batches.is_empty() {
                        batch_schema = Some(batches[0].schema());
                    }
                    first_batches.extend(batches);
                    break;
                }
                StreamingBatch::Error(msg) => return Err(Status::internal(msg)),
                _ => {}
            }
        }

        // For prepared statements, MUST use stored_schema to match what GetFlightInfo
        // advertised. The stored schema comes from get_query_schema() during
        // CreatePreparedStatement. Using batch_schema here would cause a mismatch
        // that ADBC clients reject.
        let schema = if !stored_schema.fields().is_empty() {
            stored_schema
        } else {
            batch_schema.unwrap_or_else(|| Arc::new(Schema::empty()))
        };

        // Phase 2: Build stream (zero-copy passthrough or FlightDataEncoder fallback)
        let stream = Self::build_flight_stream(schema, worker_stream, first_flight_data, first_batches)?;
        Ok(Response::new(stream))
    }

    /// Execute update statement
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let sql = ticket.query.clone();
        let user = self.extract_user(&request);
        
        info!("Flight SQL do_put_statement_update: {}", &sql[..sql.len().min(100)]);

        // Execute the update
        let _ = self.execute_query(&sql, &user).await?;
        
        // Return affected rows (we don't track this precisely, so return 1)
        Ok(1)
    }

    /// Handle prepared statement update binding
    async fn do_put_prepared_statement_query(
        &self,
        _cmd: CommandPreparedStatementQuery,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        // Parameter binding - return empty for now (no parameter updates)
        Ok(DoPutPreparedStatementResult {
            prepared_statement_handle: None,
        })
    }

    /// Register SQL info (called during initialization)
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {
        // SQL info is statically defined in TAVANA_SQL_INFO
    }
}

/// Map DuckDB type name strings (from Arrow schema debug format) to Arrow DataType.
/// This preserves native types instead of flattening everything to Utf8.
fn map_duckdb_type_to_arrow(type_name: &str) -> DataType {
    match type_name {
        "Boolean" => DataType::Boolean,
        "Int8" => DataType::Int8,
        "Int16" => DataType::Int16,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "UInt8" => DataType::UInt8,
        "UInt16" => DataType::UInt16,
        "UInt32" => DataType::UInt32,
        "UInt64" => DataType::UInt64,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "Date32" => DataType::Date32,
        "Date64" => DataType::Date64,
        s if s.starts_with("Timestamp") => {
            // Parse "Timestamp(Microsecond, None)" or "Timestamp(Microsecond, Some(\"UTC\"))"
            DataType::Utf8 // Fallback to Utf8 for complex timestamp formats
        }
        "Utf8" | "LargeUtf8" => DataType::Utf8,
        "Binary" | "LargeBinary" => DataType::Binary,
        _ => DataType::Utf8, // Default fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_info_build() {
        // Verify SQL info builds without errors
        let _ = &*TAVANA_SQL_INFO;
    }

    #[test]
    fn test_generate_handle() {
        let h1 = TavanaFlightSqlService::generate_handle();
        let h2 = TavanaFlightSqlService::generate_handle();
        assert_ne!(h1, h2);
        assert!(!h1.is_empty());
    }
}

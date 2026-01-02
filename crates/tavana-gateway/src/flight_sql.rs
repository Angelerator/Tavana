//! Arrow Flight Server Implementation
//!
//! Provides high-performance binary data transfer for analytics clients.
//! Uses the base Flight protocol (simpler than Flight SQL) for compatibility.
//!
//! Protocol: gRPC over HTTP/2
//! Port: 8815 (configurable)
//!
//! Clients can use:
//! - Python: pyarrow.flight.connect('grpc://host:8815')
//! - Go/Rust: arrow-flight client libraries

use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    SchemaResult, Ticket, encode::FlightDataEncoderBuilder,
};
use arrow_schema::{DataType, Field, Schema};
use arrow_array::{StringArray, RecordBatch};
use futures::{Stream, StreamExt, stream};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, instrument};
use uuid::Uuid;

use crate::worker_client::WorkerClient;

/// Tavana Flight Service (base Flight protocol)
/// 
/// Implements Arrow Flight for high-performance analytics.
/// Clients connect via gRPC and receive data in Arrow IPC format.
pub struct TavanaFlightSqlService {
    worker_client: Arc<WorkerClient>,
    /// Active statements
    statements: Arc<RwLock<std::collections::HashMap<String, StatementInfo>>>,
}

/// Information about an active statement/query
struct StatementInfo {
    sql: String,
    #[allow(dead_code)]
    created_at: std::time::Instant,
}

impl TavanaFlightSqlService {
    /// Create a new Flight service
    pub fn new(worker_addr: String) -> Self {
        info!("Creating Arrow Flight service connecting to worker: {}", worker_addr);
        Self {
            worker_client: Arc::new(WorkerClient::new(worker_addr)),
            statements: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Generate a unique handle
    fn generate_handle() -> String {
        Uuid::new_v4().to_string()
    }
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for TavanaFlightSqlService {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type ListActionsStream = BoxedFlightStream<ActionType>;

    /// Handle authentication handshake
    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let mut stream = request.into_inner();
        
        // Read handshake request
        let handshake = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("No handshake request received"))??;
        
        let auth_payload = String::from_utf8_lossy(&handshake.payload);
        debug!("Flight handshake: {}", auth_payload);
        
        // Accept all connections (production would validate credentials)
        let token = Self::generate_handle();
        
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: token.into_bytes().into(),
        };
        
        let stream = stream::once(async { Ok(response) });
        Ok(Response::new(Box::pin(stream)))
    }

    /// List available flights (queries)
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        // Return empty list - Tavana is query-based, not table-based
        let stream = stream::empty();
        Ok(Response::new(Box::pin(stream)))
    }

    /// Get FlightInfo for a query
    /// 
    /// Flight descriptor contains the SQL query as path[0]
    #[instrument(skip(self, request))]
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        
        // SQL query is in the path
        let sql = if !descriptor.path.is_empty() {
            descriptor.path[0].clone()
        } else if !descriptor.cmd.is_empty() {
            String::from_utf8_lossy(&descriptor.cmd).to_string()
        } else {
            return Err(Status::invalid_argument("No query provided"));
        };
        
        info!("Flight get_flight_info: {}", &sql[..sql.len().min(100)]);

        // Generate handle and store query
        let handle = Self::generate_handle();
        {
            let mut statements = self.statements.write().await;
            statements.insert(
                handle.clone(),
                StatementInfo {
                    sql,
                    created_at: std::time::Instant::now(),
                },
            );
        }

        // Create ticket for fetching results
        let ticket = Ticket {
            ticket: handle.into_bytes().into(),
        };

        let flight_info = FlightInfo::new()
            .with_descriptor(descriptor)
            .with_endpoint(arrow_flight::FlightEndpoint::new().with_ticket(ticket));

        Ok(Response::new(flight_info))
    }

    /// Get schema for a query
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        // Return empty schema (actual schema determined at query time)
        let schema = Schema::empty();
        let schema_result = SchemaResult {
            schema: schema.to_string().into_bytes().into(),
        };
        Ok(Response::new(schema_result))
    }

    /// Execute query and stream results (the main data path)
    #[instrument(skip(self, request))]
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let handle = String::from_utf8_lossy(&ticket.ticket).to_string();
        
        // Get query SQL
        let sql = {
            let statements = self.statements.read().await;
            statements
                .get(&handle)
                .map(|s| s.sql.clone())
                .ok_or_else(|| Status::not_found(format!("Query {} not found", handle)))?
        };

        info!("Flight do_get: executing query for handle {}", handle);

        // Execute query via worker
        let result = self
            .worker_client
            .execute_query(&sql, "flight-user")
            .await
            .map_err(|e| Status::internal(format!("Query failed: {}", e)))?;

        // Build Arrow schema from columns
        let fields: Vec<Field> = result.columns.iter()
            .map(|c| Field::new(&c.name, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Convert rows to Arrow RecordBatch
        let arrays: Vec<Arc<dyn arrow_array::Array>> = (0..result.columns.len())
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
                Arc::new(StringArray::from(values)) as Arc<dyn arrow_array::Array>
            })
            .collect();

        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| Status::internal(format!("Failed to create RecordBatch: {}", e)))?;

        info!("Flight do_get: returning {} rows", batch.num_rows());

        // Use FlightDataEncoder to properly encode the batch
        let batches = vec![Ok(batch)];
        let batch_stream = stream::iter(batches);
        
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map(|result| result.map_err(|e| Status::internal(format!("Encoding error: {}", e))));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    /// DoPut - for data upload (not supported)
    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Data upload not supported"))
    }

    /// DoExchange - bidirectional streaming (not supported)
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Exchange not supported"))
    }

    /// Execute an action
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        
        match action.r#type.as_str() {
            "HealthCheck" => {
                let result = arrow_flight::Result {
                    body: b"ok".to_vec().into(),
                };
                let stream = stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            _ => Err(Status::unimplemented(format!("Action {} not supported", action.r#type))),
        }
    }

    /// List available actions
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            ActionType {
                r#type: "HealthCheck".to_string(),
                description: "Check if server is healthy".to_string(),
            },
        ];
        let stream = stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    /// Poll for flight info completion
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("Polling not supported"))
    }
}

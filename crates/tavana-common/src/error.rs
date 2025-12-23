//! Error types for Tavana services

use std::fmt;
use tonic::Status;

/// Result type alias using TavanaError
pub type Result<T> = std::result::Result<T, TavanaError>;

/// Main error type for Tavana services
#[derive(Debug, thiserror::Error)]
pub enum TavanaError {
    /// Authentication failed
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Authorization denied
    #[error("Authorization denied: {0}")]
    AuthorizationDenied(String),

    /// Invalid token
    #[error("Invalid token: {0}")]
    InvalidToken(String),

    /// Token expired
    #[error("Token expired")]
    TokenExpired,

    /// Resource not found
    #[error("Resource not found: {0}")]
    NotFound(String),

    /// Resource already exists
    #[error("Resource already exists: {0}")]
    AlreadyExists(String),

    /// Invalid request/argument
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Query execution error
    #[error("Query execution failed: {0}")]
    QueryExecutionFailed(String),

    /// Query timeout
    #[error("Query timeout after {0} seconds")]
    QueryTimeout(u32),

    /// Query cancelled
    #[error("Query was cancelled")]
    QueryCancelled,

    /// Resource estimation error
    #[error("Resource estimation failed: {0}")]
    ResourceEstimationFailed(String),

    /// Storage error
    #[error("Storage error: {0}")]
    StorageError(String),

    /// Database error
    #[error("Database error: {0}")]
    DatabaseError(String),

    /// TLS/Certificate error
    #[error("TLS error: {0}")]
    TlsError(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Kubernetes error
    #[error("Kubernetes error: {0}")]
    KubernetesError(String),

    /// gRPC transport error
    #[error("gRPC error: {0}")]
    GrpcError(#[from] tonic::transport::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Rate limited
    #[error("Rate limited: retry after {0} seconds")]
    RateLimited(u32),

    /// Service unavailable
    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),
}

impl TavanaError {
    /// Create an internal error from any error type
    pub fn internal<E: fmt::Display>(err: E) -> Self {
        TavanaError::Internal(err.to_string())
    }

    /// Get the error code for this error type
    pub fn code(&self) -> &'static str {
        match self {
            TavanaError::AuthenticationFailed(_) => "UNAUTHENTICATED",
            TavanaError::AuthorizationDenied(_) => "PERMISSION_DENIED",
            TavanaError::InvalidToken(_) => "INVALID_TOKEN",
            TavanaError::TokenExpired => "TOKEN_EXPIRED",
            TavanaError::NotFound(_) => "NOT_FOUND",
            TavanaError::AlreadyExists(_) => "ALREADY_EXISTS",
            TavanaError::InvalidArgument(_) => "INVALID_ARGUMENT",
            TavanaError::QueryExecutionFailed(_) => "QUERY_FAILED",
            TavanaError::QueryTimeout(_) => "DEADLINE_EXCEEDED",
            TavanaError::QueryCancelled => "CANCELLED",
            TavanaError::ResourceEstimationFailed(_) => "RESOURCE_ESTIMATION_FAILED",
            TavanaError::StorageError(_) => "STORAGE_ERROR",
            TavanaError::DatabaseError(_) => "DATABASE_ERROR",
            TavanaError::TlsError(_) => "TLS_ERROR",
            TavanaError::ConfigError(_) => "CONFIG_ERROR",
            TavanaError::Internal(_) => "INTERNAL",
            TavanaError::KubernetesError(_) => "KUBERNETES_ERROR",
            TavanaError::GrpcError(_) => "GRPC_ERROR",
            TavanaError::SerializationError(_) => "SERIALIZATION_ERROR",
            TavanaError::RateLimited(_) => "RESOURCE_EXHAUSTED",
            TavanaError::ServiceUnavailable(_) => "UNAVAILABLE",
        }
    }
}

/// Convert TavanaError to gRPC Status for error responses
impl From<TavanaError> for Status {
    fn from(err: TavanaError) -> Self {
        let code = match &err {
            TavanaError::AuthenticationFailed(_) => tonic::Code::Unauthenticated,
            TavanaError::AuthorizationDenied(_) => tonic::Code::PermissionDenied,
            TavanaError::InvalidToken(_) => tonic::Code::Unauthenticated,
            TavanaError::TokenExpired => tonic::Code::Unauthenticated,
            TavanaError::NotFound(_) => tonic::Code::NotFound,
            TavanaError::AlreadyExists(_) => tonic::Code::AlreadyExists,
            TavanaError::InvalidArgument(_) => tonic::Code::InvalidArgument,
            TavanaError::QueryExecutionFailed(_) => tonic::Code::Internal,
            TavanaError::QueryTimeout(_) => tonic::Code::DeadlineExceeded,
            TavanaError::QueryCancelled => tonic::Code::Cancelled,
            TavanaError::ResourceEstimationFailed(_) => tonic::Code::Internal,
            TavanaError::StorageError(_) => tonic::Code::Internal,
            TavanaError::DatabaseError(_) => tonic::Code::Internal,
            TavanaError::TlsError(_) => tonic::Code::Internal,
            TavanaError::ConfigError(_) => tonic::Code::Internal,
            TavanaError::Internal(_) => tonic::Code::Internal,
            TavanaError::KubernetesError(_) => tonic::Code::Internal,
            TavanaError::GrpcError(_) => tonic::Code::Unavailable,
            TavanaError::SerializationError(_) => tonic::Code::Internal,
            TavanaError::RateLimited(_) => tonic::Code::ResourceExhausted,
            TavanaError::ServiceUnavailable(_) => tonic::Code::Unavailable,
        };

        Status::new(code, err.to_string())
    }
}

// Implement conversions from common error types
impl From<serde_json::Error> for TavanaError {
    fn from(err: serde_json::Error) -> Self {
        TavanaError::SerializationError(err.to_string())
    }
}

impl From<std::io::Error> for TavanaError {
    fn from(err: std::io::Error) -> Self {
        TavanaError::Internal(format!("IO error: {}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_codes() {
        assert_eq!(TavanaError::NotFound("test".into()).code(), "NOT_FOUND");
        assert_eq!(TavanaError::TokenExpired.code(), "TOKEN_EXPIRED");
    }

    #[test]
    fn test_error_to_status() {
        let err = TavanaError::NotFound("table 'users' not found".into());
        let status: Status = err.into();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }
}

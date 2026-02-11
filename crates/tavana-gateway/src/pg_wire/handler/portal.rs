//! Portal state management for Extended Query Protocol cursor streaming
//!
//! Supports two modes:
//! 1. **TRUE STREAMING**: Holds a gRPC stream handle, reads rows on-demand
//! 2. **BUFFERED**: Falls back to buffering for small results or compatibility

use crate::worker_client::StreamingResult;

/// Portal state for Extended Query Protocol cursor support
/// This enables JDBC setFetchSize() streaming by maintaining row state between Execute calls
pub(crate) enum PortalState {
    /// TRUE STREAMING: Holds gRPC stream handle, reads on-demand
    /// This is the preferred mode for large result sets
    Streaming(StreamingPortal),
    /// BUFFERED: Fallback for small results or when streaming setup fails
    Buffered(BufferedPortal),
}

/// Streaming portal - holds a live gRPC stream for true streaming
pub(crate) struct StreamingPortal {
    /// The gRPC streaming result from worker (owns the receiver)
    pub stream: StreamingResult,
    /// Column metadata from first batch
    pub columns: Vec<(String, String)>,
    /// Total rows sent so far (for CommandComplete)
    pub rows_sent: usize,
    /// Column count for DataRow consistency
    pub column_count: usize,
    /// Buffer for partial batch consumption (when max_rows < batch size)
    /// Rows that were read from stream but not yet sent to client
    pub pending_rows: Vec<Vec<String>>,
}

/// Buffered portal - stores all rows in memory (fallback mode)
pub(crate) struct BufferedPortal {
    /// All rows from query execution
    pub rows: Vec<Vec<String>>,
    /// Current row offset (for resumption after PortalSuspended)
    pub offset: usize,
    /// Column count for DataRow consistency
    pub column_count: usize,
}

impl PortalState {
    /// Create a new streaming portal
    pub fn new_streaming(stream: StreamingResult, columns: Vec<(String, String)>) -> Self {
        let column_count = columns.len();
        PortalState::Streaming(StreamingPortal {
            stream,
            columns,
            rows_sent: 0,
            column_count,
            pending_rows: Vec::new(),
        })
    }
    
    /// Create a new buffered portal (fallback)
    pub fn new_buffered(rows: Vec<Vec<String>>, column_count: usize) -> Self {
        PortalState::Buffered(BufferedPortal {
            rows,
            offset: 0,
            column_count,
        })
    }
}

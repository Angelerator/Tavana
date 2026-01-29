//! PostgreSQL wire protocol constants
//!
//! Transaction status codes and other protocol constants.

/// Transaction status: Idle (not in a transaction)
pub const TRANSACTION_STATUS_IDLE: u8 = b'I';

/// Transaction status: In a transaction block
pub const TRANSACTION_STATUS_IN_TRANSACTION: u8 = b'T';

/// Transaction status: In a failed transaction block
#[allow(dead_code)]
pub const TRANSACTION_STATUS_FAILED: u8 = b'E';

/// Legacy constant for backwards compatibility (when config is not passed)
pub const STREAMING_BATCH_SIZE: usize = 100;

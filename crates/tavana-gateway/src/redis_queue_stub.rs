//! Stub for removed Redis queue functionality
//! This allows existing code references to compile without Redis

use std::sync::Arc;

/// Stub queued query type
#[derive(Clone)]
pub struct QueuedQuery {
    pub id: String,
    pub sql: String,
    pub user_id: String,
}

/// Stub RedisQueue type (not implemented)
pub struct RedisQueue;

/// Stub RedisQueueConfig type (not implemented)  
pub struct RedisQueueConfig;

impl RedisQueue {
    pub async fn new(_config: RedisQueueConfig) -> anyhow::Result<Self> {
        anyhow::bail!("Redis queue not implemented")
    }

    pub async fn dequeue(&self) -> anyhow::Result<Option<QueuedQuery>> {
        Ok(None)
    }

    pub async fn complete(&self, _query_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    pub async fn fail(&self, _query_id: &str, _error: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

impl RedisQueueConfig {
    pub fn from_env() -> Self {
        Self
    }
}

/// Stub worker function (not implemented)
pub async fn start_queue_worker(
    _queue: Arc<RedisQueue>,
    _worker_client: Arc<crate::worker_client::WorkerClient>,
) {
    // No-op
}

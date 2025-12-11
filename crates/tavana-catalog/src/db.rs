//! Database operations for the catalog service

use sqlx::postgres::{PgPool, PgPoolOptions};
use anyhow::Result;
use tracing::info;

/// Database connection pool
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Create a new database connection pool
    pub async fn new(database_url: &str) -> Result<Self> {
        info!("Connecting to database...");
        
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .min_connections(1)
            .connect(database_url)
            .await?;
        
        info!("Database connection established");
        
        Ok(Self { pool })
    }

    /// Run database migrations
    pub async fn migrate(&self) -> Result<()> {
        info!("Running database migrations...");
        
        // Create catalogs table
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS catalogs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR(255) NOT NULL UNIQUE,
                comment TEXT,
                properties JSONB DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        "#)
        .execute(&self.pool)
        .await?;

        // Create schemas table
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS schemas (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                catalog_id UUID NOT NULL REFERENCES catalogs(id) ON DELETE CASCADE,
                name VARCHAR(255) NOT NULL,
                comment TEXT,
                properties JSONB DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(catalog_id, name)
            )
        "#)
        .execute(&self.pool)
        .await?;

        // Create tables table
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS tables (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                schema_id UUID NOT NULL REFERENCES schemas(id) ON DELETE CASCADE,
                name VARCHAR(255) NOT NULL,
                table_type VARCHAR(50) NOT NULL,
                data_format VARCHAR(50),
                storage_uri TEXT,
                storage_type VARCHAR(50),
                columns JSONB NOT NULL DEFAULT '[]',
                partition_columns JSONB DEFAULT '[]',
                comment TEXT,
                properties JSONB DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(schema_id, name)
            )
        "#)
        .execute(&self.pool)
        .await?;

        // Create table_statistics table
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS table_statistics (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                table_id UUID NOT NULL REFERENCES tables(id) ON DELETE CASCADE UNIQUE,
                row_count BIGINT,
                size_bytes BIGINT,
                file_count INTEGER,
                column_stats JSONB DEFAULT '{}',
                last_updated TIMESTAMPTZ DEFAULT NOW()
            )
        "#)
        .execute(&self.pool)
        .await?;

        // Create indexes
        sqlx::query(r#"
            CREATE INDEX IF NOT EXISTS idx_schemas_catalog_id ON schemas(catalog_id);
            CREATE INDEX IF NOT EXISTS idx_tables_schema_id ON tables(schema_id);
        "#)
        .execute(&self.pool)
        .await?;

        info!("Database migrations completed");
        
        Ok(())
    }

    /// Get the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}


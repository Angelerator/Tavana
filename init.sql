-- Tavana Database Schema
-- This file initializes the PostgreSQL database for catalog and metering services

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================================
-- CATALOG SCHEMA
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS catalog;

-- Catalogs (top-level namespace)
CREATE TABLE catalog.catalogs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    comment TEXT,
    properties JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Schemas (within catalogs)
CREATE TABLE catalog.schemas (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    catalog_id UUID NOT NULL REFERENCES catalog.catalogs(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    comment TEXT,
    properties JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(catalog_id, name)
);

-- Tables (within schemas)
CREATE TABLE catalog.tables (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    schema_id UUID NOT NULL REFERENCES catalog.schemas(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    table_type VARCHAR(50) NOT NULL DEFAULT 'EXTERNAL', -- MANAGED, EXTERNAL
    data_source_format VARCHAR(50) NOT NULL, -- PARQUET, DELTA, ICEBERG, CSV, JSON
    storage_location TEXT NOT NULL,
    comment TEXT,
    properties JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(schema_id, name)
);

-- Columns (within tables)
CREATE TABLE catalog.columns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_id UUID NOT NULL REFERENCES catalog.tables(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    data_type VARCHAR(100) NOT NULL,
    nullable BOOLEAN DEFAULT true,
    comment TEXT,
    position INTEGER NOT NULL,
    partition_index INTEGER, -- NULL if not a partition column
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_id, name)
);

-- Table statistics for query optimization
CREATE TABLE catalog.table_statistics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_id UUID NOT NULL REFERENCES catalog.tables(id) ON DELETE CASCADE,
    row_count BIGINT,
    size_bytes BIGINT,
    num_files INTEGER,
    last_modified TIMESTAMP WITH TIME ZONE,
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    statistics JSONB DEFAULT '{}'
);

-- Column statistics for query optimization
CREATE TABLE catalog.column_statistics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    column_id UUID NOT NULL REFERENCES catalog.columns(id) ON DELETE CASCADE,
    distinct_count BIGINT,
    null_count BIGINT,
    min_value TEXT,
    max_value TEXT,
    histogram JSONB,
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================================
-- METERING SCHEMA
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS metering;

-- Query execution logs
CREATE TABLE metering.query_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_id UUID NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    query_text TEXT NOT NULL,
    query_fingerprint VARCHAR(64), -- blake3 hash for similarity matching
    
    -- Timing
    submitted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    
    -- Status
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING', -- PENDING, RUNNING, COMPLETED, FAILED, CANCELLED
    error_message TEXT,
    
    -- Resource estimation vs actual
    estimated_memory_mb INTEGER,
    estimated_cpu_millicores INTEGER,
    actual_peak_memory_mb INTEGER,
    actual_cpu_seconds DOUBLE PRECISION,
    
    -- Data metrics
    bytes_scanned BIGINT DEFAULT 0,
    bytes_returned BIGINT DEFAULT 0,
    rows_returned BIGINT DEFAULT 0,
    
    -- Query details
    tables_accessed TEXT[],
    has_aggregation BOOLEAN DEFAULT false,
    has_join BOOLEAN DEFAULT false,
    has_window_function BOOLEAN DEFAULT false,
    
    -- Infrastructure
    pod_name VARCHAR(255),
    worker_node VARCHAR(255),
    
    -- Cost
    estimated_cost_usd DOUBLE PRECISION,
    actual_cost_usd DOUBLE PRECISION
);

-- Create indexes for common queries
CREATE INDEX idx_query_executions_user ON metering.query_executions(user_id);
CREATE INDEX idx_query_executions_submitted ON metering.query_executions(submitted_at);
CREATE INDEX idx_query_executions_fingerprint ON metering.query_executions(query_fingerprint);
CREATE INDEX idx_query_executions_status ON metering.query_executions(status);

-- Usage events for billing
CREATE TABLE metering.usage_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_id UUID REFERENCES metering.query_executions(id),
    user_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(50) NOT NULL, -- COMPUTE, STORAGE, NETWORK, DATA_SCAN
    
    -- Compute metrics
    cpu_seconds DOUBLE PRECISION DEFAULT 0,
    memory_gb_seconds DOUBLE PRECISION DEFAULT 0,
    
    -- Data metrics
    bytes_scanned BIGINT DEFAULT 0,
    bytes_returned BIGINT DEFAULT 0,
    
    -- Storage metrics
    result_cache_bytes BIGINT DEFAULT 0,
    
    -- Network metrics
    egress_bytes BIGINT DEFAULT 0,
    
    -- Billing
    unit_price_usd DOUBLE PRECISION,
    total_cost_usd DOUBLE PRECISION,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_usage_events_user ON metering.usage_events(user_id);
CREATE INDEX idx_usage_events_created ON metering.usage_events(created_at);
CREATE INDEX idx_usage_events_type ON metering.usage_events(event_type);

-- Daily usage summary for billing dashboards
CREATE TABLE metering.daily_usage_summary (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(255) NOT NULL,
    date DATE NOT NULL,
    
    -- Aggregated metrics
    total_queries INTEGER DEFAULT 0,
    successful_queries INTEGER DEFAULT 0,
    failed_queries INTEGER DEFAULT 0,
    
    total_cpu_seconds DOUBLE PRECISION DEFAULT 0,
    total_memory_gb_seconds DOUBLE PRECISION DEFAULT 0,
    total_bytes_scanned BIGINT DEFAULT 0,
    total_bytes_returned BIGINT DEFAULT 0,
    total_egress_bytes BIGINT DEFAULT 0,
    
    total_cost_usd DOUBLE PRECISION DEFAULT 0,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(user_id, date)
);

-- API Keys for authentication
CREATE TABLE metering.api_keys (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(255) NOT NULL,
    key_hash VARCHAR(128) NOT NULL, -- bcrypt hash of API key
    key_prefix VARCHAR(8) NOT NULL, -- First 8 chars for identification
    name VARCHAR(255),
    scopes TEXT[] DEFAULT '{"query:execute", "catalog:read"}',
    expires_at TIMESTAMP WITH TIME ZONE,
    last_used_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    revoked_at TIMESTAMP WITH TIME ZONE,
    
    UNIQUE(key_hash)
);

CREATE INDEX idx_api_keys_user ON metering.api_keys(user_id);
CREATE INDEX idx_api_keys_prefix ON metering.api_keys(key_prefix);

-- ============================================================================
-- SEED DATA
-- ============================================================================

-- Create default catalog
INSERT INTO catalog.catalogs (name, comment) 
VALUES ('default', 'Default catalog for Tavana');

-- Create default schema in default catalog
INSERT INTO catalog.schemas (catalog_id, name, comment)
SELECT id, 'public', 'Default public schema'
FROM catalog.catalogs WHERE name = 'default';

-- Create sample external table pointing to MinIO
INSERT INTO catalog.tables (schema_id, name, table_type, data_source_format, storage_location, comment)
SELECT s.id, 'sample_data', 'EXTERNAL', 'PARQUET', 's3://tavana-data/sample.parquet', 'Sample data for testing'
FROM catalog.schemas s
JOIN catalog.catalogs c ON s.catalog_id = c.id
WHERE c.name = 'default' AND s.name = 'public';

-- Add columns to sample table
INSERT INTO catalog.columns (table_id, name, data_type, nullable, position)
SELECT t.id, 'id', 'INTEGER', false, 1
FROM catalog.tables t WHERE t.name = 'sample_data'
UNION ALL
SELECT t.id, 'name', 'VARCHAR', true, 2
FROM catalog.tables t WHERE t.name = 'sample_data'
UNION ALL
SELECT t.id, 'value', 'DOUBLE', true, 3
FROM catalog.tables t WHERE t.name = 'sample_data'
UNION ALL
SELECT t.id, 'created_at', 'TIMESTAMP', true, 4
FROM catalog.tables t WHERE t.name = 'sample_data';

-- Create a test API key (key: tvn_test_key_12345678)
-- Hash is for demonstration only - in production use proper bcrypt
INSERT INTO metering.api_keys (user_id, key_hash, key_prefix, name, scopes)
VALUES (
    'test-user',
    '$2a$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/X4lS6VQz4VQz4VQz4', -- placeholder hash
    'tvn_test',
    'Test API Key',
    ARRAY['query:execute', 'catalog:read', 'catalog:write']
);

COMMIT;


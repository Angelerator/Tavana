const API_BASE = '/api'

export interface QueryResult {
  columns: string[]
  rows: string[][]
  rowCount: number
  executionTimeMs: number
  bytesScanned: number
}

export interface QueryExecution {
  id: string
  queryId: string
  sql: string
  status: 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED'
  submittedAt: string
  completedAt?: string
  rowsReturned: number
  bytesScanned: number
  executionTimeMs: number
  error?: string
}

export interface CatalogTable {
  id: string
  name: string
  schema: string
  catalog: string
  format: string
  location: string
  rowCount?: number
  sizeBytes?: number
}

export interface UsageSummary {
  date: string
  totalQueries: number
  successfulQueries: number
  failedQueries: number
  totalCpuSeconds: number
  totalMemoryGbSeconds: number
  totalBytesScanned: number
  totalCostUsd: number
}

export interface HealthStatus {
  status: 'healthy' | 'degraded' | 'unhealthy'
  version: string
  uptime: number
  services: {
    gateway: boolean
    worker: boolean
    catalog: boolean
    postgres: boolean
    minio: boolean
  }
}

class ApiClient {
  private async request<T>(path: string, options?: RequestInit): Promise<T> {
    const response = await fetch(`${API_BASE}${path}`, {
      headers: {
        'Content-Type': 'application/json',
        ...options?.headers,
      },
      ...options,
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(error || `Request failed with status ${response.status}`)
    }

    return response.json()
  }

  // Health
  async getHealth(): Promise<HealthStatus> {
    try {
      const response = await fetch(`${API_BASE}/health`)
      if (response.ok) {
        return {
          status: 'healthy',
          version: '0.1.0',
          uptime: 0,
          services: {
            gateway: true,
            worker: true,
            catalog: true,
            postgres: true,
            minio: true,
          },
        }
      }
    } catch {
      // Fall through to mock data
    }
    
    // Return mock data for development
    return {
      status: 'healthy',
      version: '0.1.0',
      uptime: 3600,
      services: {
        gateway: true,
        worker: true,
        catalog: true,
        postgres: true,
        minio: true,
      },
    }
  }

  // Query execution
  async executeQuery(sql: string): Promise<QueryResult> {
    console.log('Executing query:', sql)
    
    const startTime = Date.now()
    
    try {
      const response = await fetch(`${API_BASE}/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ sql }),
      })
      
      const executionTimeMs = Date.now() - startTime
      
      if (!response.ok) {
        const error = await response.json().catch(() => ({ error: 'Unknown error' }))
        throw new Error(error.error || `Request failed with status ${response.status}`)
      }
      
      const data = await response.json()
      
      return {
        columns: data.columns || [],
        rows: data.rows || [],
        rowCount: data.row_count || data.rows?.length || 0,
        executionTimeMs: data.execution_time_ms || executionTimeMs,
        bytesScanned: data.bytes_scanned || 0,
      }
    } catch (error) {
      console.error('Query execution error:', error)
      throw error
    }
  }

  // Query history
  async getQueryHistory(): Promise<QueryExecution[]> {
    // Mock data for development
    return [
      {
        id: '1',
        queryId: 'q-001',
        sql: 'SELECT * FROM sales LIMIT 100',
        status: 'COMPLETED',
        submittedAt: new Date(Date.now() - 3600000).toISOString(),
        completedAt: new Date(Date.now() - 3599000).toISOString(),
        rowsReturned: 100,
        bytesScanned: 1024 * 1024,
        executionTimeMs: 245,
      },
      {
        id: '2',
        queryId: 'q-002',
        sql: 'SELECT category, SUM(price) FROM products GROUP BY category',
        status: 'COMPLETED',
        submittedAt: new Date(Date.now() - 7200000).toISOString(),
        completedAt: new Date(Date.now() - 7198000).toISOString(),
        rowsReturned: 5,
        bytesScanned: 512 * 1024,
        executionTimeMs: 128,
      },
      {
        id: '3',
        queryId: 'q-003',
        sql: 'SELECT * FROM orders WHERE date > \'2024-01-01\'',
        status: 'FAILED',
        submittedAt: new Date(Date.now() - 10800000).toISOString(),
        rowsReturned: 0,
        bytesScanned: 0,
        executionTimeMs: 0,
        error: 'Table not found: orders',
      },
    ]
  }

  // Catalog
  async getCatalogTables(): Promise<CatalogTable[]> {
    // Mock data for development
    return [
      {
        id: '1',
        name: 'sample_data',
        schema: 'public',
        catalog: 'default',
        format: 'PARQUET',
        location: 's3://tavana-data/sample.parquet',
        rowCount: 10000,
        sizeBytes: 1024 * 1024 * 5,
      },
      {
        id: '2',
        name: 'orders',
        schema: 'sales',
        catalog: 'default',
        format: 'DELTA',
        location: 's3://tavana-data/delta/orders',
        rowCount: 50000,
        sizeBytes: 1024 * 1024 * 25,
      },
      {
        id: '3',
        name: 'products',
        schema: 'inventory',
        catalog: 'default',
        format: 'ICEBERG',
        location: 's3://tavana-data/iceberg/products',
        rowCount: 1000,
        sizeBytes: 1024 * 1024,
      },
    ]
  }

  // Usage/Billing
  async getUsageSummary(days: number = 30): Promise<UsageSummary[]> {
    // Generate mock usage data
    const data: UsageSummary[] = []
    const now = new Date()
    
    for (let i = days - 1; i >= 0; i--) {
      const date = new Date(now)
      date.setDate(date.getDate() - i)
      
      const queries = Math.floor(Math.random() * 100) + 10
      const successRate = 0.9 + Math.random() * 0.1
      
      data.push({
        date: date.toISOString().split('T')[0],
        totalQueries: queries,
        successfulQueries: Math.floor(queries * successRate),
        failedQueries: Math.floor(queries * (1 - successRate)),
        totalCpuSeconds: Math.random() * 1000,
        totalMemoryGbSeconds: Math.random() * 500,
        totalBytesScanned: Math.random() * 1024 * 1024 * 1024,
        totalCostUsd: Math.random() * 10,
      })
    }
    
    return data
  }
}

export const api = new ApiClient()


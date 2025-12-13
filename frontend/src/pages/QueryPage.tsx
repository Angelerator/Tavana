import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { api, QueryResult } from '@/api/client'
import { formatBytes, formatDuration, cn } from '@/lib/utils'
import { Play, Loader2, Copy, Download, Clock, Database } from 'lucide-react'

const SAMPLE_QUERIES = [
  {
    name: 'DuckDB Version',
    sql: 'SELECT version() as duckdb_version, current_timestamp as query_time;',
  },
  {
    name: 'Simple Math',
    sql: 'SELECT 1+1 as result, 2*3 as product, 10/2 as division;',
  },
  {
    name: 'Generate Series',
    sql: 'SELECT * FROM generate_series(1, 10) as t(num);',
  },
  {
    name: 'TPC-H Lineitem (10 rows)',
    sql: "SELECT * FROM read_parquet('s3://tavana-data/tpch/sf_1/lineitem.parquet') LIMIT 10;",
  },
  {
    name: 'TPC-H Count',
    sql: "SELECT count(*) as total_rows FROM read_parquet('s3://tavana-data/tpch/sf_1/lineitem.parquet');",
  },
  {
    name: 'TPC-H Aggregation',
    sql: `SELECT 
  l_returnflag, 
  l_linestatus,
  count(*) as count,
  sum(l_quantity) as sum_qty,
  avg(l_extendedprice) as avg_price
FROM read_parquet('s3://tavana-data/tpch/sf_1/lineitem.parquet')
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;`,
  },
]

export default function QueryPage() {
  const [sql, setSql] = useState(SAMPLE_QUERIES[0].sql)
  const [result, setResult] = useState<QueryResult | null>(null)

  const mutation = useMutation({
    mutationFn: (sql: string) => api.executeQuery(sql),
    onSuccess: (data) => {
      setResult(data)
    },
  })

  const handleExecute = () => {
    mutation.mutate(sql)
  }

  const handleCopy = () => {
    if (result) {
      const csv = [result.columns.join(','), ...result.rows.map((r) => r.join(','))].join('\n')
      navigator.clipboard.writeText(csv)
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Query Editor</h1>
          <p className="text-muted-foreground mt-1">
            Execute SQL queries against your data sources
          </p>
        </div>
      </div>

      {/* Sample Queries */}
      <div className="flex gap-2 flex-wrap">
        {SAMPLE_QUERIES.map((query) => (
          <Button
            key={query.name}
            variant="outline"
            size="sm"
            onClick={() => setSql(query.sql)}
          >
            {query.name}
          </Button>
        ))}
      </div>

      {/* Editor */}
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base">SQL Editor</CardTitle>
            <Button
              onClick={handleExecute}
              disabled={mutation.isPending || !sql.trim()}
              className="gap-2"
            >
              {mutation.isPending ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Play className="w-4 h-4" />
              )}
              Execute
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <textarea
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            className="sql-editor w-full h-48 p-4 resize-none"
            placeholder="Enter your SQL query..."
            spellCheck={false}
          />
        </CardContent>
      </Card>

      {/* Error */}
      {mutation.isError && (
        <Card className="border-destructive/50 bg-destructive/10">
          <CardContent className="p-4">
            <p className="text-destructive text-sm">
              {mutation.error instanceof Error
                ? mutation.error.message
                : 'Query execution failed'}
            </p>
          </CardContent>
        </Card>
      )}

      {/* Results */}
      {result && (
        <Card>
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-base">Results</CardTitle>
              <div className="flex items-center gap-4">
                <div className="flex items-center gap-4 text-sm text-muted-foreground">
                  <span className="flex items-center gap-1">
                    <Database className="w-4 h-4" />
                    {result.rowCount} rows
                  </span>
                  <span className="flex items-center gap-1">
                    <Clock className="w-4 h-4" />
                    {formatDuration(result.executionTimeMs)}
                  </span>
                  <span>{formatBytes(result.bytesScanned)} scanned</span>
                </div>
                <div className="flex gap-2">
                  <Button variant="outline" size="sm" onClick={handleCopy}>
                    <Copy className="w-4 h-4 mr-1" />
                    Copy
                  </Button>
                  <Button variant="outline" size="sm">
                    <Download className="w-4 h-4 mr-1" />
                    Export
                  </Button>
                </div>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="overflow-auto max-h-96 rounded-lg border border-border">
              <table className="w-full text-sm">
                <thead className="bg-secondary sticky top-0">
                  <tr>
                    {result.columns.map((col, i) => (
                      <th
                        key={i}
                        className="px-4 py-3 text-left font-medium text-muted-foreground border-b border-border"
                      >
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.rows.map((row, i) => (
                    <tr
                      key={i}
                      className={cn(
                        'border-b border-border/50 hover:bg-secondary/50 transition-colors',
                        i % 2 === 0 ? 'bg-background' : 'bg-secondary/20'
                      )}
                    >
                      {row.map((cell, j) => (
                        <td key={j} className="px-4 py-2.5 font-mono text-sm">
                          {cell || <span className="text-muted-foreground">NULL</span>}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}


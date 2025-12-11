import { useQuery } from '@tanstack/react-query'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { api } from '@/api/client'
import { formatBytes, formatNumber, cn } from '@/lib/utils'
import {
  Database,
  Table2,
  FileType,
  MapPin,
  RefreshCw,
  Search,
  Plus,
} from 'lucide-react'
import { useState } from 'react'

export default function CatalogPage() {
  const [searchTerm, setSearchTerm] = useState('')

  const { data: tables, isLoading, refetch } = useQuery({
    queryKey: ['catalog-tables'],
    queryFn: () => api.getCatalogTables(),
  })

  const filteredTables = tables?.filter(
    (t) =>
      t.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      t.schema.toLowerCase().includes(searchTerm.toLowerCase())
  )

  const formatBadge = (format: string) => {
    const colors: Record<string, string> = {
      PARQUET: 'bg-blue-500/20 text-blue-400',
      DELTA: 'bg-green-500/20 text-green-400',
      ICEBERG: 'bg-purple-500/20 text-purple-400',
      CSV: 'bg-yellow-500/20 text-yellow-400',
      JSON: 'bg-orange-500/20 text-orange-400',
    }
    return colors[format] || 'bg-gray-500/20 text-gray-400'
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Catalog</h1>
          <p className="text-muted-foreground mt-1">
            Browse and manage your data catalog
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={() => refetch()}>
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh
          </Button>
          <Button>
            <Plus className="w-4 h-4 mr-2" />
            Register Table
          </Button>
        </div>
      </div>

      {/* Search */}
      <Card>
        <CardContent className="p-4">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
            <input
              type="text"
              placeholder="Search tables..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 bg-secondary rounded-lg border border-border focus:outline-none focus:ring-2 focus:ring-primary"
            />
          </div>
        </CardContent>
      </Card>

      {/* Tables List */}
      {isLoading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {[1, 2, 3].map((i) => (
            <Card key={i} className="shimmer">
              <CardContent className="p-6 h-48" />
            </Card>
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {filteredTables?.map((table) => (
            <Card
              key={table.id}
              className="hover:border-primary/50 transition-colors cursor-pointer"
            >
              <CardHeader className="pb-3">
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-2">
                    <div className="p-2 rounded-lg bg-primary/10">
                      <Table2 className="w-4 h-4 text-primary" />
                    </div>
                    <div>
                      <CardTitle className="text-base">{table.name}</CardTitle>
                      <p className="text-xs text-muted-foreground">
                        {table.catalog}.{table.schema}
                      </p>
                    </div>
                  </div>
                  <span
                    className={cn(
                      'px-2 py-1 rounded text-xs font-medium',
                      formatBadge(table.format)
                    )}
                  >
                    {table.format}
                  </span>
                </div>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <MapPin className="w-4 h-4" />
                  <span className="truncate font-mono text-xs">
                    {table.location}
                  </span>
                </div>
                <div className="flex items-center justify-between text-sm">
                  <div className="flex items-center gap-1 text-muted-foreground">
                    <Database className="w-4 h-4" />
                    <span>{formatNumber(table.rowCount || 0)} rows</span>
                  </div>
                  <div className="flex items-center gap-1 text-muted-foreground">
                    <FileType className="w-4 h-4" />
                    <span>{formatBytes(table.sizeBytes || 0)}</span>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {filteredTables?.length === 0 && !isLoading && (
        <Card>
          <CardContent className="p-12 text-center">
            <Database className="w-12 h-12 mx-auto text-muted-foreground mb-4" />
            <h3 className="text-lg font-medium">No tables found</h3>
            <p className="text-muted-foreground mt-1">
              {searchTerm
                ? 'Try a different search term'
                : 'Register a table to get started'}
            </p>
          </CardContent>
        </Card>
      )}
    </div>
  )
}


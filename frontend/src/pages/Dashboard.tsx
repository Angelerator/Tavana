import { useQuery } from '@tanstack/react-query'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { api } from '@/api/client'
import { formatBytes, formatNumber, formatDuration } from '@/lib/utils'
import {
  Activity,
  Database,
  Clock,
  Zap,
  TrendingUp,
  HardDrive,
} from 'lucide-react'
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'

export default function Dashboard() {
  const { data: health } = useQuery({
    queryKey: ['health'],
    queryFn: () => api.getHealth(),
    refetchInterval: 30000,
  })

  const { data: history } = useQuery({
    queryKey: ['queryHistory'],
    queryFn: () => api.getQueryHistory(),
  })

  const { data: usage } = useQuery({
    queryKey: ['usage', 7],
    queryFn: () => api.getUsageSummary(7),
  })

  const stats = {
    totalQueries: history?.length || 0,
    successRate:
      history && history.length > 0
        ? (history.filter((q) => q.status === 'COMPLETED').length /
            history.length) *
          100
        : 0,
    avgExecutionTime:
      history && history.length > 0
        ? history.reduce((acc, q) => acc + q.executionTimeMs, 0) / history.length
        : 0,
    totalBytesScanned:
      history?.reduce((acc, q) => acc + q.bytesScanned, 0) || 0,
  }

  return (
    <div className="space-y-8">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
        <p className="text-muted-foreground mt-1">
          Monitor your Tavana DuckDB cluster performance
        </p>
      </div>

      {/* Service Status */}
      <Card className="border-primary/20 glow-primary">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
            <Activity className="w-4 h-4" />
            Service Status
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-6 flex-wrap">
            {health?.services &&
              Object.entries(health.services).map(([name, status]) => (
                <div key={name} className="flex items-center gap-2">
                  <div
                    className={`w-2.5 h-2.5 rounded-full ${
                      status ? 'bg-green-500' : 'bg-red-500'
                    }`}
                  />
                  <span className="text-sm capitalize">{name}</span>
                </div>
              ))}
          </div>
        </CardContent>
      </Card>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          title="Total Queries"
          value={formatNumber(stats.totalQueries)}
          icon={Database}
          trend="+12%"
        />
        <StatCard
          title="Success Rate"
          value={`${stats.successRate.toFixed(1)}%`}
          icon={TrendingUp}
          trend="+2.1%"
        />
        <StatCard
          title="Avg Execution Time"
          value={formatDuration(stats.avgExecutionTime)}
          icon={Clock}
          trend="-15%"
        />
        <StatCard
          title="Data Scanned"
          value={formatBytes(stats.totalBytesScanned)}
          icon={HardDrive}
          trend="+8%"
        />
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Query Volume (7 days)</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                  data={usage || []}
                  margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
                >
                  <defs>
                    <linearGradient id="colorQueries" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#0ea5e9" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#0ea5e9" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid
                    strokeDasharray="3 3"
                    stroke="hsl(217 33% 22%)"
                    vertical={false}
                  />
                  <XAxis
                    dataKey="date"
                    tick={{ fill: 'hsl(215 20% 65%)', fontSize: 12 }}
                    tickFormatter={(value) =>
                      new Date(value).toLocaleDateString('en-US', {
                        weekday: 'short',
                      })
                    }
                    axisLine={false}
                    tickLine={false}
                  />
                  <YAxis
                    tick={{ fill: 'hsl(215 20% 65%)', fontSize: 12 }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: 'hsl(222 47% 14%)',
                      border: '1px solid hsl(217 33% 22%)',
                      borderRadius: '8px',
                    }}
                    labelStyle={{ color: 'hsl(210 40% 98%)' }}
                  />
                  <Area
                    type="monotone"
                    dataKey="totalQueries"
                    stroke="#0ea5e9"
                    strokeWidth={2}
                    fillOpacity={1}
                    fill="url(#colorQueries)"
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base">Recent Queries</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {history?.slice(0, 5).map((query) => (
                <div
                  key={query.id}
                  className="flex items-start gap-3 p-3 rounded-lg bg-secondary/50"
                >
                  <div
                    className={`w-2 h-2 mt-2 rounded-full ${
                      query.status === 'COMPLETED'
                        ? 'bg-green-500'
                        : query.status === 'FAILED'
                        ? 'bg-red-500'
                        : 'bg-yellow-500'
                    }`}
                  />
                  <div className="flex-1 min-w-0">
                    <p className="font-mono text-sm truncate">{query.sql}</p>
                    <div className="flex gap-4 mt-1 text-xs text-muted-foreground">
                      <span>{formatDuration(query.executionTimeMs)}</span>
                      <span>{formatBytes(query.bytesScanned)}</span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

function StatCard({
  title,
  value,
  icon: Icon,
  trend,
}: {
  title: string
  value: string
  icon: React.ComponentType<{ className?: string }>
  trend: string
}) {
  const isPositive = trend.startsWith('+')
  const isNegative = trend.startsWith('-')

  return (
    <Card>
      <CardContent className="p-6">
        <div className="flex items-center justify-between">
          <div className="p-2 rounded-lg bg-primary/10">
            <Icon className="w-5 h-5 text-primary" />
          </div>
          <span
            className={`text-xs font-medium ${
              isPositive
                ? 'text-green-500'
                : isNegative
                ? 'text-red-500'
                : 'text-muted-foreground'
            }`}
          >
            {trend}
          </span>
        </div>
        <div className="mt-4">
          <p className="text-2xl font-bold">{value}</p>
          <p className="text-sm text-muted-foreground mt-1">{title}</p>
        </div>
      </CardContent>
    </Card>
  )
}


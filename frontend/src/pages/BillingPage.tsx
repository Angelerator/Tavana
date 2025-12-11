import { useQuery } from '@tanstack/react-query'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { api } from '@/api/client'
import { formatBytes, formatNumber, cn } from '@/lib/utils'
import {
  DollarSign,
  Activity,
  Database,
  HardDrive,
  TrendingUp,
  TrendingDown,
  Calendar,
} from 'lucide-react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  LineChart,
  Line,
} from 'recharts'

export default function BillingPage() {
  const { data: usage } = useQuery({
    queryKey: ['usage', 30],
    queryFn: () => api.getUsageSummary(30),
  })

  const totals = usage?.reduce(
    (acc, day) => ({
      totalQueries: acc.totalQueries + day.totalQueries,
      totalCpuSeconds: acc.totalCpuSeconds + day.totalCpuSeconds,
      totalBytesScanned: acc.totalBytesScanned + day.totalBytesScanned,
      totalCost: acc.totalCost + day.totalCostUsd,
    }),
    { totalQueries: 0, totalCpuSeconds: 0, totalBytesScanned: 0, totalCost: 0 }
  )

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Usage & Billing</h1>
          <p className="text-muted-foreground mt-1">
            Monitor your resource usage and costs
          </p>
        </div>
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Calendar className="w-4 h-4" />
          Last 30 days
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <SummaryCard
          title="Total Cost"
          value={`$${(totals?.totalCost || 0).toFixed(2)}`}
          icon={DollarSign}
          trend="+5.2%"
          trendUp={true}
        />
        <SummaryCard
          title="Total Queries"
          value={formatNumber(totals?.totalQueries || 0)}
          icon={Activity}
          trend="+12%"
          trendUp={true}
        />
        <SummaryCard
          title="CPU Time"
          value={`${((totals?.totalCpuSeconds || 0) / 3600).toFixed(1)}h`}
          icon={Database}
          trend="-8%"
          trendUp={false}
        />
        <SummaryCard
          title="Data Scanned"
          value={formatBytes(totals?.totalBytesScanned || 0)}
          icon={HardDrive}
          trend="+15%"
          trendUp={true}
        />
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Daily Cost</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={usage || []}
                  margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
                >
                  <CartesianGrid
                    strokeDasharray="3 3"
                    stroke="hsl(217 33% 22%)"
                    vertical={false}
                  />
                  <XAxis
                    dataKey="date"
                    tick={{ fill: 'hsl(215 20% 65%)', fontSize: 12 }}
                    tickFormatter={(value) =>
                      new Date(value).toLocaleDateString('en-US', { day: 'numeric' })
                    }
                    axisLine={false}
                    tickLine={false}
                  />
                  <YAxis
                    tick={{ fill: 'hsl(215 20% 65%)', fontSize: 12 }}
                    tickFormatter={(value) => `$${value.toFixed(0)}`}
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
                    formatter={(value: number) => [`$${value.toFixed(2)}`, 'Cost']}
                  />
                  <Bar
                    dataKey="totalCostUsd"
                    fill="#0ea5e9"
                    radius={[4, 4, 0, 0]}
                  />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base">Query Success Rate</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart
                  data={usage?.map((d) => ({
                    ...d,
                    successRate:
                      d.totalQueries > 0
                        ? (d.successfulQueries / d.totalQueries) * 100
                        : 0,
                  }))}
                  margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
                >
                  <CartesianGrid
                    strokeDasharray="3 3"
                    stroke="hsl(217 33% 22%)"
                    vertical={false}
                  />
                  <XAxis
                    dataKey="date"
                    tick={{ fill: 'hsl(215 20% 65%)', fontSize: 12 }}
                    tickFormatter={(value) =>
                      new Date(value).toLocaleDateString('en-US', { day: 'numeric' })
                    }
                    axisLine={false}
                    tickLine={false}
                  />
                  <YAxis
                    tick={{ fill: 'hsl(215 20% 65%)', fontSize: 12 }}
                    tickFormatter={(value) => `${value}%`}
                    domain={[0, 100]}
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
                    formatter={(value: number) => [`${value.toFixed(1)}%`, 'Success Rate']}
                  />
                  <Line
                    type="monotone"
                    dataKey="successRate"
                    stroke="#22c55e"
                    strokeWidth={2}
                    dot={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Cost Breakdown */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Cost Breakdown</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <CostRow label="Compute (CPU)" value={12.50} percentage={45} />
            <CostRow label="Memory (GB-hours)" value={8.25} percentage={30} />
            <CostRow label="Data Scanned" value={5.00} percentage={18} />
            <CostRow label="Network Egress" value={1.89} percentage={7} />
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function SummaryCard({
  title,
  value,
  icon: Icon,
  trend,
  trendUp,
}: {
  title: string
  value: string
  icon: React.ComponentType<{ className?: string }>
  trend: string
  trendUp: boolean
}) {
  return (
    <Card>
      <CardContent className="p-6">
        <div className="flex items-center justify-between">
          <div className="p-2 rounded-lg bg-primary/10">
            <Icon className="w-5 h-5 text-primary" />
          </div>
          <div
            className={cn(
              'flex items-center gap-1 text-xs font-medium',
              trendUp ? 'text-green-500' : 'text-red-500'
            )}
          >
            {trendUp ? (
              <TrendingUp className="w-3 h-3" />
            ) : (
              <TrendingDown className="w-3 h-3" />
            )}
            {trend}
          </div>
        </div>
        <div className="mt-4">
          <p className="text-2xl font-bold">{value}</p>
          <p className="text-sm text-muted-foreground mt-1">{title}</p>
        </div>
      </CardContent>
    </Card>
  )
}

function CostRow({
  label,
  value,
  percentage,
}: {
  label: string
  value: number
  percentage: number
}) {
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between text-sm">
        <span>{label}</span>
        <span className="font-medium">${value.toFixed(2)}</span>
      </div>
      <div className="h-2 rounded-full bg-secondary overflow-hidden">
        <div
          className="h-full bg-primary rounded-full transition-all"
          style={{ width: `${percentage}%` }}
        />
      </div>
    </div>
  )
}


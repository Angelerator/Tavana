import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import {
  Key,
  Shield,
  Bell,
  Database,
  Plus,
  Copy,
  Trash2,
  Eye,
  EyeOff,
} from 'lucide-react'

interface ApiKey {
  id: string
  name: string
  prefix: string
  createdAt: string
  lastUsed: string | null
  scopes: string[]
}

const MOCK_API_KEYS: ApiKey[] = [
  {
    id: '1',
    name: 'Development Key',
    prefix: 'tvn_dev',
    createdAt: '2024-01-15',
    lastUsed: '2024-01-20',
    scopes: ['query:execute', 'catalog:read'],
  },
  {
    id: '2',
    name: 'Production Key',
    prefix: 'tvn_prod',
    createdAt: '2024-01-10',
    lastUsed: '2024-01-21',
    scopes: ['query:execute', 'catalog:read', 'catalog:write'],
  },
]

export default function SettingsPage() {
  const [apiKeys] = useState<ApiKey[]>(MOCK_API_KEYS)
  const [showKey, setShowKey] = useState<string | null>(null)

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Settings</h1>
        <p className="text-muted-foreground mt-1">
          Manage your account and API access
        </p>
      </div>

      {/* API Keys */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="text-lg flex items-center gap-2">
                <Key className="w-5 h-5" />
                API Keys
              </CardTitle>
              <CardDescription>
                Manage API keys for programmatic access
              </CardDescription>
            </div>
            <Button>
              <Plus className="w-4 h-4 mr-2" />
              Create Key
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {apiKeys.map((key) => (
              <div
                key={key.id}
                className="flex items-center justify-between p-4 rounded-lg bg-secondary/50 border border-border"
              >
                <div className="space-y-1">
                  <div className="flex items-center gap-2">
                    <span className="font-medium">{key.name}</span>
                    <span className="px-2 py-0.5 rounded text-xs bg-primary/10 text-primary">
                      {key.scopes.length} scopes
                    </span>
                  </div>
                  <div className="flex items-center gap-4 text-sm text-muted-foreground">
                    <span className="font-mono">
                      {showKey === key.id ? `${key.prefix}_xxxxxxxxxxxxx` : `${key.prefix}_****`}
                    </span>
                    <span>Created: {key.createdAt}</span>
                    {key.lastUsed && <span>Last used: {key.lastUsed}</span>}
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => setShowKey(showKey === key.id ? null : key.id)}
                  >
                    {showKey === key.id ? (
                      <EyeOff className="w-4 h-4" />
                    ) : (
                      <Eye className="w-4 h-4" />
                    )}
                  </Button>
                  <Button variant="ghost" size="icon">
                    <Copy className="w-4 h-4" />
                  </Button>
                  <Button variant="ghost" size="icon" className="text-destructive">
                    <Trash2 className="w-4 h-4" />
                  </Button>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Security Settings */}
      <Card>
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <Shield className="w-5 h-5" />
            Security
          </CardTitle>
          <CardDescription>
            Configure security and authentication settings
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="font-medium">Two-Factor Authentication</p>
              <p className="text-sm text-muted-foreground">
                Add an extra layer of security to your account
              </p>
            </div>
            <Button variant="outline">Enable 2FA</Button>
          </div>
          <div className="flex items-center justify-between">
            <div>
              <p className="font-medium">OIDC Integration</p>
              <p className="text-sm text-muted-foreground">
                Connect with your identity provider
              </p>
            </div>
            <Button variant="outline">Configure</Button>
          </div>
          <div className="flex items-center justify-between">
            <div>
              <p className="font-medium">IP Allowlist</p>
              <p className="text-sm text-muted-foreground">
                Restrict API access to specific IP addresses
              </p>
            </div>
            <Button variant="outline">Manage</Button>
          </div>
        </CardContent>
      </Card>

      {/* Data Sources */}
      <Card>
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <Database className="w-5 h-5" />
            Data Source Credentials
          </CardTitle>
          <CardDescription>
            Configure credentials for cloud storage access
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <CredentialRow
            name="AWS S3"
            status="connected"
            description="Access to s3://tavana-data/*"
          />
          <CredentialRow
            name="Azure ADLS"
            status="not_configured"
            description="Not configured"
          />
          <CredentialRow
            name="Google Cloud Storage"
            status="not_configured"
            description="Not configured"
          />
        </CardContent>
      </Card>

      {/* Notifications */}
      <Card>
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <Bell className="w-5 h-5" />
            Notifications
          </CardTitle>
          <CardDescription>
            Configure how you receive alerts and updates
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <NotificationRow
            title="Query Failures"
            description="Get notified when queries fail"
            enabled={true}
          />
          <NotificationRow
            title="Billing Alerts"
            description="Get notified when spending exceeds thresholds"
            enabled={true}
          />
          <NotificationRow
            title="Weekly Summary"
            description="Receive weekly usage reports"
            enabled={false}
          />
        </CardContent>
      </Card>
    </div>
  )
}

function CredentialRow({
  name,
  status,
  description,
}: {
  name: string
  status: 'connected' | 'not_configured' | 'error'
  description: string
}) {
  return (
    <div className="flex items-center justify-between p-4 rounded-lg bg-secondary/50 border border-border">
      <div className="flex items-center gap-3">
        <div
          className={cn(
            'w-2 h-2 rounded-full',
            status === 'connected'
              ? 'bg-green-500'
              : status === 'error'
              ? 'bg-red-500'
              : 'bg-gray-500'
          )}
        />
        <div>
          <p className="font-medium">{name}</p>
          <p className="text-sm text-muted-foreground">{description}</p>
        </div>
      </div>
      <Button variant="outline" size="sm">
        {status === 'connected' ? 'Update' : 'Configure'}
      </Button>
    </div>
  )
}

function NotificationRow({
  title,
  description,
  enabled,
}: {
  title: string
  description: string
  enabled: boolean
}) {
  const [isEnabled, setIsEnabled] = useState(enabled)

  return (
    <div className="flex items-center justify-between">
      <div>
        <p className="font-medium">{title}</p>
        <p className="text-sm text-muted-foreground">{description}</p>
      </div>
      <button
        onClick={() => setIsEnabled(!isEnabled)}
        className={cn(
          'relative w-11 h-6 rounded-full transition-colors',
          isEnabled ? 'bg-primary' : 'bg-secondary'
        )}
      >
        <span
          className={cn(
            'absolute top-0.5 left-0.5 w-5 h-5 rounded-full bg-white transition-transform',
            isEnabled && 'translate-x-5'
          )}
        />
      </button>
    </div>
  )
}


# Tavana Authentication Guide

Tavana provides a **pluggable authentication gateway** that supports multiple authentication methods. **Authentication is optional** and can be completely disabled for community editions or internal deployments.

## Quick Start

### Community Edition (No Auth)

```yaml
# values.yaml
auth:
  mode: "passthrough"  # Default - no auth required
```

```bash
# Connect without credentials
psql -h tavana.example.com -p 5432 -U anyuser -d postgres
```

### Enterprise Edition (With Auth)

```yaml
# values.yaml
auth:
  mode: "required"
  separ:
    enabled: true
    endpoint: "http://separ:8080"
```

```bash
# Connect with JWT token as password
psql -h tavana.example.com -p 5432 -U user@company.com -d postgres
# Password: <JWT token or password>
```

---

## Authentication Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `passthrough` | No authentication required (default) | Community edition, dev/test |
| `required` | Authentication mandatory | Enterprise production |
| `optional` | Auth optional, allow anonymous | Hybrid environments |

---

## Authentication Providers

Tavana supports multiple auth providers. You can enable one or more simultaneously.

### 1. Separ (Centralized Authorization)

Enterprise-grade multi-tenant authorization platform.

```yaml
auth:
  mode: "required"
  separ:
    enabled: true
    endpoint: "http://separ.separ.svc.cluster.local:8080"
    enableAuthz: true
    tenantExtraction: "email_domain"
    existingSecret: "tavana-separ-auth"
    existingSecretKey: "separ-api-key"
```

**Supported credentials:**
- Username/password
- JWT tokens
- API keys (`sk_*`, `pat_*`)
- Service account keys (`sak_*`)

### 2. Direct JWT/OIDC (Any Identity Provider)

Validate JWTs directly without Separ. Works with any OIDC-compliant IdP:
- Azure AD / Entra ID
- Okta
- Auth0
- Keycloak
- Google Identity

```yaml
auth:
  mode: "required"
  jwt:
    enabled: true
    issuer: "https://login.microsoftonline.com/{tenant}/v2.0"
    audience: "tavana-api"
    algorithm: "RS256"
    jwksUri: "https://login.microsoftonline.com/{tenant}/discovery/v2.0/keys"
```

### 3. Static API Keys

Simple API key authentication for services.

```yaml
auth:
  mode: "required"
  staticKeys:
    enabled: true
```

Create API keys via Kubernetes secrets or Tavana admin API.

### 4. Passthrough (No Auth)

Accept all connections without authentication.

```yaml
auth:
  mode: "passthrough"
```

---

## Adding Custom Providers

Tavana's auth system is extensible. To add a new provider:

1. Implement the `AuthProvider` trait:

```rust
#[async_trait]
pub trait AuthProvider: Send + Sync {
    fn name(&self) -> &str;
    fn can_handle(&self, credentials: &Credentials) -> bool;
    async fn authenticate(&self, credentials: &Credentials) -> AuthResult;
}
```

2. Register in `AuthGateway::new()`:

```rust
if let Some(ref custom_config) = config.custom_provider {
    providers.push(Arc::new(CustomProvider::new(custom_config)));
}
```

---

## Credential Types

| Type | Format | Example |
|------|--------|---------|
| Password | Any string | `mysecretpassword` |
| JWT | `eyJ...` | `eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...` |
| API Key | `tvn_*`, `sk_*` | `tvn_abc123def456` |
| Personal Access Token | `pat_*` | `pat_xyz789abc` |
| Service Account Key | `sak_*` | `sak_service123` |

---

## Connection Examples

### psql
```bash
PGPASSWORD=<jwt_or_password> psql -h tavana.example.com -p 5432 -U user@company.com
```

### DBeaver / Database Clients
- Host: `tavana.example.com`
- Port: `5432`
- User: `user@company.com`
- Password: `<JWT token or password>`

### Tableau
Use PostgreSQL connector with JWT in password field.

### Python (psycopg2)
```python
import psycopg2
conn = psycopg2.connect(
    host="tavana.example.com",
    port=5432,
    user="user@company.com",
    password="<jwt_token>",
    sslmode="require"
)
```

---

## Security Best Practices

1. **Never hardcode credentials** - Use Kubernetes secrets or Azure Key Vault
2. **Enable TLS** - Use `sslmode=require` for all connections
3. **Use short-lived JWTs** - Prefer tokens with < 1 hour expiry
4. **Enable rate limiting** - Protect against brute force attacks
5. **Enable caching** - Reduce load on auth providers

```yaml
auth:
  cache:
    enabled: true
    ttlSecs: 300  # 5 minute cache
  rateLimit:
    enabled: true
    maxFailedAttempts: 5
    banDurationSecs: 300
```

---

## Troubleshooting

### "password authentication failed"

1. Check auth mode: `kubectl get deployment gateway -o yaml | grep AUTH_MODE`
2. If `passthrough`, auth is disabled - check your password is correct format
3. If `required`, ensure provider is configured correctly

### "connection refused to auth provider"

1. Check network policies allow gateway → auth provider
2. Verify auth provider endpoint is correct
3. Check auth provider is healthy

### "token expired"

1. Refresh your JWT token
2. Increase cache TTL if tokens are short-lived


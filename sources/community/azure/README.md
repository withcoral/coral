# Azure

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 3

Query Azure subscriptions, resource groups, and resources from Azure Resource Manager. Monitor cloud infrastructure inventory and configuration through SQL.

## Installation

Install the source via the CLI:

```bash
coral source add --file sources/community/azure/manifest.yaml
```

For persistent access with auto-refresh, use the device code OAuth flow:

```bash
coral source add --interactive --file sources/community/azure/manifest.yaml
```

## Credentials

This source supports two authentication methods:

### Option 1: Device Code OAuth (recommended)

1. Run `coral source add --interactive` and select "Sign in with Azure"
2. Visit `https://microsoft.com/devicelogin` and enter the code shown
3. Sign in with your Azure account
4. Coral stores a refresh token for automatic renewal

Requires a registered Azure AD app (public client). A default app ID is provided, or you can register your own:

```bash
az ad app create --display-name "Coral Azure Source" \
  --public-client-redirect-uris "http://127.0.0.1" \
  --is-fallback-public-client true
```

### Option 2: Manual access token

1. Generate a token: `az account get-access-token --query accessToken -o tsv`
2. Set environment variables:

```bash
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_SUBSCRIPTION_ID="your-subscription-id"
export AZURE_ACCESS_TOKEN="eyJ0eXAi..."
```

**Note:** Manual tokens expire after ~1 hour. Use the device code flow for persistent access.

## Quick Start

```sql
-- List subscriptions
SELECT subscription_id, display_name, state
FROM azure.subscriptions;

-- List resource groups
SELECT name, location, provisioning_state
FROM azure.resource_groups;

-- List all resources
SELECT name, type, location
FROM azure.resources
LIMIT 20;

-- Filter by resource type (within first page of up to 1000)
SELECT name, location
FROM azure.resources
WHERE type = 'Microsoft.App/containerApps';
```

## Tables

### `subscriptions`

Azure subscriptions accessible by the authenticated user. No required filters.

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `subscription_id` | Utf8 | Unique identifier for the subscription |
| `display_name` | Utf8 | Display name of the subscription |
| `state` | Utf8 | Subscription state (Enabled, Disabled, Deleted, PastDue, Warned) |
| `tenant_id` | Utf8 | Azure AD tenant ID |

---

### `resource_groups`

Resource groups in the Azure subscription (first page, up to 1000). Each group is a container for related Azure resources. No required filters.

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `id` | Utf8 | Full resource ID of the resource group |
| `name` | Utf8 | Name of the resource group |
| `location` | Utf8 | Azure region (e.g. eastus, centralindia) |
| `provisioning_state` | Utf8 | Provisioning state (Succeeded, Failed, etc.) |
| `tags` | Json | Tags assigned to the resource group |

---

### `resources`

Resources in the Azure subscription (first page, up to 1000). Includes VMs, storage accounts, container apps, databases, and other services. Subscriptions with more than 1000 resources will return partial results. No required filters.

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `id` | Utf8 | Full resource ID |
| `name` | Utf8 | Name of the resource |
| `type` | Utf8 | Resource type (e.g. Microsoft.App/containerApps) |
| `location` | Utf8 | Azure region |
| `tags` | Json | Tags assigned to the resource |
| `kind` | Utf8 | Resource kind (e.g. app, functionapp) |
| `sku` | Json | SKU/pricing tier of the resource |

## Source scope

- Targets the Azure Resource Manager API at `https://management.azure.com`.
- Supports device code OAuth flow for persistent auth with auto-refresh, or manual access token.
- `AZURE_TENANT_ID` and `AZURE_SUBSCRIPTION_ID` are required as variables.
- `resource_groups` and `resources` use the subscription ID in the URL path.
- API versions are hardcoded in the request paths (2022-12-01 for subscriptions, 2024-03-01 for resource groups and resources).
- 1 declared test query (`subscriptions`) requires no filters.
- Returns the first page of each list endpoint. See Limitations for pagination details.
- Provides read-only access. Creating, updating, or deleting resources is out of scope.

## Limitations

- Manual access tokens expire after ~1 hour. Use the device code OAuth flow for persistent access.
- The default app ID (`d255a859-bceb-450d-bb8f-f23175794825`) is provided for convenience. For production use, register your own Azure AD app.
- Azure ARM list endpoints use `nextLink` URL-based pagination. Coral does not currently support following `nextLink` URLs from JSON response bodies. Each table returns the first page only (typically up to 100-1000 items depending on the endpoint). Subscriptions with more resources than a single page will return partial inventory. Check for truncation by comparing row counts against the Azure Portal.
- Resource details (properties, diagnostics, metrics) are not exposed — only list-level metadata.
- The source queries a single subscription. Multi-subscription queries require adding the source multiple times with different `AZURE_SUBSCRIPTION_ID` values.

## Provider docs

- Azure Resource Manager REST API: https://learn.microsoft.com/en-us/rest/api/resources/
- Subscriptions API: https://learn.microsoft.com/en-us/rest/api/resources/subscriptions/list
- Resource Groups API: https://learn.microsoft.com/en-us/rest/api/resources/resource-groups/list
- Resources API: https://learn.microsoft.com/en-us/rest/api/resources/resources/list

## Live validation output

Validated against a live Azure for Students subscription.

```bash
$ coral source lint sources/community/azure/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/azure/manifest.yaml
Added source azure

  ✓ azure connected successfully

    azure (3 tables)
    ├─ resource_groups
    ├─ resources
    └─ subscriptions
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT subscription_id, display_name, state FROM azure.subscriptions LIMIT 3
      1 row
```

**Live subscriptions proof:**

```sql
SELECT subscription_id, display_name, state FROM azure.subscriptions;
```

```text
+--------------------------------------+--------------------+---------+
| subscription_id                      | display_name       | state   |
+--------------------------------------+--------------------+---------+
| 0e85ba00-0000-0000-0000-4726e1da39a5 | Azure for Students | Enabled |
+--------------------------------------+--------------------+---------+
```

**Live resource_groups proof:**

```sql
SELECT name, location, provisioning_state FROM azure.resource_groups;
```

```text
+------------+--------------+--------------------+
| name       | location     | provisioning_state |
+------------+--------------+--------------------+
| user-rg    | eastus       | Succeeded          |
| user-rg-2  | centralindia | Succeeded          |
+------------+--------------+--------------------+
```

**Live resources proof:**

```sql
SELECT name, type, location FROM azure.resources LIMIT 5;
```

```text
+--------------------------------------+------------------------------------------+--------------+
| name                                 | type                                     | location     |
+--------------------------------------+------------------------------------------+--------------+
| user-law                             | Microsoft.OperationalInsights/workspaces | centralindia |
| user-appi                            | Microsoft.Insights/components            | centralindia |
| user-env                             | Microsoft.App/managedEnvironments        | centralindia |
| Application Insights Smart Detection | microsoft.insights/actiongroups          | global       |
| user-api                             | Microsoft.App/containerApps              | centralindia |
+--------------------------------------+------------------------------------------+--------------+
```

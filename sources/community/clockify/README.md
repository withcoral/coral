# Clockify source

Query Clockify workspaces, projects, clients, users, and tags through SQL.

## Authentication

This source uses a **Personal API Key** to authenticate with the Clockify API.

1. Go to [Clockify Profile Settings](https://app.clockify.me/user/preferences#advanced).
2. Scroll down to the **API** section.
3. Click **Generate** to create a new API key.
4. Copy the generated key.

You also need your **Workspace ID**. You can find this in the URL when you are logged into Clockify and navigating your workspace (e.g., `https://app.clockify.me/workspaces/{workspace_id}/...`), or by initially querying the `clockify.workspaces` table to discover available workspaces.

## Install

```bash
# From the coral repo root:
CLOCKIFY_API_KEY=your_api_key \
CLOCKIFY_WORKSPACE_ID=your_workspace_id \
coral source add --file sources/community/clockify/manifest.yaml

coral source test clockify
```

Or interactively:

```bash
coral source add --interactive --file sources/community/clockify/manifest.yaml
coral source test clockify
```

## Tables

| Table | Description |
|---|---|
| `clockify.workspaces` | List of workspaces the authenticated user belongs to |
| `clockify.projects` | Projects available in the configured workspace |
| `clockify.clients` | Clients available in the configured workspace |
| `clockify.users` | Users belonging to the configured workspace |
| `clockify.tags` | Tags available in the configured workspace |

## Example queries

```sql
-- List all available workspaces to find your Workspace ID
SELECT id, name, hourly_rate__amount, hourly_rate__currency
FROM clockify.workspaces;

-- List all projects in the workspace
SELECT id, name, client_id, billable, public
FROM clockify.projects
ORDER BY name;

-- List all clients
SELECT id, name, address
FROM clockify.clients
ORDER BY name;

-- View all users and their status
SELECT id, name, email, status, active_workspace
FROM clockify.users
ORDER BY name;

-- List all tags
SELECT id, name
FROM clockify.tags
ORDER BY name;
```

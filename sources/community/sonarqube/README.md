# SonarQube Connector for Coral

This is a Coral source manifest for [SonarQube](https://www.sonarqube.org/) and [SonarCloud](https://sonarcloud.io/). It maps SonarQube's REST API into relational, queryable tables, allowing you to easily extract metrics, issues, pull requests, and security hotspots.

## How to Connect & Authenticate

To connect Coral to your SonarQube or SonarCloud instance, you will need a personal User Token.

### Permissions
A User Token with standard project **Browse** access is sufficient for querying projects, measures, issues, and hotspots.
However, be aware of the following permission limitations:
* **`projects` (Self-hosted)**: Querying projects on a self-hosted instance requires **Administer System** permissions.
* **`user_groups` and `users`**: Querying user groups requires **Administer System** permissions. Additionally, standard users may not be able to view certain fields like user email addresses; admin permissions are often required to retrieve full user details.

### 1. Generate a SonarQube Token
1. Log into your SonarQube or SonarCloud instance.
2. Click on your profile picture in the top right corner and select **My Account**.
3. Go to the **Security** tab (in some versions or on SonarCloud, this might be labeled **Tokens**, **Access Tokens**, or found directly on the main account page).
4. Under the "Generate Tokens" section, enter a name (e.g., "Coral Integration"), ensure the type is **User Token**, and click **Generate**.
5. **Copy the generated token immediately**, as you won't be able to see it again.

### 2. Configure Coral
This connector requires your token to be securely passed as a secret input named `SONARQUBE_API_KEY`.

If you are using the **Coral CLI** to test this connector locally, you can provide the key as an environment variable and add the source:

```bash
export SONARQUBE_API_KEY="your-generated-token"
export SONARQUBE_API_BASE="https://sonarcloud.io/api" # Or your self-hosted URL

# Add the source to Coral
coral source add --file sources/community/sonarqube/manifest.yaml

# Test the connection by running a simple query
coral sql "SELECT * FROM sonar.metrics_catalog LIMIT 5"
```

## Available Tables

This manifest provides comprehensive access to the following tables:

| Table Name | Description | Endpoint | Requires Filter | Paginated |
|---|---|---|---|---|
| `project_measures` | Live source code metrics, security vulnerabilities, and bug counts | `/measures/component` | `component`, `metric_keys` | No |
| `projects` | Search projects | `/projects/search` | `organization` (SonarCloud only) | Yes |
| `issues` | Search issues for a project | `/issues/search` | `component_keys` (Cloud) or `components` (Server) | Yes |
| `qualitygates_status` | Quality gate status of a project | `/qualitygates/project_status` | `project_key` | No |
| `hotspots_cloud` | Search security hotspots (SonarCloud) | `/hotspots/search` | `project_key` | Yes |
| `hotspots_server` | Search security hotspots (SonarQube Server) | `/hotspots/search` | `project` | Yes |
| `component_tree` | File-level measures | `/measures/component_tree` | `component`, `metric_keys` | Yes |
| `project_branches` | List branches of a project | `/project_branches/list` | `project` | No |
| `metrics_catalog` | List of all available metrics | `/metrics/search` | *(None)* | No |
| `users` | Search users (Self-hosted only) | `/users/search` | *(None)* | Yes |
| `rules` | Search coding rules | `/rules/search` | `organization` (SonarCloud only) | Yes |
| `project_pull_requests` | List pull requests of a project | `/project_pull_requests/list` | `project` | No |
| `qualityprofiles` | Search quality profiles | `/qualityprofiles/search` | `organization` (SonarCloud only) | No |
| `user_groups` | Search user groups (may require admin privileges) | `/user_groups/search` | `organization` (SonarCloud only) | Yes |

## Features

- **Robust Pagination**: Core list tables (`issues`, `users`, `projects`, etc.) utilize Coral's built-in `page` mode pagination. You can request thousands of rows, and the engine will seamlessly paginate through the `p` and `ps` API parameters behind the scenes.
- **Secure Authentication**: Uses the `secret` input kind to ensure tokens are securely handled and injected as HTTP Bearer tokens.

## Example Queries

Once loaded into Coral with your `SONARQUBE_API_KEY` set in your environment, you can run queries like:

**Get all issues for a specific project (SonarCloud uses `component_keys`, self-hosted uses `components`):**
```sql
SELECT key, message, severity, status
FROM sonar.issues
WHERE component_keys = 'my-project-key';
```

**Find security hotspots (SonarCloud):**
```sql
SELECT message, vulnerability_probability
FROM sonar.hotspots_cloud
WHERE project_key = 'my-project-key';
```

**List users (Self-hosted SonarQube only):**
*(Note: SonarCloud does not expose this endpoint. It is only available for self-hosted SonarQube Server instances.)*
```sql
SELECT login, name, email
FROM sonar.users;
```

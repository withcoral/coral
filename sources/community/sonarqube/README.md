# SonarQube Connector for Coral

This is a Coral source manifest for [SonarQube](https://www.sonarqube.org/) and [SonarCloud](https://sonarcloud.io/). It maps SonarQube's REST API into relational, queryable tables, allowing you to easily extract metrics, issues, pull requests, and security hotspots.

## How to Connect & Authenticate

To connect Coral to your SonarQube or SonarCloud instance, you will need a personal User Token. 

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
coral source add --file manifest.yaml

# Test the connection by running a simple query
coral sql "SELECT * FROM sonar.metrics_catalog LIMIT 5"
```

## Available Tables

This manifest provides comprehensive access to the following tables:

| Table Name | Description | Endpoint | Requires Filter | Paginated |
|---|---|---|---|---|
| `project_measures` | Live source code metrics, security vulnerabilities, and bug counts | `/measures/component` | `component`, `metricKeys` | No |
| `projects` | Search projects by organization | `/projects/search` | `organization` | Yes |
| `issues` | Search issues for a project | `/issues/search` | `projects` | Yes |
| `qualitygates_status` | Quality gate status of a project | `/qualitygates/project_status` | `projectKey` | No |
| `hotspots` | Search security hotspots | `/hotspots/search` | `projectKey` | Yes |
| `component_tree` | File-level measures | `/measures/component_tree` | `component`, `metricKeys` | Yes |
| `project_branches` | List branches of a project | `/project_branches/list` | `project` | No |
| `metrics_catalog` | List of all available metrics | `/metrics/search` | *(None)* | No |
| `users` | Search users | `/users/search` | *(None)* | Yes |
| `rules` | Search coding rules | `/rules/search` | *(None)* | Yes |
| `project_pull_requests` | List pull requests of a project | `/project_pull_requests/list` | `project` | No |
| `qualityprofiles` | Search quality profiles | `/qualityprofiles/search` | *(None)* | No |
| `user_groups` | Search user groups | `/user_groups/search` | *(None)* | Yes |

## Features

- **Robust Pagination**: Core list tables (`issues`, `users`, `projects`, etc.) utilize Coral's built-in `page` mode pagination. You can request thousands of rows, and the engine will seamlessly paginate through the `p` and `ps` API parameters behind the scenes.
- **Secure Authentication**: Uses the `secret` input kind to ensure tokens are securely handled and injected as HTTP Bearer tokens.

## Example Queries

Once loaded into Coral with your `SONARQUBE_API_KEY` set in your environment, you can run queries like:

**Get all issues for a specific project:**
```sql
SELECT key, message, severity, status 
FROM sonar.issues 
WHERE projects = 'my-project-key';
```

**Find security hotspots:**
```sql
SELECT message, vulnerability_probability 
FROM sonar.hotspots 
WHERE project_key = 'my-project-key';
```

**List users:**
```sql
SELECT login, name, email 
FROM sonar.users;
```

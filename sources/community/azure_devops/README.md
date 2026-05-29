# Azure DevOps source

Query Azure DevOps projects, repositories, pull requests, commits, builds,
pipelines, WIQL results, and work items from Coral SQL.

## Credentials

Create a personal access token with read scopes for the Azure DevOps areas you
want to query, then add the source:

```bash
export AZURE_DEVOPS_ORGANIZATION="my-org"
export AZURE_DEVOPS_PAT="..."
coral source add --file sources/community/azure_devops/manifest.yaml
```

PAT authentication uses HTTP Basic auth. Azure DevOps accepts an empty username
with the PAT as the password, so `AZURE_DEVOPS_USERNAME` defaults to an empty
string.

## Start here

```sql
SELECT id, name, state, visibility
FROM azure_devops.projects
ORDER BY name;
```

List repositories:

```sql
SELECT id, name, project__name, default_branch, web_url
FROM azure_devops.repositories
LIMIT 50;
```

Inspect pull requests in a project:

```sql
SELECT pull_request_id, title, status, repository_name, creation_date
FROM azure_devops.pull_requests
WHERE project = 'MyProject'
  AND status = 'active'
LIMIT 50;
```

Run WIQL and fetch matching work item details:

```sql
SELECT id, url
FROM azure_devops.wiql(
  project => 'MyProject',
  query => 'SELECT [System.Id] FROM WorkItems WHERE [System.State] <> ''Closed'''
)
LIMIT 20;
```

```sql
SELECT id, type, title, state, assigned_to, changed_date
FROM azure_devops.work_item
WHERE id = 12345;
```

## Notes

- The source is read-only and avoids mutation endpoints.
- Work item fields vary by process template, so the full field map is exposed
  as JSON in the `fields` column.
- Nested columns use Coral's double-underscore convention, for example
  `project__name` and `created_by__display_name`.

## References

- Azure DevOps REST API patterns: <https://learn.microsoft.com/en-us/azure/devops/integrate/how-to/call-rest-api>
- Projects list API: <https://learn.microsoft.com/en-us/rest/api/azure/devops/core/projects/list>
- Repositories list API: <https://learn.microsoft.com/en-us/rest/api/azure/devops/git/repositories/list>
- WIQL API: <https://learn.microsoft.com/en-us/rest/api/azure/devops/wit/wiql/query-by-wiql>

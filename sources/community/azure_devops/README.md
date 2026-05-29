# Azure DevOps source

Query Azure DevOps projects, repositories, pull requests, commits, builds,
pipelines, WIQL results, and work items from Coral SQL.

## Credentials

Create a personal access token with the read scopes for the Azure DevOps areas
you want to query. The exposed tables use these minimum scopes:

| Surface | Minimum PAT scope |
| --- | --- |
| Projects | `vso.project` |
| Repositories, pull requests, commits | `vso.code` |
| Builds, pipelines | `vso.build` |
| WIQL, work items | `vso.work` |

Then add the source:

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

Inspect recent commits in a repository:

```sql
SELECT commit_id, author__name, comment, author__date
FROM azure_devops.commits
WHERE project = 'MyProject'
  AND repository_id = 'repo-id-here'
ORDER BY author__date DESC
LIMIT 50;
```

Inspect builds and pipelines in a project:

```sql
SELECT id, build_number, status, result, queue_time
FROM azure_devops.builds
WHERE project = 'MyProject'
ORDER BY queue_time DESC
LIMIT 50;
```

```sql
SELECT id, name, folder, revision
FROM azure_devops.pipelines
WHERE project = 'MyProject'
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
- `azure_devops.wiql(...)` supports flat WIQL queries that return
  `workItems`. OneHop and Tree queries return `workItemRelations`, which this
  function does not model.
- Projects, pull requests, and commits use documented offset pagination.
  Builds and pipelines request a bounded first page because Azure DevOps uses
  continuation tokens that are not numeric offsets.
- Work item fields vary by process template, so the full field map is exposed
  as JSON in the `fields` column.
- Nested columns use Coral's double-underscore convention, for example
  `project__name` and `created_by__display_name`.

## Schema overview

| Name | Required filters | Pagination notes |
| --- | --- | --- |
| `azure_devops.projects` | none | Offset pagination with `$skip` and `$top`. |
| `azure_devops.repositories` | none | Single API response. |
| `azure_devops.pull_requests` | `project` | Offset pagination with `$skip` and `$top`. |
| `azure_devops.commits` | `project`, `repository_id` | Offset pagination with `searchCriteria.$skip` and `searchCriteria.$top`. |
| `azure_devops.builds` | `project` | Bounded first page only; Azure uses continuation tokens. |
| `azure_devops.pipelines` | `project` | Bounded first page only; Azure uses continuation tokens. |
| `azure_devops.work_item` | `id` | Single work item lookup. |
| `azure_devops.wiql(...)` | `project`, `query` args | Flat WIQL only. |

## Validation evidence

Static validation run locally:

```bash
coral source lint sources/community/azure_devops/manifest.yaml
make lint-sources
yamllint sources/community/azure_devops/manifest.yaml
git diff --check origin/main..HEAD
gitleaks detect --no-banner --redact --source . --log-opts=origin/main..HEAD
```

Credentialed `coral source add --file`, `coral source test azure_devops`, and
representative live queries require an Azure DevOps organization PAT and were
not run in this workspace.

## References

- Azure DevOps REST API patterns: <https://learn.microsoft.com/en-us/azure/devops/integrate/how-to/call-rest-api>
- Projects list API: <https://learn.microsoft.com/en-us/rest/api/azure/devops/core/projects/list>
- Repositories list API: <https://learn.microsoft.com/en-us/rest/api/azure/devops/git/repositories/list>
- WIQL API: <https://learn.microsoft.com/en-us/rest/api/azure/devops/wit/wiql/query-by-wiql>

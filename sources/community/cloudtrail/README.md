# AWS CloudTrail

Query AWS management event history via
[AWS CloudTrail LookupEvents](https://docs.aws.amazon.com/awscloudtrail/latest/APIReference/API_LookupEvents.html).
Covers the last 90 days of management (control-plane) events — API calls that
create, modify, or delete resources — regardless of whether the caller used
CloudFormation, Terraform, CDK, GitHub Actions, the AWS CLI, or the console.

**Scope:** LookupEvents returns management events only. Data events (S3 object
reads/writes, Lambda invocations, etc.) and network activity events are not
available here; those require a Trail with the appropriate event selectors
and are typically queried via S3 + Athena or CloudTrail Lake.

The 90-day event history is available in every AWS account where CloudTrail is
enabled (default since 2019) with no additional setup or S3 log access required.

## Authentication

AWS CloudTrail uses AWS Signature Version 4. You need an IAM user with
the `cloudtrail:LookupEvents` permission.

### Step 1 — Create an IAM policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["cloudtrail:LookupEvents"],
      "Resource": "*"
    }
  ]
}
```

### Step 2 — Create access keys

Attach the policy to an IAM user and generate an access key pair.

### Step 3 — Add the source

```sh
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_REGION="us-east-1"
coral source add --file sources/community/cloudtrail/manifest.yaml
```

## Tables

| Table | Description | Filters |
|---|---|---|
| `cloudtrail.management_events` | Write-only management events across all AWS services (ReadOnly=false) | `start_time`, `end_time` (optional; when omitted AWS scans the full 90-day window) |
| `cloudtrail.lambda_events` | Lambda function events — includes read and write; filter by `event_name` or `read_only = 'false'` for writes | `start_time`, `end_time` (optional; when omitted AWS scans the full 90-day window) |
| `cloudtrail.cloudformation_events` | CloudFormation stack events — includes read and write; filter by `event_name` or `read_only = 'false'` for mutations | `start_time`, `end_time` (optional; when omitted AWS scans the full 90-day window) |
| `cloudtrail.ec2_events` | EC2 events — includes read and write; filter by `event_name` or `read_only = 'false'` for mutations | `start_time`, `end_time` (optional; when omitted AWS scans the full 90-day window) |

All time filters are **Unix epoch seconds** (Int64). When omitted, AWS uses its own defaults: `start_time` falls back to the earliest event within the last 90 days, `end_time` falls back to the current time. **Always supply explicit `start_time` and `end_time` values** — omitting them causes the effective window to differ between the initial request and each subsequent NextToken page, which can invalidate pagination.

## Example queries

### Find all infrastructure changes in the last 24 hours

```sql
SELECT event_name, event_source, resource_name, username, event_time
FROM cloudtrail.management_events
WHERE start_time = CAST(EXTRACT(EPOCH FROM NOW() - INTERVAL '24 hours') AS BIGINT)
  AND end_time   = CAST(EXTRACT(EPOCH FROM NOW()) AS BIGINT)
ORDER BY event_time DESC
LIMIT 50
```

### Find Lambda changes in the last 24 hours (for cost spike investigation)

Use a rolling window. CloudTrail's `LookupEvents` only returns events from the
last 90 days. A time range that falls entirely outside the 90-day window
returns an empty result (not an error), so events simply age out silently —
keep the filter relative to `NOW()` to stay within bounds.

```sql
SELECT event_name, resource_name, username, event_time, cloudtrail_event
FROM cloudtrail.lambda_events
WHERE start_time = CAST(EXTRACT(EPOCH FROM NOW() - INTERVAL '24 hours') AS BIGINT)
  AND end_time   = CAST(EXTRACT(EPOCH FROM NOW()) AS BIGINT)
ORDER BY event_time DESC
```

### Find CloudFormation deployments and correlate with GitHub PRs

```sql
SELECT
    ct.resource_name  AS stack_name,
    ct.event_name     AS operation,
    ct.event_time     AS deploy_time,
    ct.username       AS deployed_by,
    g.number          AS pr_number,
    g.title           AS pr_title,
    g.user__login     AS pr_author,
    g.merged_at
FROM cloudtrail.cloudformation_events ct
JOIN github.pulls g
    ON g.merged_at <= ct.event_time
    AND g.merged_at >= ct.event_time - INTERVAL '30 minutes'
    AND g.owner = 'your-org'
    AND g.repo = 'your-repo'
    AND g.state = 'closed'
WHERE ct.start_time = CAST(EXTRACT(EPOCH FROM NOW() - INTERVAL '30 days') AS BIGINT)
  AND ct.end_time   = CAST(EXTRACT(EPOCH FROM NOW()) AS BIGINT)
  AND ct.event_name IN ('UpdateStack', 'ExecuteChangeSet', 'CreateStack')
ORDER BY ct.event_time DESC
```

### Inspect what changed (e.g. Lambda memory)

```sql
SELECT
    resource_name,
    event_time,
    username,
    json_get_json(cloudtrail_event, 'requestParameters') AS request_params
FROM cloudtrail.lambda_events
WHERE start_time = CAST(EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days') AS BIGINT)
  AND end_time   = CAST(EXTRACT(EPOCH FROM NOW()) AS BIGINT)
  AND event_name = 'UpdateFunctionConfiguration'
```

### Find new EC2 instances and correlate with cost

```sql
SELECT event_name, resource_name, username, event_time
FROM cloudtrail.ec2_events
WHERE start_time = CAST(EXTRACT(EPOCH FROM NOW() - INTERVAL '30 days') AS BIGINT)
  AND end_time   = CAST(EXTRACT(EPOCH FROM NOW()) AS BIGINT)
  AND event_name = 'RunInstances'
ORDER BY event_time DESC
```

## Notes

- **`management_events` is the only write-only table:** It uses `ReadOnly=false` as its single LookupAttribute. The LookupEvents API accepts only one attribute per request, so the service-scoped tables (`lambda_events`, `cloudformation_events`, `ec2_events`) use `EventSource` as their filter and return both read and write operations. Use `WHERE event_name IN (...)` or `WHERE read_only = 'false'` in your query to restrict to mutations.
- **`event_name` and `read_only` filters are post-fetch (client-side):** For the service-scoped tables, the `EventSource` filter is the only server-side filter. Any `WHERE event_name` or `WHERE read_only` clause is applied locally after Coral retrieves results. The 500-event cap (10 pages × 50) may be reached on reads before the target mutations are fetched — narrow the time window if you need complete mutation coverage.
- **Always supply explicit time filters:** Omitting `start_time` and `end_time` lets AWS choose the window, and that window is resolved independently on the initial request and on every NextToken page. This can cause `InvalidNextTokenException` or silently shift the result set mid-pagination. Use `CAST(EXTRACT(EPOCH FROM NOW() - INTERVAL '24 hours') AS BIGINT)` and pin both values.
- **Time filters are Unix epoch seconds:** Convert with
  `CAST(EXTRACT(EPOCH FROM NOW() - INTERVAL '30 days') AS BIGINT)`.
- **90-day retention:** LookupEvents only returns events from the last 90 days.
  For older data, query CloudTrail logs in S3 via Athena.
- **`InvalidTimeRangeException`:** AWS returns HTTP 400 if `start_time` is after `end_time`, or if the timestamps are otherwise outside the range of values AWS accepts. A valid window that simply falls outside the 90-day retention period is **not** an error — it returns an empty result and ages out silently. Use rolling `NOW()`-relative expressions and keep `start_time` before `end_time`.
- **Rate limit (no auto-retry):** LookupEvents is limited to 2 requests per second
  per account per region. CloudTrail returns `ThrottlingException` as an HTTP **400**
  (not 429), and Coral's rate-limit retry path only triggers on 429, so a throttled
  page surfaces as a hard query error rather than being retried. There is no
  inter-request pacing between pages, so keep time windows narrow to avoid bursts.
- **500-event cap:** Pagination is capped at 10 pages × 50 results = 500 events per query.
  This deliberately low cap keeps a single scan well under the 2 req/s throttling limit.
  If a query needs more than 500 events, Coral stops with a pagination error rather than
  returning a partial result — narrow the time window (e.g. query day-by-day) and page
  through smaller windows.
- **Management events only:** LookupEvents covers management (control-plane) events only. Data events (S3 object reads/writes, Lambda invocations, DynamoDB item operations, etc.) and network activity events are not returned here. For data events, enable a Trail with the appropriate event selectors and query via S3 + Athena or CloudTrail Lake.
- **CloudTrail must be enabled:** Most accounts have CloudTrail enabled by
  default. If not, events will be empty rather than returning an error.
- **`cloudtrail_event` is a JSON column:** Use `json_get_json` to extract nested
  objects like `requestParameters`. Use `json_get_str` for scalar string fields.
  The `requestParameters` object shows what values were set. The
  `clientRequestToken` field (when set by CI/CD) can be a direct foreign key to
  the commit or PR that triggered the change: `json_get_str(cloudtrail_event, 'requestParameters', 'clientRequestToken')`.
- **Empty `resource_name` on some events:** Events like `ConsoleLogin`,
  `GetSigninToken`, and service-linked role creation do not target a specific
  resource. For these, the `Resources` array is empty and `resource_name` and
  `resource_type` will be `null` — this is expected behavior, not a query error.
- **Only the first resource is surfaced as a column:** `resource_name` and
  `resource_type` map the first entry of the event's `Resources` array
  (`Resources[0]`). Some events reference several resources — for example
  `CreateDefaultVpcResourceCreation` can list a VPC's subnets, route tables,
  and gateways together. To see every referenced resource, read the full
  array with `json_get_json(cloudtrail_event, 'resources')` rather than
  relying on the single `resource_name`/`resource_type` columns.
- **One region per workspace:** Each region has its own CloudTrail event
  history. This source queries the single region in `AWS_REGION`. The source
  name is fixed as `cloudtrail`, so re-adding the manifest with a different
  `AWS_REGION` **replaces** the existing source rather than creating a second
  regional copy — `coral source add` rejects a custom name when `--file` is
  used. To query another region, change `AWS_REGION` and re-add, or maintain
  separate workspaces per region.
- **Global-service events (IAM / CloudFront) live in us-east-1:** As of
  November 22, 2021, AWS records IAM and CloudFront events in the Region where
  they occur, which is `us-east-1`. Use `AWS_REGION=us-east-1` when you are
  looking for identity, access-key, IAM policy, or CloudFront changes; other
  regions will not surface them.
- **AWS STS events depend on the endpoint:** Calls to the global STS endpoint
  (`sts.amazonaws.com`) are recorded in `us-east-1`, while calls to a regional
  STS endpoint (e.g. `sts.us-west-2.amazonaws.com`) are recorded in that
  region. Query the region matching the endpoint your workloads use; if you
  rely on regional STS endpoints, `us-east-1` alone will miss those events.

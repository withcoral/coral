# GCP Billing Budgets

Query GCP Cloud Billing budgets from the
[Cloud Billing Budgets API (v1)](https://cloud.google.com/billing/docs/reference/budget/rest).

This source stands on its own: pass a billing account resource name as a
**literal** argument to
`budgets(billing_account => 'billingAccounts/…')`. Table function arguments must
be string literals; column values from joins or subqueries are not supported.

## Prerequisites

- **`GCP_USER_PROJECT` (required).** Every API request sends this project as
  `x-goog-user-project` for quota attribution. Enable the Budget API on it and
  grant the caller `serviceusage.services.use` on that project (for example
  `roles/serviceusage.serviceUsageConsumer`):
  ```sh
  gcloud services enable billingbudgets.googleapis.com --project=my-project-id
  gcloud projects add-iam-policy-binding my-project-id \
    --member="user:you@example.com" \
    --role="roles/serviceusage.serviceUsageConsumer"
  ```
- **Billing-account IAM** (typical setup): `billing.budgets.list` and
  `billing.budgets.get` on each billing account you query (`roles/billing.viewer`).
  List uses `.list`; `get_budget` uses `.get`.
- **Project IAM** (single-project budgets only): `billing.resourcebudgets.read` and
  `resourcemanager.projects.get` on the project (`roles/viewer`, `roles/editor`, or
  `roles/owner`). You still pass `billing_account` in queries. See
  [Budget API access control](https://cloud.google.com/billing/docs/how-to/budget-api-access-control).
- A billing account resource name in the form `billingAccounts/{id}` (from the
  [GCP Console](https://console.cloud.google.com/billing) or another data source).

## Authentication

The Budgets API accepts an OAuth 2.0 Bearer token with either
[`https://www.googleapis.com/auth/cloud-billing`](https://cloud.google.com/billing/docs/reference/budget/rest/v1/billingAccounts.budgets/list)
or `https://www.googleapis.com/auth/cloud-platform`. The readonly billing scope
(`cloud-billing.readonly`) is **not** accepted.

Two setup paths are supported.

> **Access and refresh tokens (interactive OAuth).** Google access tokens are
> short-lived (about one hour). For Desktop (installed) OAuth clients, Google's
> authorization-code exchange always includes a `refresh_token` — see
> [OAuth 2.0 for iOS & Desktop Apps](https://developers.google.com/identity/protocols/oauth2/native-app).
> Coral refreshes stored OAuth credentials automatically when a refresh token is
> available. The authorization URL uses `prompt=consent` so Google always shows the
> consent screen and issues a fresh refresh token on reconnect.
> If the OAuth consent screen publishing status is **Testing**, refresh tokens
> expire after 7 days unless the app is published to Production — see
> [Refresh token expiration](https://developers.google.com/identity/protocols/oauth2#expiration).
> Re-run `coral source add --interactive` to reconnect when tokens are revoked or
> missing.

### Option 1 — OAuth 2.0 (recommended)

#### Step 1 — Enable the Cloud Billing Budget API

In [Google Cloud Console](https://console.cloud.google.com/), open the project that will
host your OAuth client. Navigate to **APIs & Services → Library**, search for **Cloud
Billing Budget API**, and click **Enable**.

#### Step 2 — Create a Desktop OAuth 2.0 client

Navigate to **APIs & Services → Credentials → Create credentials → OAuth client ID**. Set
the application type to **Desktop app** (not Web application). Google only allows loopback
redirects for Desktop clients; you typically do not register an exact redirect URI in the
console for this app type.

Coral uses a random loopback port and callback path (`http://127.0.0.1:<port>/oauth/callback`).
You do not need to pre-register that URI. If the browser cannot reach Coral's listener (SSH,
VM, or split browser), paste the final `http://127.0.0.1:...` redirect URL from the address
bar when the CLI prompts you.

Note your **Client ID** and **Client secret**. You will enter these during
`coral source add --interactive`.

> **Refresh tokens.** When the consent screen is in Testing mode, add your Google
> account under **Test users**. The manifest authorization URL uses `prompt=consent`
> so Google always shows the consent screen and issues a fresh refresh token — see
> [OAuth 2.0 for Desktop Apps](https://developers.google.com/identity/protocols/oauth2/native-app).

#### Step 3 — Grant billing.budgets.list and billing.budgets.get

See [Permissions](#permissions) below.

#### Step 4 — Add the source

```sh
export GCP_USER_PROJECT="my-project-id"
coral source add --interactive --file sources/community/gcp_billing_budgets/manifest.yaml
```

When prompted, enter your Desktop OAuth Client ID and Client Secret, set
`GCP_USER_PROJECT` if not already exported, then follow the
**Connect with Google** prompt. Coral opens a browser for the OAuth consent flow, then
stores the access token. If your browser cannot reach Coral's loopback listener (for example
when running Coral over SSH or inside a VM), paste the final `http://127.0.0.1:...` redirect
URL into the terminal when prompted.

> **Consent screen.** Google shows permission to manage billing and cost management
> services. That corresponds to the full `cloud-billing` scope required by the Budgets API.

### Option 2 — Paste access token

Generate a short-lived token with `gcloud`. The Budgets API accepts
`cloud-billing` or `cloud-platform`; `gcloud auth print-access-token --scopes`
only allows a fixed allowlist that includes `cloud-platform` but not
`cloud-billing`.

For a user account (sign in first if needed):

```sh
gcloud auth login
gcloud auth print-access-token \
  --scopes=https://www.googleapis.com/auth/cloud-platform
```

For a service account (also grant it `roles/billing.viewer` on the billing
account and `roles/serviceusage.serviceUsageConsumer` on `GCP_USER_PROJECT` —
see [Permissions](#permissions)):

```sh
gcloud auth activate-service-account --key-file=key.json
gcloud auth print-access-token \
  --scopes=https://www.googleapis.com/auth/cloud-platform
```

Then add the source:

```sh
export GCP_ACCESS_TOKEN="$(gcloud auth print-access-token \
  --scopes=https://www.googleapis.com/auth/cloud-platform)"
export GCP_USER_PROJECT="my-project-id"
coral source add --file sources/community/gcp_billing_budgets/manifest.yaml
```

The pasted token expires after 1 hour. Re-export and re-add when it expires.

## Permissions

The Budgets API is **free**. There is no per-request charge.

| IAM role | Role ID | Includes `billing.budgets.list` / `billing.budgets.get`? | Notes |
|---|---|---|---|
| Billing Account Viewer | `roles/billing.viewer` | Yes (view-only) | **Recommended** for this source |

`roles/billing.viewer` includes the
[`billing.budgets.list`](https://cloud.google.com/billing/docs/reference/budget/rest/v1/billingAccounts.budgets/list)
and
[`billing.budgets.get`](https://cloud.google.com/billing/docs/reference/budget/rest/v1/billingAccounts.budgets/get)
permissions this source needs. Write-capable roles such as
`roles/billing.costsManager` or `roles/billing.admin` are not required for
read-only queries.

### Troubleshooting `PERMISSION_DENIED`

Before changing IAM roles, confirm:

1. **`billing_account` format** — use the full resource name
   `billingAccounts/{id}`, not the short ID alone (HTTP 400).
2. **`GCP_USER_PROJECT`** — `billingbudgets.googleapis.com` enabled and the caller
   has `serviceusage.services.use` on that project
   (`roles/serviceusage.serviceUsageConsumer`).
3. **OAuth scope** — `cloud-billing` or `cloud-platform`; not
   `cloud-billing.readonly`.
4. **Billing-account IAM** — `roles/billing.viewer` on the billing account for
   list/get, or project IAM for single-project budgets (see below).
5. **Principal match** — the IAM binding `member` matches the authenticated user
   or service account.

### Project-level access (single-project budgets)

For budgets scoped to one project, Google allows project IAM instead of
billing-account roles ([access control](https://cloud.google.com/billing/docs/how-to/budget-api-access-control)):

| Project role | Role ID | Includes `billing.resourcebudgets.read` and `resourcemanager.projects.get`? |
|---|---|---|
| Project Viewer | `roles/viewer` | Yes (read budgets scoped to the project) |
| Project Editor | `roles/editor` | Yes |
| Project Owner | `roles/owner` | Yes |

This path
applies to single-project budgets; listing all budgets on a billing account still
requires `billing.budgets.list` on the account.

Grant `roles/billing.viewer` on a specific billing account:

```sh
gcloud billing accounts add-iam-policy-binding BILLING_ACCOUNT_ID \
  --member="user:you@example.com" \
  --role="roles/billing.viewer"
```

Replace `BILLING_ACCOUNT_ID` with the short ID (e.g. `012345-567890-ABCDEF`) or the
full resource name — both forms are accepted by `gcloud`. Use `serviceAccount:...`
for service accounts. Coral SQL still requires the `billingAccounts/{id}` prefix in
query arguments.

### GCP project for quota attribution

`GCP_USER_PROJECT` is **required** on every request. It sets the
[`x-goog-user-project`](https://cloud.google.com/apis/docs/system-parameters) header so
Google can attribute API quota to a project. This project does not need to be linked to the
billing account being queried.

1. Enable the Budget API on the project:
   ```sh
   gcloud services enable billingbudgets.googleapis.com --project=my-project-id
   ```
2. Grant the caller `serviceusage.services.use` on that project (included in
   `roles/serviceusage.serviceUsageConsumer`). Without this permission, requests often fail
   with `PERMISSION_DENIED` even when billing-account IAM is correct:
   ```sh
   gcloud projects add-iam-policy-binding my-project-id \
     --member="user:you@example.com" \
     --role="roles/serviceusage.serviceUsageConsumer"
   ```

## Tables

| Table / Function | Description | Arguments |
|---|---|---|
| `gcp_billing_budgets.budgets(billing_account)` | Cloud Billing budgets for a billing account | `billing_account` (required), `scope` (optional project filter) |
| `gcp_billing_budgets.get_budget(name)` | One budget by full resource name | `name` (required) |

## Columns

`budgets` and `get_budget` expose the same [Budget](https://cloud.google.com/billing/docs/reference/budget/rest/v1/billingAccounts.budgets#Budget)
fields. `budgets` adds virtual `billing_account` (the function argument). For the same
budget, list and get return identical column values.

### Usually present on successful reads

| Column | Type | Notes |
|---|---|---|
| `name` | Utf8 | Full resource name `billingAccounts/{id}/budgets/{budgetId}` |
| `display_name` | Utf8 | Console display name |
| `etag` | Utf8 | Concurrency token; often present |
| `amount` | Json | `specifiedAmount` or `lastPeriodAmount` union |
| `amount_type` | Utf8 | Derived: `SPECIFIED` or `LAST_PERIOD` |
| `specified_amount_units` | Utf8 | Fixed budget amount (string int64); NULL for last-period budgets |
| `specified_amount_currency_code` | Utf8 | e.g. `USD`, `INR`; NULL for last-period budgets |
| `calendar_period` | Utf8 | `MONTH`, `QUARTER`, `YEAR`, … for recurring budgets |
| `credit_types_treatment` | Utf8 | How credits affect threshold spend |
| `threshold_rules` | Json | Alert thresholds; `thresholdPercent` is 0.0–1.0 |

### Often NULL or empty (optional API fields)

| Column | When absent | Meaning |
|---|---|---|
| `ownership_scope` | Blank on many budgets | Optional enum; older budgets often omit it |
| `filter_projects` | Empty | All projects on the billing account |
| `filter_resource_ancestors` | Empty | Not scoped to folders/orgs |
| `filter_services` | Empty | All services |
| `filter_credit_types` | Empty | Normal unless `credit_types_treatment` is `INCLUDE_SPECIFIED_CREDITS` |
| `filter_subaccounts` | Empty | Parent account and all subaccounts |
| `filter_labels` | Empty | No label filter |
| `filter_custom_period` | Empty | Budget uses `calendar_period` instead |
| `notifications_rule` | `{}` or partial JSON | GCP omits unset keys; only configured delivery fields appear |

### API constraints worth remembering

- `calendar_period` and `filter_custom_period` are mutually exclusive ([Filter usage_period](https://cloud.google.com/billing/docs/reference/budget/rest/v1/billingAccounts.budgets#Filter)).
- `lastPeriodAmount` budgets cannot use `filter_custom_period` ([BudgetAmount](https://cloud.google.com/billing/docs/reference/budget/rest/v1/billingAccounts.budgets#BudgetAmount)).
- `spendBasis: FORECASTED_SPEND` in `threshold_rules` applies only with `calendar_period`, not custom periods ([Basis](https://cloud.google.com/billing/docs/reference/budget/rest/v1/billingAccounts.budgets#Basis)).
- `enableProjectLevelRecipients` in `notifications_rule` applies only when `filter_projects` contains exactly one project ([NotificationsRule](https://cloud.google.com/billing/docs/reference/budget/rest/v1/billingAccounts.budgets#NotificationsRule)).

## gcp_billing_budgets.budgets(billing_account, scope)

Lists Cloud Billing budgets for a billing account. Pass the full resource name
`billingAccounts/{id}` as `billing_account`. Optionally pass `scope` as a project resource
name (`projects/my-project-id` or `projects/123456789`) to return only budgets that track
that project — see
[`billingAccounts.budgets.list`](https://cloud.google.com/billing/docs/reference/budget/rest/v1/billingAccounts.budgets/list).

Requires `billing.budgets.list` (included in `roles/billing.viewer`). Issues one API call per
invocation (plus pagination). Each invocation follows `nextPageToken` for up to 50 pages of
100 budgets (5,000 rows). Billing accounts with more budgets return a truncated list — see
[Budget API quotas](https://cloud.google.com/billing/docs/how-to/budget-api).

> **Argument format.** The `billing_account` argument must be the full resource name:
> `billingAccounts/012345-567890-ABCDEF`. Passing the ID portion alone
> (`012345-567890-ABCDEF`) is invalid and returns HTTP 400.

> **API WARNING** (from GCP documentation): Some fields visible in the Google Cloud Console
> are not available through the Budgets API. The `notifications_rule` JSON column exposes
> what the API returns; additional Console-only alert settings may still be missing.

## gcp_billing_budgets.get_budget(name)

Fetches one budget by full resource name via
[`billingAccounts.budgets.get`](https://cloud.google.com/billing/docs/reference/budget/rest/v1/billingAccounts.budgets/get).
Pass `name` as `billingAccounts/{billingAccountId}/budgets/{budgetId}` — the same value
returned in the `name` column from `budgets(billing_account => ...)`.

Requires `billing.budgets.get` on the billing account (`roles/billing.viewer`), or
for single-project budgets, `billing.resourcebudgets.read` and
`resourcemanager.projects.get` on the project (`roles/viewer`, `roles/editor`, or
`roles/owner`). Returns exactly one row
when the budget exists, with the same columns as `budgets` (except `billing_account`).
HTTP 404 for an unknown or deleted budget name returns **zero rows**, not an error.

```sql
SELECT display_name, calendar_period, specified_amount_units, threshold_rules
FROM gcp_billing_budgets.get_budget(
    name => 'billingAccounts/012345-567890-ABCDEF/budgets/abc123-def456'
)
```

Use the `name` value from a `budgets(...)` row when drilling into one budget.

### List all budgets for one billing account

```sql
SELECT
    display_name,
    calendar_period,
    CASE
        WHEN specified_amount_units IS NOT NULL
        THEN 'Fixed: ' || specified_amount_units || ' ' || specified_amount_currency_code
        ELSE 'Last period spend'
    END AS budget_amount,
    credit_types_treatment
FROM gcp_billing_budgets.budgets(billing_account => 'billingAccounts/012345-567890-ABCDEF')
ORDER BY display_name
```

### List budgets for one project (API filter)

```sql
SELECT display_name, calendar_period, specified_amount_units
FROM gcp_billing_budgets.budgets(
    billing_account => 'billingAccounts/012345-567890-ABCDEF',
    scope => 'projects/my-project-id'
)
ORDER BY display_name
```

### Expand threshold rules — one row per threshold

`thresholdPercent` is a float from `0.0` to `1.0`, where `0.9` means 90%. Multiply by 100
for display.

```sql
SELECT
    b.display_name,
    b.calendar_period,
    json_get_float(t, 'thresholdPercent') * 100  AS alert_pct,
    json_get_str(t, 'spendBasis')                AS spend_basis
FROM (
    SELECT display_name, calendar_period, unnest(json_get_array(threshold_rules)) AS t
    FROM gcp_billing_budgets.budgets(billing_account => 'billingAccounts/012345-567890-ABCDEF')
) b
ORDER BY b.display_name, alert_pct
```

### Query multiple billing accounts

Table function arguments must be **string literals**. To list budgets for more
than one billing account, run one literal call per account and combine with
`UNION ALL`:

```sql
SELECT display_name, calendar_period, specified_amount_units
FROM gcp_billing_budgets.budgets(billing_account => 'billingAccounts/012345-567890-ABCDEF')
UNION ALL
SELECT display_name, calendar_period, specified_amount_units
FROM gcp_billing_budgets.budgets(billing_account => 'billingAccounts/FEDCBA-098765-543210')
ORDER BY display_name
```

Column values and subquery results cannot be used as `billing_account` arguments.

### Inspect project filters on returned budgets

```sql
SELECT
    display_name,
    p AS project,
    specified_amount_units
FROM (
    SELECT
        display_name,
        specified_amount_units,
        unnest(json_get_array(filter_projects)) AS p
    FROM gcp_billing_budgets.budgets(billing_account => 'billingAccounts/012345-567890-ABCDEF')
)
WHERE p IS NOT NULL
ORDER BY display_name, project
```

Budgets with no project filter return `filter_projects` NULL or empty and are omitted by
the `WHERE p IS NOT NULL` clause above.

### Smoke test (requires credentials)

After adding the source with a valid token and `GCP_USER_PROJECT`, confirm API wiring:

```sql
SELECT name, display_name
FROM gcp_billing_budgets.budgets(
    billing_account => 'billingAccounts/YOUR-BILLING-ACCOUNT-ID'
)
LIMIT 5
```

Replace the billing account resource name with one from the
[GCP Console](https://console.cloud.google.com/billing).

## Notes

- **Free API.** The Cloud Billing Budget API has no per-request charge.
- **OAuth scope.** Accepts `https://www.googleapis.com/auth/cloud-billing` or
  `https://www.googleapis.com/auth/cloud-platform`. Readonly `cloud-billing.readonly`
  tokens are rejected. For `gcloud auth print-access-token --scopes`, use
  `cloud-platform` (gcloud does not allow `cloud-billing` in `--scopes`).
- **Token expiry.** Interactive OAuth: Coral refreshes stored credentials when Google
  returns a refresh token; Testing-mode refresh tokens expire after 7 days. The
  authorization URL uses `prompt=consent` so Google issues a fresh token on every
  reconnect. Pasted `gcloud` tokens expire after ~1 hour; re-export and re-add.
- **`thresholdPercent` is 0.0–1.0.** Multiply by 100 before displaying as a percentage.
- **List/get parity.** `budgets` and `get_budget` return the same Budget field values for a
  given budget; prefer list when scanning an account, get when you already have `name`.
- **Union amount type.** Fixed amounts use `specifiedAmount`; otherwise budgets track last
  calendar period spend (`lastPeriodAmount`). `lastPeriodAmount` cannot combine with
  `filter_custom_period`.
- **Literal table-function args.** `budgets(billing_account => …)` and
  `get_budget(name => …)` require string literals; use `UNION ALL` across accounts
  rather than passing column values from joins.
- **`GCP_USER_PROJECT` (required).** Sets `x-goog-user-project` on every request. Enable
  `billingbudgets.googleapis.com` on that project and grant the caller
  `serviceusage.services.use` there.
- **List pagination cap.** `budgets(...)` returns at most 5,000 rows per invocation (50
  pages × 100 budgets).
- **No spend actuals.** Budget configuration and thresholds only — not current spend. Use
  BigQuery billing export or other cost tooling for usage amounts.
- **`ownership_scope`.** Often blank; the API may omit it on older budgets.
- **`notifications_rule`.** Sparse JSON — `{}` is valid. `enableProjectLevelRecipients` only
  applies when the budget filters to exactly one project.
- **`FORECASTED_SPEND`.** Only valid in `threshold_rules` when the budget uses
  `calendar_period`, not `filter_custom_period`.

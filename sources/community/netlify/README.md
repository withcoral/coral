# Netlify Connector

This source queries the [Netlify REST API](https://docs.netlify.com/api/get-started/)
to expose sites, deploys, forms, submissions, environment variables, DNS zones,
and accounts as queryable tables.

## Auth

Requires a `NETLIFY_ACCESS_TOKEN` credential. Generate a personal access token
from [app.netlify.com/user/applications](https://app.netlify.com/user/applications)
under **Personal access tokens**, then add the source:

```bash
coral source add --file manifest.yaml
```

## Start querying

List all sites in your account:

```sql
SELECT id, name, custom_domain, state, plan, account_name
FROM netlify.sites
ORDER BY updated_at DESC
LIMIT 20;
```

Review recent production deploys for a site:

```sql
SELECT id, state, branch, commit_ref, title, created_at, published_at
FROM netlify.deploys
WHERE site_id = '<your-site-id>'
  AND context = 'production'
ORDER BY created_at DESC
LIMIT 10;
```

List forms and their submission counts for a site:

```sql
SELECT id, name, submission_count, created_at
FROM netlify.forms
WHERE site_id = '<your-site-id>'
ORDER BY submission_count DESC;
```

Read form submissions for a site:

```sql
SELECT id, email, name, summary, created_at
FROM netlify.submissions
WHERE site_id = '<your-site-id>'
ORDER BY created_at DESC
LIMIT 50;
```

List environment variables for a site:

```sql
SELECT key, is_secret, scopes, updated_at
FROM netlify.env_vars
WHERE site_id = '<your-site-id>';
```

List DNS zones managed by Netlify:

```sql
SELECT id, name, domain, account_name, dedicated, ipv6_enabled
FROM netlify.dns_zones;
```

List all accounts you belong to:

```sql
SELECT id, name, slug, type_name, billing_period, created_at
FROM netlify.accounts;
```

## Tables

### sites

All sites you have access to. No filter required. Maps to `GET /sites`.
Paginated via Link header with up to 100 items per page.

### deploys

Deploy history for a specific site. Requires `site_id`. Maps to
`GET /sites/{site_id}/deploys`. Optional filters: `branch`, `state`.

### forms

Form definitions and submission counts for a specific site. Requires
`site_id`. Maps to `GET /sites/{site_id}/forms`.

### submissions

Individual form submissions for a specific site. Requires `site_id`. Maps to
`GET /sites/{site_id}/submissions`. Paginated via Link header.

### env_vars

Environment variables configured for a specific site. Requires `site_id`.
Maps to `GET /sites/{site_id}/env`. Optional filters: `context_name`, `scope`.

### dns_zones

DNS zones managed by Netlify. No filter required. Maps to `GET /dns_zones`.
Optional filter: `account_slug` to scope to a specific team.

### accounts

Teams and accounts you are a member of. No filter required. Maps to
`GET /accounts`.

## Rate limiting

The Netlify API allows up to 500 requests per minute. This source requests
100 items per page to minimise API calls during pagination.

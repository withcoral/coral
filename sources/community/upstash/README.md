# Upstash

Query Redis databases, database statistics, and Vector indexes from Upstash.

## Setup

### Get Your API Key

1. Log in to the [Upstash Console](https://console.upstash.com)
2. Navigate to **Account → Management API**
3. Click **Create API Key** and copy the key
4. Note your account email address — it is used as the Basic auth username

### Add the Source

```bash
coral source add --file sources/community/upstash/manifest.yaml
```

When prompted, provide:

- `UPSTASH_EMAIL` — your Upstash account email
- `UPSTASH_API_KEY` — the Management API key you created above

## Tables

### `redis_databases`

Lists all Redis databases for the authenticated account. Returns
configuration, resource limits, region topology, and plan details.
This is the discovery table — use `database_id` to query stats.

**Useful for:**

- Redis database inventory across your account
- Identifying database regions, plans, and resource tiers
- Auditing TLS, eviction, and consistency settings
- Getting database IDs for deeper stats queries

### `redis_database_stats`

Returns detailed usage statistics for a specific Redis database.
Includes daily command counts, monthly request and bandwidth totals,
storage usage, and billing information.

**Requires:** `database_id` filter (from `redis_databases`)

> **Note:** Time-series fields like `connection_count`, `keyspace`,
> `throughput`, `latency_mean`, `hits`, `misses`, and `bandwidths` are returned as JSON
> arrays of `{x, y}` data points. Use `json_get_*` functions to
> extract individual values.

**Useful for:**

- Monitoring daily and monthly usage
- Tracking billing costs per database
- Analyzing latency and throughput trends
- Capacity planning with storage metrics

**Example:**

```sql
SELECT database_id, daily_net_commands,
       total_monthly_requests, total_monthly_billing,
       current_storage
FROM upstash.redis_database_stats
WHERE database_id = 'your-database-id';
```

### `vector_indexes`

Lists all Upstash Vector indexes for the authenticated account.
Returns index configuration including name, region, dimension_count,
similarity function, and plan details.

**Useful for:**

- Vector index inventory
- Reviewing dimension_count and similarity function settings
- Checking rate limits and quotas per index
- Identifying index regions and plans

## Authentication

The source uses HTTP Basic authentication. Your Upstash account email
is the username and the Management API key is the password. The API
key is sent as a `secret` input and never exposed.

To generate a Base64-encoded credential for manual testing:

```bash
echo -n "you@example.com:your-api-key" | base64
```

## Limits

- The Developer API returns all items in a single response (no
  pagination). Accounts with many databases may see larger payloads.
- `redis_database_stats` requires a `database_id` filter — it
  queries a single database at a time.
- Time-series statistics fields are returned as JSON arrays, not
  flattened columns. Use `json_get_*` SQL functions to extract values.

## Example Queries

### List all Redis databases with their plan and region

```sql
SELECT database_id, database_name, region, state, type, budget
FROM upstash.redis_databases;
```

### Find databases with eviction enabled

```sql
SELECT database_name, region, eviction, db_memory_threshold
FROM upstash.redis_databases
WHERE eviction = true;
```

### Check monthly billing for a database

```sql
SELECT database_id, total_monthly_billing,
       total_monthly_requests, current_storage
FROM upstash.redis_database_stats
WHERE database_id = 'your-database-id';
```

### Audit daily usage

```sql
SELECT database_id, daily_net_commands,
       daily_read_requests, daily_write_requests,
       daily_bandwidth
FROM upstash.redis_database_stats
WHERE database_id = 'your-database-id';
```

### List all Vector indexes

```sql
SELECT name, region, dimension_count, similarity_function,
       type, max_vector_count
FROM upstash.vector_indexes;
```

## Notes

- All endpoints are under `https://api.upstash.com/v2/` (no pagination)
- The `redis_databases` table is the discovery table — its
  `database_id` column is the required filter for
  `redis_database_stats`
- `redis_database_stats` uses `row_strategy: direct` because the
  endpoint returns a single stats object, not an array
- Timestamps use `format_timestamp` with `seconds` input to convert
  Unix epoch seconds to proper Timestamp columns
- Security add-ons (`securityAddons`) are preserved as a Json column
  — use `json_get_bool(security_addons, 'vpcPeering')` to inspect
- Region arrays (`primary_members`, `all_members`, `read_regions`)
  are preserved as Json columns

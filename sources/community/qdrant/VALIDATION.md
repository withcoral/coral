# Qdrant live validation

Executed on 2026-10-08 using Qdrant **1.19.2** (official macOS arm64
binary), a Coral debug CLI built from this change, and a fresh isolated
workspace. This is local server evidence, not a Qdrant Cloud or distributed
cluster certification.

Manifest SHA-256: `10ab2b21e9e6cbc164c0ac418930caad53f684339a818d8763b7148c14717c96`.

## Fixture

A new local collection, `coral_pr_audit`, used four-dimensional Cosine vectors:

- Numeric IDs 1 through 205: vector `[1, id / 205, 0.1, 0.2]`.
- UUID `550e8400-e29b-41d4-a716-446655440000`: vector `[1, 0.3, 0.1, 0.2]`.
- Odd numeric IDs and the UUID had payload `city: Berlin`; even IDs used `Paris`.
- Numeric payload names were `point-<id>`; the UUID name was `uuid-point`.

The server listened only on `127.0.0.1:16333`. Inputs were
`QDRANT_HOST=http://127.0.0.1:16333` and the dummy `QDRANT_API_KEY=none`.
The fresh Coral configuration used `[credentials] storage = "file"`, so no
system-keychain credentials were needed. `coral source lint`, `source add`,
and `source test qdrant` all succeeded.

## Observed results

| Check | Result |
| --- | --- |
| Collection listing and details | One collection; status green; 206 points |
| Scroll without SQL LIMIT | All 206 unique IDs matched the fixture; three HTTP 200 scroll requests (100, 100, 6 rows) |
| Mixed integer/UUID lookup | IDs 1, 205 and the UUID returned |
| Table and function search | Five rows each, all scores at least 0.9 |
| Object payload filter | Three Berlin points returned |
| Table and function recommendation | Three rows each; IDs 2, 3, 4 |
| Target-only discovery | Three rows returned |
| Context-only discovery | Three rows returned |
| Discovery without either input | Rejected with an at-least-one-filter error |
| Exact point count | 206 |
| Virtual JSON input columns | IDs, target and positive inputs returned as original text, without an extra JSON string encoding |
| Collection cluster info | One active local shard, 206 points |
| Cluster and telemetry | Cluster disabled as expected for a single node; telemetry reports Qdrant 1.19.2 |
| Aliases and snapshots | Empty arrays on the new fixture |
| Node locks | Qdrant 1.19.2 returns HTTP 404; the documented `allow_404_empty` fallback returns zero rows |

The locks payload, populated snapshot/alias results, a distributed cluster,
and authenticated Cloud deployment were not exercised.

## Representative command output

These are captured stdout values from successful `coral sql --format json`
commands below. The mixed-ID result retains the CLI JSON column encoding.

```sql
SELECT id FROM qdrant.points_by_id WHERE collection_name = 'coral_pr_audit' AND ids = '[1,205,"550e8400-e29b-41d4-a716-446655440000"]';
```

```json
[{"id":"1"},{"id":"205"},{"id":"\"550e8400-e29b-41d4-a716-446655440000\""}]
```

```sql
SELECT count FROM qdrant.points_count WHERE collection_name = 'coral_pr_audit' AND exact = true;
```

```json
[{"count":206}]
```

```sql
SELECT ids FROM qdrant.points_by_id WHERE collection_name = 'coral_pr_audit' AND ids = '[1,205]';
```

```json
[{"ids":"[1,205]"},{"ids":"[1,205]"}]
```

## Executed SQL

These queries ran with `coral sql --format json`. The final query is expected
to fail; the others exited successfully.

```sql
SELECT name FROM qdrant.collections;

SELECT status, points_count FROM qdrant.collection_details WHERE collection_name = 'coral_pr_audit';

SELECT id FROM qdrant.points WHERE collection_name = 'coral_pr_audit';

SELECT id FROM qdrant.points_by_id WHERE collection_name = 'coral_pr_audit' AND ids = '[1,205,"550e8400-e29b-41d4-a716-446655440000"]';

SELECT id, score FROM qdrant.points_search WHERE collection_name = 'coral_pr_audit' AND vector = '[1,0.3,0.1,0.2]' AND limit = 5 AND score_threshold = 0.9;

SELECT id, score FROM qdrant.search_points(collection_name => 'coral_pr_audit', vector => '[1,0.3,0.1,0.2]', limit => 5, score_threshold => 0.9);

SELECT id FROM qdrant.points_search WHERE collection_name = 'coral_pr_audit' AND vector = '[1,0.3,0.1,0.2]' AND filter = '{"must":[{"key":"city","match":{"value":"Berlin"}}]}' AND limit = 3;

SELECT id, score FROM qdrant.points_recommend WHERE collection_name = 'coral_pr_audit' AND positive = '[1]' AND limit = 3;

SELECT id, score FROM qdrant.recommend_points(collection_name => 'coral_pr_audit', positive => '[1]', limit => 3);

SELECT id, score FROM qdrant.points_discover WHERE collection_name = 'coral_pr_audit' AND target = '[1,0.3,0.1,0.2]' AND limit = 3;

SELECT id, score FROM qdrant.points_discover WHERE collection_name = 'coral_pr_audit' AND context = '[{"positive":1,"negative":205}]' AND limit = 3;

SELECT count FROM qdrant.points_count WHERE collection_name = 'coral_pr_audit' AND exact = true;

SELECT write_locked FROM qdrant.locks;

SELECT * FROM qdrant.aliases;

SELECT * FROM qdrant.snapshots;

SELECT * FROM qdrant.cluster_info;

SELECT * FROM qdrant.telemetry;

SELECT * FROM qdrant.collection_cluster_info WHERE collection_name = 'coral_pr_audit';

SELECT * FROM qdrant.collection_aliases WHERE collection_name = 'coral_pr_audit';

SELECT * FROM qdrant.collection_snapshots WHERE collection_name = 'coral_pr_audit';

SELECT ids FROM qdrant.points_by_id WHERE collection_name = 'coral_pr_audit' AND ids = '[1,205]';

SELECT target FROM qdrant.points_discover WHERE collection_name = 'coral_pr_audit' AND target = '[1,0.3,0.1,0.2]' AND limit = 1;

SELECT positive FROM qdrant.points_recommend WHERE collection_name = 'coral_pr_audit' AND positive = '[1]' AND limit = 1;

SELECT id FROM qdrant.points_discover WHERE collection_name = 'coral_pr_audit';
```

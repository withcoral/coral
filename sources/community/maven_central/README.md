# maven_central

Query the [Maven Central](https://search.maven.org) repository — the standard artifact
registry for Java and JVM-based libraries — using SQL. Search artifacts by keyword,
list all published versions of a dependency, and audit packaging metadata.

No API key or account required.

---

## Authentication

None. The Maven Central search API is fully public.

---

## Installation

```bash
coral source add --file sources/community/maven_central/manifest.yaml
```

---

## Tables

| Table       | Description                                      | Required Filter                                              |
|-------------|--------------------------------------------------|--------------------------------------------------------------|
| `artifacts` | Search artifacts by keyword, one row per library | `WHERE q = '...'`                                            |
| `versions`  | List published versions of a specific artifact   | `WHERE q = 'g:"groupId" AND a:"artifactId"'`                |

### Column reference

**artifacts**

| Column         | Type   | Description                                      |
|----------------|--------|--------------------------------------------------|
| id             | Utf8   | `groupId:artifactId`                             |
| g              | Utf8   | Group ID e.g. `com.google.guava`                 |
| a              | Utf8   | Artifact ID e.g. `guava`                         |
| latest_version | Utf8   | Most recently published version                  |
| version_count  | Int64  | Total number of published versions               |
| packaging      | Utf8   | Primary packaging type: `jar`, `bundle`, `pom`   |
| timestamp      | Int64  | Unix milliseconds of latest release              |
| repository_id  | Utf8   | Always `central`                                 |
| q              | Utf8   | Echoes the `q` filter used for this scan         |

**versions**

| Column    | Type   | Description                                  |
|-----------|--------|----------------------------------------------|
| id        | Utf8   | `groupId:artifactId:version`                 |
| g         | Utf8   | Group ID                                     |
| a         | Utf8   | Artifact ID                                  |
| v         | Utf8   | Version string e.g. `33.4.8-jre`            |
| packaging | Utf8   | `jar`, `bundle`, `pom`                       |
| timestamp | Int64  | Unix milliseconds of release                 |
| q         | Utf8   | Echoes the `q` filter used for this scan     |

---

## Example Queries

```sql
-- Search for HTTP client libraries
SELECT g, a, latest_version, version_count
FROM maven_central.artifacts
WHERE q = 'http client'
ORDER BY version_count DESC
LIMIT 10;

-- Find all Spring Boot versions
SELECT g, a, v, timestamp
FROM maven_central.versions
WHERE q = 'g:"org.springframework.boot" AND a:"spring-boot-starter"'
ORDER BY timestamp DESC
LIMIT 20;

-- Check versions of a specific library
SELECT v, packaging, timestamp
FROM maven_central.versions
WHERE q = 'g:"com.google.guava" AND a:"guava"'
ORDER BY timestamp DESC;

-- Search for logging libraries
SELECT g, a, latest_version, version_count, packaging
FROM maven_central.artifacts
WHERE q = 'logging'
ORDER BY version_count DESC
LIMIT 15;

-- Join with GitHub to see issues for a popular artifact
-- (requires github source also added)
SELECT m.a, m.latest_version, i.title, i.state
FROM maven_central.artifacts m
JOIN github.issues i
  ON i.owner = 'google' AND i.repo = 'guava' AND i.state = 'open'
WHERE m.q = 'guava' AND m.g = 'com.google.guava'
LIMIT 10;
```

---

## Notes

- The `artifacts` table requires a keyword via `WHERE q = '...'`. It returns one row
  per `groupId:artifactId` pair with the latest published version.
- The `versions` table requires a Solr query via `WHERE q = 'g:"groupId" AND a:"artifactId"'`.
  Use `maven_central.artifacts` first to discover valid groupId and artifactId values.
- The `q` filter supports full Solr syntax. For example, `g:"org.springframework"` to
  match a specific group, or `a:"guava"` to match a specific artifact.
- Timestamps are Unix milliseconds — divide by 1000 for Unix seconds.
- Maven Central enforces rate limits. Avoid tight query loops.

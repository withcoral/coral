# pgvector on RDS and self-hosted Postgres for Coral

Research for [#2861](https://github.com/withcoral/coral/issues/2861), part of the semantic search map [#2856](https://github.com/withcoral/coral/issues/2856). Written 2026-11-04 from the sources listed at the end. Nothing here ran against a database or an AWS account. Each claim carries a source; inferred claims carry a confidence level.

## Answer in one screen

**Who creates the extension on RDS.** `vector` is not an RDS trusted extension. AWS's trusted list has 22 entries and `vector` is not one of them; `pg_trgm` is ([S1]). Upstream agrees: pgvector's control file sets no `trusted = true` ([S2]), and two requests to add it were closed without the change ([S3]). On RDS only `rds_superuser` can add it, or a user holding the delegated `rds_extension` role ([S4], [S5]). The master user created with the instance is a member of `rds_superuser` ([S6]).

**Coral's role can create it itself on the sandbox.** coral-cloud runs Coral as the master user. `aws_db_instance.database` sets `username = var.database_username` (default `coral`) and the app's `DATABASE_URL` is built from the same name and password ([S7], [S8]). So the role Coral connects with is `rds_superuser`, and the boot check that creates `pg_trgm` today ([S9]) can create `vector` the same way. No separate coral-cloud step is needed for the sandbox as deployed. Confidence: high for the Terraform tree; the live instance was not queried.

**Coral's role can use it once it exists.** An extension is created once per database and its objects land in one schema ([S10]). Coral's `DATABASE_URL` sets no `search_path`, so that schema is `public` ([S8]). Every role has `USAGE` on `public` by default ([S11]). Postgres 15 and later grant no `CREATE` on `public` to everyone, but creation of the extension objects is done by the creating role, not by the users of the type. sqlx resolves the type by its unqualified name through `to_regtype`, which follows the search path ([S12]). Migration 0030 already relies on the same rule for `gin_trgm_ops` ([S13]).

**Where the one-time step goes in coral-cloud, if any.** There is no place for it today and none is needed. The Terraform tree has no `postgresql` provider, no `CREATE ROLE` and no `CREATE EXTENSION` ([S7]). Terraform runs on GitHub-hosted runners ([S14]) and the instance is private with ingress only from the VPC CIDR ([S7]), so a Terraform database resource could not reach it. The only operator path is the runbook in `infra/terraform/eks/README.md` plus a one-off `psql` pod that reads the `database` Secret ([S15], [S16]). Recommendation: add one runbook line that says the server creates `vector` at boot as the master user, and one fallback line for the day the app role is demoted: grant `rds_extension` to it ([S5]) or create the extension once as the master user from a one-off pod.

**Self-hosted.** Outside RDS, `CREATE EXTENSION vector` needs a real superuser because the extension is untrusted ([S2], [S17]). The operator installs the package, then either runs `CREATE EXTENSION vector` once per database as a superuser, or runs Coral as a superuser role. Coral's boot check should try the create and otherwise fail with the Postgres error text plus a one-line remedy, as it does for `pg_trgm` ([S9]).

## 1. RDS versions and rights

### pgvector per engine version

From the RDS release notes extension table ([S18]). The `R2` rows carry the same extension version as their base minor.

| RDS for PostgreSQL 17 | pgvector |
|---|---|
| 17.1 | 0.7.3 |
| 17.2 | 0.7.4 |
| 17.3 to 17.7 | 0.8.0 |
| 17.8, 17.9 | 0.8.1 |
| 17.10, 17.11 | 0.8.2 |

| RDS for PostgreSQL 18 | pgvector |
|---|---|
| 18.0 Preview, RC1, betas | 0.8.0 |
| 18.1 to 18.3 | 0.8.1 |
| 18.4, 18.6 | 0.8.2 |

The AWS announcement of 0.8.0 names "17.1 and higher" ([S19]), but the per-minor table shows 0.7.3 for 17.1 and 0.8.0 from 17.3. Trust the table; it is the per-minor record. Confidence: moderate on which of the two AWS pages is wrong.

Three rules from the same page ([S18]):

- Extension versions do not upgrade when the engine upgrades. AWS recommends upgrading each extension when the engine moves. The upgrade is `ALTER EXTENSION vector UPDATE` in each database ([S20]).
- AWS plans to ship a new extension version in the next quarterly release after it is available.
- AWS gives about 12 months' notice before it drops an extension.

### What the sandbox runs

- The Terraform tree on `origin/main` has `database_engine_version = null`, so the instance took the regional default at creation ([S21]).
- The unmerged cutover branch (`origin/pawel/search-postgres-cutover`, commit 5b98e83) pins `"17"` and says a major-only value lets RDS choose the minor ([S22]).
- P's note from 2026-10-15 records the sandbox instance `coral-cloud-dev-postgres` as PostgreSQL 18, `db.t4g.micro`, private, IAM auth off ([S16]).

The current minor was not checked. Any 18.1 or later minor ships pgvector 0.8.1 or later. Confidence: high that the extension is available; unknown which version.

### Rights on RDS

- "To install most PostgreSQL extensions requires `rds_superuser` privileges." Trusted extensions are the exception and need only `CREATE` on the database ([S1]).
- The trusted list: bool_plperl, btree_gin, btree_gist, citext, cube, dict_int, fuzzystrmatch, hstore, intarray, isn, jsonb_plperl, ltree, pg_trgm, pgcrypto, plperl, plpgsql, pltcl, tablefunc, tsm_system_rows, tsm_system_time, unaccent, uuid-ossp ([S1]). No `vector`.
- The master user "is a member of the `rds_superuser` group" ([S6]). `rds_superuser` may "Add extensions that are available for use with Amazon RDS" ([S23]). It is `NOSUPERUSER`; RDS has no real superuser login ([S23]).
- `rds.allowed_extensions` defaults to `*`. A restricted extension fails with `permission denied to create extension "x"` and the hint `This extension is not specified in "rds.allowed_extensions".` ([S1]).
- Delegation: `rds_extension` is a role an `rds_superuser` can grant. It may create, update and drop extensions named in `rds.allowed_delegated_extensions`, which defaults to an empty string ([S5]).
- pgvector needs no `shared_preload_libraries` entry, no parameter group change and no reboot. The upstream install is `CREATE EXTENSION vector` alone ([S20]), and the RDS extension page lists no preload step for it ([S4]). Confidence: high.

## 2. coral-cloud's database and role bootstrap

What the tree does ([S7], [S8], [S21]):

- `random_password.database` makes the master password.
- `aws_db_instance.database` creates the instance with `db_name = var.database_name` (default `coralcloud`) and `username = var.database_username` (default `coral`).
- `kubernetes_secret_v1.database` stores `DATABASE_URL` as `postgresql://<master user>:<password>@<host>:<port>/coralcloud?sslmode=verify-full&sslrootcert=...`. The app reads it as `CORAL_DATABASE_URL`.
- Nothing creates a second role, a schema, or an extension. The cutover branch README says: "The server creates `pg_trgm` (trusted on RDS, no parameter group, no reboot) and fails startup, naming the extension, if it cannot" ([S15]).

What this means for `vector`:

- The server role is `rds_superuser`, so the boot check can run `CREATE EXTENSION vector` itself. The README sentence about `pg_trgm` is true for the wrong reason: the role would succeed even for an untrusted extension.
- If the app role is ever demoted to a plain role, `vector` stops being creatable by Coral while `pg_trgm` stays creatable. That is the one place where the two extensions differ on RDS. The fix is then `GRANT rds_extension` plus `rds.allowed_delegated_extensions = 'vector'` ([S5]), or a one-off create by the master user.
- The one-off path that exists today: a temporary `postgres` pod in namespace `coral-cloud` with `DATABASE_URL` from the `database` Secret and the RDS CA bundle copied in ([S16]). P asks before creating that pod on the shared cluster.

The cutover branch README also says catalog search uses "one schema per workspace (`search_ws_<id>`)" ([S15]). The coral tree on `origin/main` has no `search_ws_` or `search_registry` string; the search tables live in the shared migration chain ([S13]). That README sentence is stale. Confidence: high.

## 3. `vector` versus `halfvec`, limits, exact scan, iterative scan

All from the pgvector README at 0.8.7 ([S20]) unless marked.

| | `vector` | `halfvec` |
|---|---|---|
| Element | 32-bit float | 16-bit float |
| Storage per row | `4 * dimensions + 8` bytes | `2 * dimensions + 8` bytes |
| Max dimensions, no index | 16,000 | 16,000 |
| Max dimensions, HNSW or IVFFlat | 2,000 | 4,000 |
| Since | 0.1 | 0.7.0 (2024-04-29) ([S24]) |

Operators: `<->` L2, `<#>` negative inner product, `<=>` cosine, `<+>` L1. A `vector` column can be indexed as `halfvec` through an expression index: `USING hnsw ((embedding::halfvec(3)) halfvec_l2_ops)`. The README's own guidance on the choice: "Use the `halfvec` type instead of `vector` for a smaller working set."

Candidate model dimensions on the map (from the model cards, not re-verified in this ticket): potion-base-8M 256, all-MiniLM-L6-v2 384, bge-small 384, bge-base 768, arctic-embed-xs and -s 384, nomic-embed-text-v1.5 768, Qwen3-Embedding-0.6B 1024, jina-embeddings-v2-base-code 768. Every one fits `vector` with an index. The dimension limit does not force `halfvec`; only storage and scan speed do. Confidence: moderate on the numbers.

**Exact scan.** "By default, pgvector performs exact nearest neighbor search, which provides perfect recall." Without an index, `ORDER BY embedding <=> $1 LIMIT k` is a sequential scan with a sort. A `WHERE` clause on such a query is an ordinary filter: it drops no results. The README's one tuning line for this path: "To speed up queries without an index, increase `max_parallel_workers_per_gather`." For normalised vectors it recommends inner product (`<#>`) as the fastest operator. The map chose exact scan with HNSW only when measured latency demands it, so none of the index material below applies until then.

**Filtering with an approximate index.** "With approximate indexes, filtering is applied *after* the index is scanned." "If a condition matches 10% of rows, with HNSW and the default `hnsw.ef_search` of 40, only 4 rows will match on average." A B-tree on the filter column keeps the search exact when the filter is selective. Partial indexes and partitioning are the other two tools.

**Iterative index scans, added in 0.8.0 (2024-10-30) ([S24]).** The scan continues through the index until enough rows pass the filter. Settings: `hnsw.iterative_scan = strict_order | relaxed_order` (and `ivfflat.iterative_scan = relaxed_order`), `hnsw.max_scan_tuples` (20,000 by default), `hnsw.scan_mem_multiplier` (1, as a multiple of `work_mem`), `ivfflat.max_probes`. Strict order "ensures results are in the exact order by distance"; relaxed "allows results to be slightly out of order by distance, but provides better recall". Every RDS 17 minor from 17.3 and every RDS 18 GA minor ships 0.8.0 or later, so iterative scans are available on RDS if Coral ever adds HNSW with a workspace or kind filter.

**Versions after 0.8.0** are bug fixes ([S24]): 0.8.1 adds Postgres 18; 0.8.2 fixes a parallel HNSW build overflow; 0.8.3 fixes "possible index corruption with HNSW vacuuming"; 0.8.4 to 0.8.7 fix more HNSW vacuum and IVFFlat build bugs. None touches exact scan. The unreleased 0.9.0 drops Postgres 13 and 14. The README still says "supports Postgres 13+" ([S20]).

## 4. Rust access from sqlx 0.9

Coral locks `sqlx 0.9.0`, `sea-query 1.0.2` and `sea-query-sqlx 0.9.1` ([S25]).

- The `pgvector` crate is at 0.4.2 (2026-05-22) with the changelog line "Added support for SQLx 0.9" ([S26], [S27]). Its `Cargo.toml` declares `sqlx = { version = ">= 0.8, < 0.10", default-features = false, features = ["postgres"], optional = true }` and features `postgres` and `halfvec`; `sqlx` is an implicit optional-dependency feature ([S28]). sqlx 0.9.0 was published on 2026-05-21, one day before ([S29]).
- With `features = ["sqlx"]` the crate implements `Type`, `Encode`, `Decode` and `PgHasArrayType` for `Vector` under the type name `"vector"`; with `halfvec` as well it does the same for `HalfVector` in `src/sqlx_ext/halfvec.rs` ([S30], [S31]). Its CI runs `cargo test --features sqlx` and `--features sqlx,halfvec` against pgvector v0.8.5 with an unpinned sqlx dev-dependency (`version = "0"`), which resolves to 0.9 today ([S32]).
- Wire format, if Coral ever encodes by hand: a big-endian `u16` dimension count, a `u16` that must be 0, then one big-endian `f32` per element; the crate checks nothing against 16,000 ([S30], [S33]). A hand-written type is about 40 lines; the crate is small and maintained, so the crate is the better choice. Confidence: high.
- Query building: `sea-query 1.0.2` has the feature `postgres-vector` which adds `Value::Vector(Option<pgvector::Vector>)` and depends on `pgvector ^0.4`; `sea-query-sqlx 0.9.1` has `postgres-vector = ["sea-query/postgres-vector", "pgvector/sqlx"]` with `pgvector ^0.4.2` and `sqlx ^0.9`, and binds the value with `args.add(v)` ([S34], [S35], [S36], [S37]). Coral turns those two features on in the workspace `Cargo.toml` and binds a `pgvector::Vector` like any other value.
- sea-query has no `HalfVector` value ([S36], [S37]). If the store uses a `halfvec` column, bind a `Vector` and cast in SQL (`$1::halfvec`), or bind with sqlx directly in `coral-search` as the storage probes already do.
- Type resolution: sqlx looks the name up with `to_regtype(E'vector')::oid` and returns `Error::TypeNotFound` when it is `NULL` ([S12]). `to_regtype` honours `search_path`, so the schema that holds the extension must be on it. Production has the default `"$user", public`; the test fixture sets `<schema>,public` and installs extensions into `public` for that reason ([S38]).

## 5. Local images for the OrbStack containers

Observed on this machine with `docker inspect` on 2026-11-04 (read-only):

| Container | Image | Role | Notes |
|---|---|---|---|
| `coral-desktop-postgres` | `docker.io/library/postgres:17`, server 17.10 (`PG_VERSION=17.10-1.pgdg13+1`, Debian 13 trixie) | `POSTGRES_USER=coral`, `POSTGRES_DB=coral` | volume `coral-desktop-pgdata`, restart `unless-stopped`; see P's note ([S39]) |
| `coral-test-postgres` | `postgres:17` | `postgres` | created by `scripts/postgres.ts`, image from `LOCAL_POSTGRES_IMAGE`, default `docker.io/library/postgres:17` ([S40]) |

Both roles are superusers: the official image "will create the specified user with superuser power", and the default user is `postgres` ([S41]). So locally Coral's boot check can create `vector` itself once the image carries the extension.

**The image.** `pgvector/pgvector` is the official `postgres` image plus the compiled extension: `FROM postgres:$PG_MAJOR-$DEBIAN_CODENAME` with `ARG DEBIAN_CODENAME=bookworm`, then `make install` of the pinned tag ([S42]). Tags on Docker Hub for PG 17 and PG 18 cover 0.8.0 to 0.8.7, each as plain, `-bookworm` and `-trixie`, all with amd64 and arm64 ([S43], [S44]). The plain tag is the bookworm build; the digests of `0.8.7-pg17` and `0.8.7-pg17-bookworm` are equal, and `0.8.7-pg17-trixie` differs ([S45]).

**Which tag.** Two facts decide it:

1. The bare `postgres:17` tag is trixie today: the official tag list reads "`17.11`, `17`, `17.11-trixie`, `17-trixie`" ([S46]), and the desktop container's package string ends in `pgdg13`. Debian 13 ships a newer glibc than Debian 12. A glibc change under an existing cluster can change the sort order of libc collations; Postgres then warns `collation ... has version mismatch` and asks for `REINDEX` ([S47]). Keep the codename the volume was initialised with: use a `-trixie` tag for `coral-desktop-postgres`. Confidence: high on the mechanism, moderate on whether these two glibc versions differ for the collations Coral uses.
2. RDS ships 0.8.2 on the newest 17 and 18 minors. Pinning `0.8.2-pg17-trixie` locally mirrors RDS; `pg17-trixie` (0.8.7) carries the later HNSW and IVFFlat fixes, which do not matter for an exact scan.

Recommendation: `pgvector/pgvector:0.8.2-pg17-trixie` for both containers, bumped together with the RDS engine. The desktop switch is `docker stop`, `docker rm` (the volume survives), `docker run` with the same flags and the new image ([S39] has the flags). Then `CREATE EXTENSION vector` happens at Coral's next boot. Confidence: high.

**The bench container.** `LocalPostgres.start()` reuses an existing container by name and only pulls the image when it creates one ([S40]). Changing the default image in `scripts/postgres.ts` therefore needs each developer to remove the old `coral-test-postgres` once. `Makefile` already exports `LOCAL_POSTGRES_IMAGE` ([S48]), so the override path exists today. The test fixture installs shared extensions with `CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public` and tolerates the duplicate-object race ([S38]); `vector` goes into the same function. No GitHub workflow in the coral tree names a Postgres image; the script default is the single place to change. Confidence: moderate on CI, from a grep of `.github/workflows`.

## 6. The self-hosted prerequisite and the migration failure

**What the operator installs** ([S20]):

- Debian and Ubuntu: `sudo apt install postgresql-17-pgvector` from the PostgreSQL APT repository (the README shows 18 and says to replace it with the server major).
- RHEL family: `sudo yum install pgvector_17` or `dnf` from the PostgreSQL Yum repository.
- macOS Homebrew Postgres: `brew install pgvector`.
- Docker: `pgvector/pgvector:pg17-trixie` or a tag above.
- From source: `make` and `make install` against the server's development headers; "supports Postgres 13+".

**What the operator then runs, once per database:** `CREATE EXTENSION vector;` as a superuser ([S20]). On a self-hosted server a non-superuser role cannot do this, whatever its `CREATE` rights, because the control file is untrusted ([S2], [S17]). The documented alternative is to run Coral as a superuser role, as the Docker images do.

**The three failure texts Coral will see** (Postgres 17 source, [S17]; RDS, [S1]):

```
ERROR:  extension "vector" is not available
DETAIL:  Could not open extension control file ".../extension/vector.control": No such file or directory.
HINT:  The extension must first be installed on the system where PostgreSQL is running.
```

```
ERROR:  permission denied to create extension "vector"
HINT:  Must be superuser to create this extension.
```

```
ERROR: permission denied to create extension "vector"
HINT: This extension is not specified in "rds.allowed_extensions".
```

**How Coral's migration should fail.** The map says the migration creates the extension or fails with a one-line instruction. Coral already has the mechanism, and it sits before the chain, not inside it:

- `coral_search::prepare_storage` runs at boot right before `Database::migrate`. For Postgres it reads `pg_extension` for `pg_trgm`, tries `CREATE EXTENSION pg_trgm`, reads again, and otherwise returns `StorageError::MissingFeature { database, feature, cause, remedy }`, whose message is one line: `Postgres 17.10 does not provide required search feature: pg_trgm extension (<Postgres error>); <remedy>` ([S9], [S49]).
- The module doc states the reason: the search objects "need database features that a migration cannot create or explain" ([S9]). sqlx wraps each migration in its own transaction under a `pg_advisory_lock`; a failing step records no ledger row and surfaces as `MigrateError::ExecuteMigration` with the raw Postgres text ([S50]). There is no place in that path for a remedy sentence.
- Coral's migration rule also forbids `IF NOT EXISTS` except for the three search steps that adopted an older stream ([S13], [S51]), so `CREATE EXTENSION IF NOT EXISTS vector` inside a migration would break a documented rule.

Recommendation: extend `probe_postgres` and `check_postgres_features` with a second feature, `vector`, with the same probe-create-reprobe shape and the remedy `install pgvector on the server and run CREATE EXTENSION vector as a superuser in Coral's database, or let Coral's database role create it`. Read `extversion` in the same probe and refuse versions below 0.7.0 if the store uses `halfvec` (0.7.0 introduced it, [S24]); RDS 17 GA minors from 17.3 and RDS 18 from 18.1 are all at 0.8.0 or later, so the floor only bites self-hosted servers. The table test in `crates/coral-search/src/storage/tests.rs` gains one row per outcome ([S49]). The vector migration that follows can then use the type unqualified, as 0030 uses `gin_trgm_ops`. Confidence: high that this matches the existing pattern; the floor value is a design choice.

## 7. Open questions

1. **Which minor the sandbox runs today.** P's note says PostgreSQL 18; the merged Terraform leaves the version to AWS; the unmerged cutover branch pins 17. One `SELECT version(), extversion FROM pg_extension WHERE extname = 'vector'` from a one-off pod settles both the engine and whether anyone created the extension already.
2. **Whether the app keeps the master user.** Running the server as `rds_superuser` is what makes the boot-time create work. If coral-cloud ever moves the app to a least-privilege role, `vector` needs `rds_extension` or a one-off create, while `pg_trgm` does not. The README line proposed above records this.
3. **`vector` or `halfvec`.** Every candidate model fits `vector` with room for an index. `halfvec` halves storage. Whether it changes exact-scan latency on the Graph corpus is unmeasured; the hyperparameter task can measure both on the same rows.
4. **The version floor in the boot check.** 0.7.0 for `halfvec`, or 0.8.0 to match the lowest RDS 17 GA version, or none.
5. **Upstream trust.** Issues #288 and #1017 asked for `trusted = true` and were closed as completed, but `vector.control` at 0.8.7 still has no such line ([S2], [S3]). If a later release adds it, self-hosted roles with `CREATE` on the database could create the extension and the remedy text can soften.
6. **Debian codename for the desktop image.** The collation check after switching to `pgvector/pgvector:0.8.2-pg17-trixie` is one query (`SELECT datname, datcollversion FROM pg_database` against `pg_database_collation_actual_version`); it was not run here.
7. **sea-query feature or direct sqlx.** `postgres-vector` pulls `pgvector` into every crate that builds sea-query. Binding directly in `coral-search` keeps the dependency local. Either compiles; the choice is about dependency scope.

## Sources

- [S1] AWS, "Supported PostgreSQL extension versions", sections "Restricting installation of PostgreSQL extensions" and "PostgreSQL trusted extensions". https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.FeatureSupport.Extensions.html (read as the `.md` rendering). Fetched 2026-11-04.
- [S2] pgvector `vector.control` at master (`default_version = '0.8.7'`, `relocatable = true`, no `trusted`). https://github.com/pgvector/pgvector/blob/master/vector.control
- [S3] pgvector issues #288 (2023-09-29) and #1017 (2026-08-14), "Any chance to see this extension packaged as trusted?", both closed. https://github.com/pgvector/pgvector/issues/288 and https://github.com/pgvector/pgvector/issues/1017
- [S4] AWS, "Using PostgreSQL extensions with Amazon RDS for PostgreSQL". https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.Extensions.html
- [S5] AWS, "Using Amazon RDS delegated extension support for PostgreSQL". https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/RDS_delegated_ext.html
- [S6] AWS, "Understanding PostgreSQL roles and permissions". https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.Roles.html
- [S7] coral-cloud, `infra/terraform/eks/rds.tf` and `variables.tf` on branch `pawel/search-postgres-cutover` (local worktree `.claude/worktrees/search-postgres-cutover`, commit 5b98e83).
- [S8] coral-cloud, `infra/terraform/eks/kubernetes_app.tf`, `kubernetes_secret_v1.database` and the `CORAL_DATABASE_URL` env entry, same branch.
- [S9] coral, `crates/coral-search/src/storage.rs` and `storage/repository.rs` at `origin/main` c82b1700e (`prepare_storage`, `probe_postgres`, `check_postgres_features`, `create_pg_trgm`).
- [S10] PostgreSQL 17, `CREATE EXTENSION`, "Description" and the `schema_name` parameter. https://www.postgresql.org/docs/17/sql-createextension.html
- [S11] PostgreSQL 17, "5.10.4 Schemas and Privileges" and "5.10.6 Usage Patterns". https://www.postgresql.org/docs/17/ddl-schemas.html
- [S12] sqlx-postgres 0.9.0, `src/connection/resolve.rs` lines 173 to 192 (`to_regtype(E'<name>')::oid`, `Error::TypeNotFound`), read from the local cargo registry.
- [S13] coral, `crates/coral-db/migrations/postgres/0030_search_text_search.sql` (comment on unqualified `gin_trgm_ops`; `IF NOT EXISTS` only for adopted objects).
- [S14] coral-cloud, `infra/terraform/eks/README.md`, "GitHub Deployment" (the `deploy-terraform-eks.yml` workflow runs `terraform apply`).
- [S15] coral-cloud, `infra/terraform/eks/README.md`, "Runbook: cutover to Postgres catalog search", same branch.
- [S16] P's note `coral-sandbox-rds-access` (2026-10-15): private PG 18 instance, one-off `postgres:18` pod with the `database` Secret and the RDS CA bundle.
- [S17] PostgreSQL 17 source, `src/backend/commands/extension.c`: `parse_extension_control_file` ENOENT branch, `execute_extension_script` permission branch, `extension_is_trusted`. https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/commands/extension.c
- [S18] AWS, "Extension versions for Amazon RDS for PostgreSQL", tables for 17 and 18 and the intro rules. https://docs.aws.amazon.com/AmazonRDS/latest/PostgreSQLReleaseNotes/postgresql-extensions.html
- [S19] AWS What's New, 2024-11-21, "Amazon RDS for PostgreSQL supports pgvector 0.8.0". https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-rds-for-postgresql-pgvector-080/
- [S20] pgvector README at master (0.8.7): Installation, Getting Started, Storing, Querying, Indexing, Filtering, Iterative Index Scans, Half-Precision Vectors, Performance, Upgrading, Reference. https://github.com/pgvector/pgvector/blob/master/README.md
- [S21] coral-cloud `origin/main` (2d9b54f), `infra/terraform/eks/variables.tf`: `database_engine_version` default `null`.
- [S22] coral-cloud branch `pawel/search-postgres-cutover`, `variables.tf`: `database_engine_version` default `"17"` with the pinning comment.
- [S23] AWS, "Understanding the rds_superuser role". https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.Roles.rds_superuser.html
- [S24] pgvector `CHANGELOG.md` at master: 0.9.0 (unreleased), 0.8.7 (2026-10-01) back to 0.7.0 (2024-04-29). https://github.com/pgvector/pgvector/blob/master/CHANGELOG.md
- [S25] coral `Cargo.toml` and `Cargo.lock` at c82b1700e: `sqlx 0.9.0`, `sea-query 1.0.2`, `sea-query-sqlx 0.9.1`.
- [S26] crates.io API for `pgvector`: 0.4.2 (2026-05-22), 0.4.1 (2025-05-20), 0.4.0 (2024-07-28). https://crates.io/api/v1/crates/pgvector
- [S27] pgvector-rust `CHANGELOG.md`: "0.4.2 (2026-05-22) Added support for SQLx 0.9". https://github.com/pgvector/pgvector-rust/blob/master/CHANGELOG.md
- [S28] pgvector-rust `Cargo.toml` at master. https://github.com/pgvector/pgvector-rust/blob/master/Cargo.toml
- [S29] crates.io API for `sqlx`: 0.9.0 published 2026-05-21. https://crates.io/api/v1/crates/sqlx
- [S30] pgvector-rust `src/sqlx_ext/vector.rs` (type name `"vector"`, `encode_by_ref`, header layout). https://github.com/pgvector/pgvector-rust/blob/master/src/sqlx_ext/vector.rs
- [S31] pgvector-rust tree: `src/sqlx_ext/{bit,halfvec,sparsevec,vector}.rs`. https://api.github.com/repos/pgvector/pgvector-rust/git/trees/master?recursive=1
- [S32] pgvector-rust `.github/workflows/build.yml` (pgvector v0.8.5, `cargo test --features sqlx` and `sqlx,halfvec`). https://github.com/pgvector/pgvector-rust/blob/master/.github/workflows/build.yml
- [S33] pgvector-rust `src/vector.rs`, `from_sql`. https://github.com/pgvector/pgvector-rust/blob/master/src/vector.rs
- [S34] crates.io API, `sea-query` 1.0.2 features (`postgres-vector = ["pgvector"]`) and dependencies (`pgvector ^0.4`, optional). https://crates.io/api/v1/crates/sea-query/1.0.2
- [S35] crates.io API, `sea-query-sqlx` 0.9.1 features and dependencies (`pgvector ^0.4.2`, `sqlx ^0.9`, `sea-query ^1.0.0`). https://crates.io/api/v1/crates/sea-query-sqlx/0.9.1
- [S36] sea-query `src/value.rs` at master: `Vector(Option<pgvector::Vector>)` under `postgres-vector`. https://github.com/SeaQL/sea-query/blob/master/src/value.rs
- [S37] sea-query `sea-query-sqlx/src/sqlx_postgres.rs` at master: `Value::Vector(v) => args.add(v)`. https://github.com/SeaQL/sea-query/blob/master/sea-query-sqlx/src/sqlx_postgres.rs
- [S38] coral, `crates/coral-test-support/src/lib.rs` (`postgres` fixture, `install_shared_extensions`).
- [S39] P's note `coral-desktop-on-postgres` (2026-10-16, updated 2026-11-03): container flags, volume, pinning through `desktop.json`.
- [S40] coral, `scripts/postgres.ts` (`LocalPostgres`, image default, reuse by name).
- [S41] Docker official image docs for `postgres`, `POSTGRES_USER`. https://github.com/docker-library/docs/blob/master/postgres/content.md
- [S42] pgvector `Dockerfile` at master. https://github.com/pgvector/pgvector/blob/master/Dockerfile
- [S43] Docker Hub API, `pgvector/pgvector` tags containing `pg17` (28 tags). https://hub.docker.com/v2/repositories/pgvector/pgvector/tags?page_size=100&name=pg17
- [S44] Docker Hub API, tags containing `pg18` (24 tags). https://hub.docker.com/v2/repositories/pgvector/pgvector/tags?page_size=100&name=pg18
- [S45] Docker Hub API, tags `0.8.7-pg17`, `0.8.7-pg17-bookworm`, `0.8.7-pg17-trixie` with digests. https://hub.docker.com/v2/repositories/pgvector/pgvector/tags?page_size=100&name=0.8.7-pg17
- [S46] Docker official image docs for `postgres`, "Supported tags". https://github.com/docker-library/docs/blob/master/postgres/README.md
- [S47] PostgreSQL 17, `ALTER COLLATION`, "Notes" on collation versions. https://www.postgresql.org/docs/17/sql-altercollation.html
- [S48] coral, `Makefile` line 107 (`export ... LOCAL_POSTGRES_IMAGE ...`).
- [S49] coral, `crates/coral-search/src/storage/tests.rs` (the "Postgres without pg_trgm" case and its message).
- [S50] sqlx-postgres `src/migrate.rs` at main: `apply` (transaction unless `no_tx`), `lock` (`pg_advisory_lock`). https://github.com/launchbadge/sqlx/blob/main/sqlx-postgres/src/migrate.rs
- [S51] coral, `crates/coral-db/src/migration.rs` (`Database::migrate` doc: "callers must not use `IF NOT EXISTS` to conceal unexpected schema state").

Unreachable or empty during this research: `https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL_pgvector.html` and `.../PostgreSQL_Trusted_Extensions.html` returned no body on two tries each; the trusted list came from [S1] instead. The pgvector README's "Hosted Postgres" section only links to issue #54 and was not followed.

---
status: accepted
---

# Admin operations are a Built-in Provider

Agent onboarding needs an agent to add a Provider. Earlier designs gave `coral.admin` a separate surface with its own
catalog, policy, approval and records. Coral instead gives every Workspace one **Built-in Provider**: a real Provider
row with a real Materialized Operation Catalog whose Operations Coral defines in Rust and performs itself. Search,
describe, Operation Access State, Approval Requests and Operation Invocation records therefore apply unchanged, and
Coral grows the catalog one Operation at a time, starting with `createProvider`. This supersedes those designs.

Each Provider row stores its full **Provider Call Root** (`coral.providers.github`, or `coral.admin` for the Built-in
Provider) instead of a short namespace that code prefixes with `coral.providers.`. Call Paths come straight from the
row, Workspace uniqueness applies to the Call Root, and a Provider named `admin` cannot clash with the Built-in
Provider. Coral does this before the stable release, because changing stored roots later would mean migrating customer
data.

The catalog tables only accept the known input kinds and protocols, and the Provider lifecycle table only accepts the
known actions. One SQL file runs on both SQLite and Postgres, and no portable statement can widen a CHECK constraint:
SQLite has to rebuild the table with foreign keys off, which Postgres rejects, and Postgres's
`ALTER TABLE … DROP CONSTRAINT` does not exist in SQLite. Coral therefore edits
`0003_materialized_operation_catalog.sql` in place, once, as approved on 5 October 2026. The same change also widens
the lifecycle action list in `0004_workspace_provider_publication.sql`, so that installing the Built-in Provider
records its own action instead of a Publication that never happened. This breaks every database created before the
change, including public beta installs: the boot fence rejects them and they must be recreated. The pull request that
makes the edit merges with `scripts/check-migrations.sh` overridden. The script and the append-only rule stay
unchanged for every later change.

The Built-in Provider is immutable apart from its access state. The server rejects rename, reconfiguration,
regeneration, account work and removal for it, and an Owner can only disable or enable it and set its Operations'
policy.

Rejected: a separate admin surface, which duplicates catalog, policy and approval; a reserved `admin` namespace under
`coral.providers`, which special-cases the call path; and a new rebuilding migration, which cannot be portable.

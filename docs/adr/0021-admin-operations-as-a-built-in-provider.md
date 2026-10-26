---
status: accepted
---

# Admin operations are a Built-in Provider

Agent onboarding needs an agent to add a Provider. Earlier designs gave `coral.admin` a separate surface with its own
catalog, policy, approval and records. Coral instead gives every Workspace one **Built-in Provider**: a real Provider
row with a real Materialized Operation Catalog whose Operations Coral defines in Rust and performs itself. Search,
describe, Operation Access State, Approval Requests and Operation Invocation records therefore apply unchanged, and
Coral grows the catalog one Operation at a time, starting with `addProvider`. This supersedes those designs.

Each Built-in Operation takes the words Reef uses for the same action, since an Owner reads its name when approving a
call: `addProvider` for "Add provider" (it was first named `createProvider`), and later `removeProvider` for "Remove
provider", not `deleteProvider`.

Each Provider row stores its full **Provider Call Root** (`coral.providers.github`, or `coral.workspace` for the Built-in
Provider) instead of a short namespace that code prefixes with `coral.providers.`. Call Paths come straight from the
row, Workspace uniqueness applies to the Call Root, and a Provider named `workspace` cannot clash with the Built-in
Provider. Migration `0028` prefixes the namespace of every existing row, so Providers from an earlier release keep their
Call Paths.

The catalog tables only accept the known input kinds and protocols, and the Provider lifecycle table only accepts the
known actions. Migration `0026` widens the catalog rules to accept `builtin`. Migration `0027` adds the
`provider.builtin_installed` action, so installing the Built-in Provider records its own action instead of a
Publication that never happened. Postgres can replace a CHECK constraint and SQLite cannot, so each of the two has one
file per database: Postgres replaces the constraint, and SQLite rebuilds the table. A database from an earlier release
upgrades in place.

The Built-in Provider is immutable apart from its access state. The server rejects rename, reconfiguration,
regeneration, account work and removal for it, and an Owner can only disable or enable it and set its Operations'
policy.

Amended by [ADR 0022](0022-coral-updates-the-builtin-catalog-at-boot.md): when Coral boots, it installs the Built-in
Provider in each Workspace that has none and updates it to the catalog it serves. An Owner still cannot regenerate or
update it.

[ADR 0024](0024-add-provider-returns-a-link-and-does-not-wait.md) records why `addProvider` returns a Reef link when a
person must sign in, instead of waiting for them.

Rejected: a separate admin surface, which duplicates catalog, policy and approval; a reserved `workspace` namespace under
`coral.providers`, which special-cases the call path; and editing `0003` and `0004` in place, which makes Coral refuse
every database from an earlier release.

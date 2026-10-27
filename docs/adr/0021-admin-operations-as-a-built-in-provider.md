---
status: accepted
---

# Admin operations are a Built-in Provider

Agent onboarding needs an agent to add a Provider. Earlier designs gave `coral.admin` a separate surface with its own
catalog, policy, approval and records. Instead every Workspace has one **Built-in Provider**: a real Provider with a
real Materialized Operation Catalog, called at `coral.workspace`, whose Operations Coral defines in Rust and performs
itself. Search, describe, Operation Access State, Approval Requests and Invocation records apply unchanged, and the
catalog grows one Operation at a time. Each Operation takes the words Reef uses for the same action, such as
`addProvider` for "Add provider", because an Owner reads that name when approving a call.

Each Provider row stores its full Provider Call Root (`coral.providers.github`, `coral.workspace`), so Call Paths come
straight from the row and no Provider can clash with the Built-in one.

Rejected: a separate admin surface, which duplicates catalog, policy and approval; and a reserved `workspace`
namespace under `coral.providers`, which special-cases the call path.

## Owners cannot change it; Coral updates it at boot

Owners can only enable or disable the Built-in Provider and set its Operations' policy. Rename, reconfiguration,
regeneration, account work and removal are rejected. When a Coral process boots, before it takes requests, it installs
the Built-in Provider in any Workspace that has none and moves every Workspace's Built-in Provider to the catalog it
serves, with Coral as the recorded actor. New Operations simply appear, at the Access State their hints give.

Catalogs are not ordered. An older Coral that boots after a newer one, in a rollback or a rolling deploy, moves
Workspaces back to its own catalog, and the next newer boot moves them forward. Rejected: Owners reviewing and applying
each update, which leaves Workspaces stale until someone acts; and ordering catalogs by version, which needs a manual
bump or release parsing and leaves a rolled-back Coral serving Operations it cannot perform.

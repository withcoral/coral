---
status: accepted
---

# Enterprise code lives in `ee/` and runs only in the Enterprise Edition

Coral needs Enterprise Features that are not Apache-2.0. We keep their code in an `ee/` directory of this public
repository, under its own licence, in the same Cargo workspace and CI. A licence, not repository access, protects that
code. One repository means a change across the boundary lands in one PR, with no export or sync tooling to maintain.

Dependencies point one way: `ee/` depends on core, and core never depends on, imports, or names `ee/` code. When an
Enterprise Feature needs something new from core, core gains a general extension point and `ee/` implements it. A plain
`cargo build` at the root builds only core, so a source build is pure Apache-2.0.

We release one binary built from `ee/`. It always starts its `ee/` code and registers its Enterprise Features with
core; core keeps no list of them, so a source build does not know they exist. Whether an Enterprise Feature works is
decided at runtime by the Edition, not by a compile-time feature: the Edition can change while Coral runs, so each
Enterprise Feature checks it every time it is used, and one used while the Edition is Community fails closed without
deleting anything.

`ee/` never adds a migration. The schema an Enterprise Feature needs is general and lives in core's one migration
chain, so a deployment can move between the released binary and a source build without meeting migrations it does not
know.

## Considered options

- **A separate private repository.** Rejected: export, import and revision-bump tooling, and every change across the
  boundary split over two PRs, for secrecy we do not need.
- **An `enterprise` Cargo feature.** Rejected: Cargo has no workspace features, a compile-time switch cannot follow a
  licence that changes at runtime, and it would put enterprise switches in core.
- **Migrations owned by `ee/`.** Rejected: a deployment that moves to a build without them fails the schema checks at
  boot.

## Consequences

- Build a release from the released binary's package alone, never from a whole-workspace build: Cargo builds a
  dependency shared by several packages once, with every feature any of them asks for.
- Commit messages, PR descriptions and `ee/` code are all public.

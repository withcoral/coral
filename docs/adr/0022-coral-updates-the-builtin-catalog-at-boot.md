---
status: accepted
---

# Coral updates the Built-in Provider's catalog at boot

Each Workspace has its own Generation of the Built-in Provider's catalog. Coral writes it when it creates the Workspace.
A Workspace from a release before the Built-in Provider has none.

When a Coral process boots, before it takes requests, it first installs the Built-in Provider in each Workspace that
has none, in any Workspace state. Each install has its own transaction, the served catalog, Coral as the recorded actor
and one `provider.builtin_installed` event. Then the process finds every Workspace's Built-in Provider whose Generation
pins a catalog identity other than the one the process serves. It updates each in its own transaction: it publishes a
Generation of the served catalog and activates it, with Coral as the recorded actor and one `provider.updated` event
with the changed field paths `provider.active_generation` and `catalog`. The Provider keeps its state, and an archived
Workspace updates too. Coral asks and tells nobody; the new Operations simply appear. A Workspace whose update fails
keeps its catalog until the next boot, and the other Workspaces still update.

Processes that boot together each try the install and the update. The first to hold the Workspace row installs the
Provider, and the others then find it installed. The first to hold the Provider row replaces the Generation it listed.
The others then find another Generation active and skip the Provider, even when that Generation pins another catalog;
their next boot moves it to their own. Coral reads one row per Workspace to find what to install or update, so a boot
with nothing to do stays cheap.

Catalogs are not ordered. A Coral older than the one that booted last, after a rollback or when an old process restarts
during a rolling deploy, moves Workspaces back to its own catalog, and the next newer boot moves them forward again. An
older Coral that reads a newer catalog in the meantime still searches and describes its Operations, and fails an
Invocation of one it does not perform with `operation_unavailable`.

A new Operation starts at the Access State its hints give, and Owners change it with the existing Operation policy.
Exact rules are keyed by Operation Key, so a rule set on an Operation applies again when the Operation returns.

This amends [ADR 0021](0021-admin-operations-as-a-built-in-provider.md), which says the server rejects regeneration and
the Built-in Provider is immutable apart from its access state. An Owner still cannot regenerate, rename, reconfigure,
update or remove it. Coral updates it at boot.

Rejected: an Owner reviewing and applying each update on the providers page, which leaves Workspaces on an old catalog
until someone acts and makes every Owner repeat the same step; and ordering catalogs by a version number so that an
older Coral never moves a Workspace back, which needs a manual bump, release parsing or build changes, and leaves a
rolled-back Coral serving Operations it cannot perform.

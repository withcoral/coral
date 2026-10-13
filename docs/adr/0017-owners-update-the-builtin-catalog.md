---
status: accepted
---

# Owners update the Built-in Provider's catalog

Each Workspace has its own Generation of the Built-in Provider's catalog. Coral writes it when it creates the Workspace,
so an Operation that a later Coral defines never reaches an existing Workspace on its own.

When the Coral that serves a request defines a catalog other than the Workspace's, so that their fingerprints differ,
the providers page shows an Owner what an update adds, removes and changes. An Owner who selects Update publishes a
Generation of the served catalog and activates it, with themselves as the recorded actor and one `provider.updated`
event with the changed field paths `provider.active_generation` and `catalog`. The Provider keeps its state. Archiving
blocks the update, as it blocks every configuration change other than restoration. Nothing updates on its own: Coral
has no boot step for this, so processes of different versions never move a Workspace back and forth.

Catalogs are not ordered. A Coral older than the one an Owner updated with, during a rolling deploy or after a rollback,
offers an update that removes or reverts what the newer one added, and the page shows exactly that before the Owner
selects Update. An older Coral that reads a newer catalog still searches and describes its Operations, and fails an
Invocation of one it does not perform with `operation_unavailable`.

A new Operation starts at the Access State its hints give, and Owners change it with the existing Operation policy.
Exact rules are keyed by Operation Key, so a rule set on an Operation applies again when the Operation returns.

This amends ADR 0016, which says the server rejects regeneration and the Built-in Provider is immutable apart from its
access state. An Owner still cannot regenerate, rename, reconfigure or remove it. An Owner can update it to the catalog
the serving Coral defines.

Rejected: updating every Workspace at boot, which changes Workspaces without their Owners and needs stored state to tell
Owners what changed; marking added Operations "New" per Owner or per Workspace, which needs the same state; and ordering
catalogs by a version number, which needs a manual bump, release parsing or build changes only to hide a prompt that
appears in a rare deploy or rollback window.

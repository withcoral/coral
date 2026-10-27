---
status: accepted
---

# Use Workspace-local Providers and Accounts in v1

In v1 a Provider belongs to one Workspace, with its own namespace, catalog, account setting, Operation policy and
lifecycle. Organization Providers, distribution across Workspaces and shared organization credentials would reduce
repeated setup, but their ownership, availability, account-selection and update concepts cost more than v1 can
justify. Each Workspace sets up and maintains its own Providers. No dormant scaffolding for the removed model remains.

## Accounts are local records, not an inventory

A Provider uses no account, one shared Provider Account, or one Provider Account per caller (User or Agent). An Account
is a local authorization record with protected credentials, not a verified upstream identity. A second simultaneous
account for the same caller, or a second shared account, needs another Provider. Removing and recreating a namespace
carries over no identity, policy or accounts.

Coral keeps no dormant credentials. Switching the account setting clears the previous setting's credentials once the
new authorization succeeds; keeping them for a switch back was rejected as hidden credential inventory. Credentials used
only to discover Operations during setup are temporary. They become a runtime Account only when explicitly kept as the
shared Account or the creating User's own.

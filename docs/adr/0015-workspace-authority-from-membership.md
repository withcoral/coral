---
status: accepted
---

# Workspace authority comes only from a Workspace Membership

An Organization Owner used to hold Owner authority in every Workspace without a Membership. The need was an IT admin who
can see and recover any Workspace. That answer was much bigger than the need: the Organization Owner became a working
Owner everywhere and appeared in Workspaces nobody had invited them to. Authority inside a Workspace now comes only from
a Workspace Membership. Removing one always ends access there and clears that User's stored Provider Account
credentials and Saved Functions.

An Organization Owner keeps a small governance surface over Workspaces they are not in: list them with their state and
designated Owners, assign a designated Owner (themselves included), archive and restore. Assigning themselves is the
explicit, recorded way in. Rejected: a separate IT admin or auditor role, which self-assignment makes unnecessary for
now.

## Every Workspace keeps a designated Owner

Leaving, removal and demotion of the last designated Owner are denied, and removing the Organization Membership of a
sole Owner needs a transfer target. The rule counts Owners on record, enabled or not. Only the Organization needs a
count of enabled Owners, because nothing sits above it; a Workspace has the Organization Owner above it. So suspending
a person never changes ownership, and there is no "needs an Owner" state. The accepted cost: a Workspace whose remaining
Owner is disabled has nobody to administer it until an Organization Owner assigns someone. Rejected: archiving a
Workspace automatically when its last Owner leaves, which would stop every Agent and Member in it.

## Identity-provider deprovisioning

When Coral adds SCIM or a similar feed, deprovisioning disables the User and never removes the Organization Membership,
even on a hard delete. Removal deletes credentials and Saved Functions and can need a transfer target, so a person
decides it. A manual disablement stays in force when the identity provider later reports the User as active.
Deprovisioning the last enabled Organization Owner fails.

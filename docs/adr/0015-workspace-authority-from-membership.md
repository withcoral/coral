---
status: accepted
---

# Workspace authority comes only from a Workspace Membership

An Organization Owner used to hold Owner authority in every Workspace without a Membership. We wanted an IT admin who
can see every Workspace and recover one, and that answer was much bigger than the need: the Organization Owner became a
working Owner everywhere and turned up in Workspaces nobody had invited them to. Authority inside a Workspace now comes
only from a Workspace Membership. An Organization Owner keeps a small governance surface over Workspaces they are not
in: list them with their state and designated Owners, assign a designated Owner (including themselves), archive, and
restore. Assigning themselves is the explicit, recorded way to do setup in a Workspace.

This supersedes two statements in [ADR 0007](0007-defer-organization-providers.md): that inherited Workspace Owner
authority remains unchanged, and that inherited Organization Owner access prevents treating a Membership removal as
loss of access. Removing a Workspace Membership now always ends access there and always clears the User's stored
Provider Account credentials and current Saved Functions. It also reverses an earlier
resolution that Coral need not preserve a designated Workspace Owner.

## Every Workspace keeps a designated Owner

Leave, removal, and demotion of the last designated Owner are denied. Removing an Organization Membership needs a
transfer target when the User is the sole designated Owner of a Workspace; the target becomes Owner of each such
Workspace in the same transaction as the removal and its cleanup.

The rule counts Owners on record and ignores whether their Organization Membership is enabled. The last-enabled
Organization Owner guard counts enabled Owners because nothing sits above the Organization to recover it. A Workspace
has the Organization Owner above it, who can always assign another Owner, so one count is enough. Suspending a person
never changes ownership and only deleting them forces a transfer. This removes a second notion of "ownerless" (an Owner
on record who cannot act) and with it any "needs an Owner" state. The cost is one case we accept: an enabled Owner can
leave a Workspace whose only other Owner is disabled, and it then has nobody to administer it until an Organization
Owner assigns someone. The Organization's Workspace list shows that Owner as disabled.

## Considered options

- **A separate IT admin or auditor role, or Organization-level Provider setup.** Rejected for now: ADR 0007 already
  accepts separate setup in each Workspace, and self-assignment covers the need without a new role.
- **Count only enabled Owners.** Rejected for the reason above. It also made the transfer rule and the Owner listings
  disagree about who the Owner is.
- **Automatic archiving when the last Owner leaves.** Rejected: archiving stops every Agent and every other Member.
- **No backfill at the cutover.** Rejected: every Organization Owner would lose access on upgrade and leave stored
  credentials and Saved Functions in Workspaces they could no longer open.

## Consequences

- **The cutover backfills.** One migration makes every enabled Organization Owner a designated Owner of every existing
  Workspace and promotes one who is already a Member there. Those Owners hold that authority today; the backfill makes
  it visible and removable and grants nothing new. Its Administrative Lifecycle Events carry each Workspace's last
  update time and a fixed trace identifier, because a migration has no clock or trace. A User who becomes an
  Organization Owner later gets no Workspace Membership from that role.
- **Organization Owner governance is a recorded authority source.** Owner assignment, archive, restore, and the transfer
  on Organization Membership removal record `organization_owner`. No other runtime action does; the backfill's events carry it too.
- **Agent credentials outlive their creator.** Disabling or removing a User does not rotate the bearer credential of an
  Agent they created or saw. The Workspace owns the Agent, and another Owner rotates the credential when needed.
- **Assignment needs an active Workspace.** An Organization Owner restores an archived Workspace before assigning an
  Owner. The transfer on Organization Membership removal still applies to archived Workspaces.

## Automated deprovisioning

Coral has no SCIM or other identity-provider deprovisioning today. When it is added, it follows this rule:

- Deprovisioning disables the User. It never removes the Organization Membership, including when the identity provider
  sends a hard delete. Removal deletes stored Provider Account credentials and Saved Functions and can need a transfer
  target, so a person must decide it.
- A manual disablement by an Organization Owner stays in force when the identity provider later reports the User as
  active. Track the two reasons separately.
- Deprovisioning the last enabled Organization Owner fails with a clear error.

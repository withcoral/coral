---
status: accepted
---

# A change takes its precondition as an argument, and a resource's version lives on its row

Each resource that a management change can conflict on keeps a version on its own row: a positive integer that the
statement which changes the row raises by one. A method whose change can conflict takes the version the caller read as
a typed argument, and a method that must not run twice takes a typed request id. Neither travels in the change
context, so such a method cannot be called without one, and a method that has no such argument checks nothing. The
etag that an Adapter shows is the version in the text form its type owns, and a client treats it as opaque.

Before this, Coral did the conflict check in three ways. A Workspace, a Workspace Membership, an Agent and an Agent
Registration each had a random token in a separate table of concurrency metadata, which every change replaced, and
that table was the only place that held an Agent's and a Registration's creation and update times. An Organization
Membership counted its revisions on its row. A Provider counted its versions on its row and showed a token derived
from its identity and that count. The etag and the request id travelled as optional fields of the change context. So
a caller could leave the check out without the compiler noticing, and a method that checked nothing accepted an etag
and ignored it.

The etag of a Workspace, a Workspace Membership, an Agent, an Agent Registration and an Organization Membership is its
version as a plain decimal number. A Workspace has one version for its name and its lifecycle state, so a rename
raises it as an archive or a restore does. The type that holds a version reads only the text it writes, so one version
has one spelling. An etag of the earlier form, a UUID, stays well formed: Coral refuses it as stale, not as invalid.
A page that was open across the upgrade then asks the person to reload it, and does not blame the form's fields.

A request id is a UUID in any form, and is stored in one. Two requests with the same id from the same User are the
same request, and the second is refused.

## Considered options

- **Keep the precondition as an optional field of the change context.** Rejected: the signature cannot require it, and
  a method that checks nothing accepts one silently.
- **Keep a random token, and copy it onto the row.** Rejected: a data migration cannot make one in portable SQL, so an
  earlier backfill had to borrow another identifier, and three other resources already count.
- **Stagger the move over four releases, so that no two builds ever disagree.** Rejected at first for the risk below,
  in exchange for two releases. The move then shipped over four anyway, and that is the safer order.

## Consequences

- This record states where the changes end. They arrive in steps: until the change that types every call site lands,
  the etag and the request id still reach a method through the change context, built on the same typed values.
- A Provider's etag keeps its text, the token derived from its identity and its version. Only its argument is typed.
- A Workspace Membership that is removed and added again starts at the first version, so an etag from before the
  removal can match it. The rule that keeps one designated Owner still holds, and the role is the only other field a
  change can lose.
- A Workspace Membership that is added again shows the time of that grant as its creation time.
- Tests may write without a precondition through a value that exists only in test builds.
- The move takes four releases, one step each. 1.9.0 adds the version and the times to each row, and raises and
  writes them with every change, while it still shows the random token and checks it against the table of
  concurrency metadata. 1.11.0 reads the version and the times from the row, and still writes the table. The release
  after it stops writing the table, and the one after that drops it. Two releases that follow one another agree on
  what a change is checked against: each reads only what the one before it writes.
- On `main` the steps merged one at a time, and each finished its rollout before the next merged. Nothing in the code
  enforces that order. A deployment that tracks `main` and skips a build has the window below.

### The risk we accept

A self-hoster on Postgres who does a rolling upgrade across more than one of those steps, such as from 1.10.0 straight
to the release that stops writing the table, runs both on one database for a while. A rolling upgrade from one step
to the next has no such window. On SQLite one process holds the state root, so two releases never serve one database.
In that window:

- **A stale save can pass.** A release before 1.9.0 never raises a row's version, which a release from 1.11.0 checks.
  A release before 1.11.0 checks the random token against the table, which a release that stopped writing it never
  changes. Each older side hides its changes from the newer one, or misses the newer one's.
- **The older release cannot read what the newer one creates.** A release before 1.11.0 finds no row in the table
  for a Workspace, a Workspace Membership, an Agent or an Agent Registration that a release which stopped writing it
  created, and reports corrupt data for the list that holds it.
- **A save can be refused with a misleading message.** A release before 1.9.0 rejects a version as a malformed etag,
  which Reef shows as a problem with the form's fields.
- **An Agent that a release before 1.9.0 creates has no times on its row.** That release does not know the new
  columns. A later release reads a missing time as absent, and the release that drops the table fills the time from
  it first.

We accept this because the self-host guides stop Coral for an upgrade: the Kubernetes guide runs one replica and
replaces it, so two releases never serve side by side. Rolling upgrades are planned in
[#3138](https://github.com/withcoral/coral/issues/3138). The window ends when the last Coral Instance of the older
release stops, and it cannot open again: that release refuses to start on a database that a newer one has migrated.

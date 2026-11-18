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
- **Stagger the move over four releases, so that no two builds ever disagree.** Rejected for the risk below, which we
  accept in exchange for two releases.

## Consequences

- A Provider's etag keeps its text, the token derived from its identity and its version. Only its argument is typed.
- A Workspace Membership that is removed and added again starts at the first version, so an etag from before the
  removal can match it. The rule that keeps one designated Owner still holds, and the role is the only other field a
  change can lose.
- A Workspace Membership that is added again shows the time of that grant as its creation time.
- Tests may write without a precondition through a value that exists only in test builds.
- The move takes two releases. The first adds the version and the times to each row and raises and writes them with
  every change, then reads them, then stops writing the table of concurrency metadata. The second drops that table.
  Inside the first release these three changes merge one at a time, and each finishes its rollout before the next
  merges, so that two builds that follow one another agree on what a change is checked against. While only the first
  of them is deployed, Coral still shows the random token and checks it against the table.
- Nothing in the code enforces that order. A deployment that tracks `main` and skips a build has the window below.

### The risk we accept

A self-hoster who does a rolling upgrade from the release before the first one, straight to it, runs both on one
database for a while. In that window:

- **A stale save can pass.** The older release checks only the table of concurrency metadata and never raises a
  row's version. The newer release checks only the row's version and never writes the table. Each side misses the
  other's changes.
- **A save can be refused with a misleading message.** The older release rejects a version as a malformed etag, which
  Reef shows as a problem with the form's fields. A token sent to the newer release is a stale etag.
- **An Agent that the older release creates has no times on its row.** That release does not know the new columns. The
  newer release reads a missing time as absent, and the second release fills it from the table before it drops it.

We accept this because the self-host guides stop Coral for an upgrade: the Kubernetes guide runs one replica and
replaces it, so two releases never serve side by side. Rolling upgrades are planned in
[#3138](https://github.com/withcoral/coral/issues/3138). The window ends when the last Coral Instance of the older
release stops, and it cannot open again: that release refuses to start on a database that a newer one has migrated.

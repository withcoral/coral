---
status: accepted
---

# Universal Search never gates the catalog

Universal Search is a projection of the catalog. It never changes whether an Operation can be called, and no Setup or
Publication waits for it. An index bound or an indexing failure is logged when the index is written and is never a
reason to refuse a call. An agent that cannot find an Operation through search can still call it.

Search therefore prepares its entries from the stored Generation after the commit, not during materialization. When the
walk that builds search material ran inside materialization, a bound on it (one declaration field over 256 KiB) made an
Operation unavailable, and its cost (about 13 s on Microsoft Graph) landed on every Setup preview.

Rejected: keeping the walk in Publication so that the index activates atomically with the Generation. The reconcile task
already tolerates the gap, and retrieval reads only each Provider's active Generation.

## Source changes never wait for search

The rule covers deletes too. A write to a source never reads or writes the search tables, and those tables hold no foreign
key to a Saved Function or a Provider Generation. Every Saved Function change goes through one rule that reads the function
by id: index what is stale, remove what is gone, otherwise do nothing. A notification after commit names ids alone, so it may
run late, twice or out of order, and a lost one costs only time: a reconcile pass runs the same rule over every function it
finds stale or orphaned, by default every five minutes. Provider Generations reach the same end by their own paths: each
notification names ids, and a pass removes what is no Provider's active Generation. Reads answer only from live sources, so
a deleted function leaves search at once, before its row does.

A cascade from Saved Functions into `search_entities` made a delete wait for any index write that held the row, and on
Postgres the two could deadlock (#2684).

Each search table keeps its foreign key to `workspace_records`. No user action deletes a Workspace, so that cascade is not
on any request path, and it removes a tenant's search text when a host removes the Workspace.

Rejected: taking the Workspace's index lock in the delete, or holding the function row in the index write. Each fixes this
deadlock, but the first makes a delete wait for search and the second leaves the lock order a rule every writer must
remember. Rejected: a transactional outbox. Reconcile compares state, so it finds a lost notification without a queue, and
an outbox would add a write to every source transaction.

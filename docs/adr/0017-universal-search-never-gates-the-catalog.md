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

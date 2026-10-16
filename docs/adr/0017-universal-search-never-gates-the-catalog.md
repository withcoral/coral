---
status: accepted
---

# Universal Search never gates the catalog

Universal Search is a projection of the catalog. It never changes an Operation's availability, and no Setup or
Publication call waits for it. The index logs a bound or an indexing failure when it writes the Generation; neither is
ever a reason to refuse a call. How Reef shows them is a separate decision (#2469).

This replaces one sentence of ADR 0011, "the entries the owner prepared". The decision: the workflow that commits a
Provider Generation tells search that the Generation is active, and search prepares the entries from the stored
Generation after the commit, the way the reconcile task already does. Publication still decides what may be
searched, through availability. Search decides nothing about availability.

Issue numbers prefixed `lagoon` are in withcoral/lagoon, Coral's previous repository; bare numbers are in this one.

## Why

Until lagoon #366, one Operation over the search bound rejected the whole catalog; Slack and Xero both hit it. #366
kept a remnant: a declaration field over 256 KiB still marked its Operation Unavailable, so Coral refused to invoke
it. That bound was a copy of the 256 KiB Authoring Description budget with no reason of its own, and the stored lane
column it once protected went in lagoon #430. Lagoon #116 and #186 framed an index failure as "that Operation's
diagnostic and availability". That framing is withdrawn. An agent that cannot find an Operation through search can
still call it, and an Operation the catalog accepts is callable whatever search makes of it.

The walk that prepares search material also runs inside `materialize`, so the Setup preview and Publication both pay
for it, and the preview drops the result. On Microsoft Graph that is about 13 s per call against Reef's 10 s deadline
(#2344).

## Consequences

- Done with this record: `prepare_operation_declaration_fields` cannot fail. It truncates breadth-first at the field
  bound and warns, and a schema it cannot read stops it with a warning.
- Done in #2463: the walk leaves `materialize`, and search prepares its entries from the stored Generation after the
  commit. The walk and its field and depth bounds stay in coral-catalog, which owns the schema projection, but only
  search runs them, so they bound the index and never the catalog. Their warnings leave the Operation; the index logs
  them when it writes the Generation, and #2469 decides how Reef shows them.
- The Setup preview then shows availability from structural diagnostics only. Search coverage is reported after
  indexing.
- Rejected: keeping the walk in Publication so the index activates atomically with the Generation. The reconcile task
  already tolerates the gap, and retrieval reads only each Provider's active Generation.

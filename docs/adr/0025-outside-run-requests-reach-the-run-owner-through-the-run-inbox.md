---
status: accepted
---

# Outside Run Requests reach the Run Owner through the Run Inbox

A decision on an Approval Request or a cancellation can arrive at any Coral Instance, but a Run's Run Owner runs only in
the Coral Instance that holds its claim ([ADR 0018](0018-run-owner-coordinates-each-run.md)). The others used to answer
`OwnedElsewhere`, so with two Coral Instances behind a load balancer about half of these requests failed. Reef, which
holds one connection to one Coral Instance, failed every request for a Run that the other one owned.

The receiving Coral Instance now inserts the request into the Run's **Run Inbox**, rows in one table in the Coral
database, and answers once the request is Recorded. The owning Coral Instance reads the Run Inbox for the Runs it holds
and hands each row to the Run Owner, which applies it in its one order with everything else and deletes the row in the
chunk commit that records it. Every outside request takes this path, even when the owner is local. The Run row records
the per-boot id of the Coral Instance that holds its claim.

The database still holds no Run work queue. The Run Inbox holds only outside requests, a few per Run at the pace of a
person, and Program requests never use it.

## The seam

The Run Inbox sits behind a trait in `coral-run`, and the table is its only implementation today. Any implementation
promises:

- Requests are addressed by Run, not by Coral Instance.
- There is one request per decided thing (the Approval Request, or the Run for a cancellation), and the first wins.
- Each request is delivered at least once, and applying a repeat changes nothing.
- A request is deleted only once it is Recorded.

A broker could replace the table if it keeps these promises, acknowledging a message only after the chunk commit that
records it.

## Consequences

- **A waiting request survives the owner's death.** A recovered owner applies the Run's passed timers, then its Run
  Inbox in insert order, then ends the Run (ADR 0018, Recovery). A cancellation waiting when the owning instance died
  ends the Run `canceled`, unless the Run's deadline passed first. A panicked owner's supervisor does not read the Run
  Inbox, so there a waiting cancellation ends with the Run `interrupted`.
- **Delivery lags slightly.** A decision made just before its Approval Request's deadline can end as expired.
- **A requester may hear "not yet recorded".** Past a short wait the answer is retryable. No requester is told a
  request was Recorded when it was not.
- **Scale-out needs every single-process assumption gone.** Several Coral Instances may own Runs and serve outside
  requests against one Postgres database. MCP in-band elicitation kept a session on one instance until
  [ADR 0026](0026-mcp-requests-are-served-without-sessions.md) removed MCP sessions.

## Rejected

- **Forwarding to the owner over an internal endpoint.** It needs addressable, mutually authenticated instances and
  loses the request when the owner dies mid-call.
- **A message broker now.** It would be a second source of truth beside the database, another stateful service for
  every self-host and an in-process stand-in for Desktop, for a few requests that need no instant push.
- **A Postgres queue extension.** It has no SQLite equivalent and adds nothing the table lacks at this volume.
- **A proxy that routes by key.** Deploys and scale events still move Runs, so the inbox is still needed.
- **An in-memory path for a local owner.** It gives two behaviors, and a cancellation that does not survive the owner.
- **Answering when the row is inserted.** Clients could not act on an answer that precedes the record.
- **A stable Coral Instance name.** It is safe only if two live instances can never share it.

---
status: accepted
---

# Outside Run Requests reach the Run Owner through the Run Inbox

A decision on an Approval Request, or a request to cancel a Run, can arrive at any Coral Instance. The Run Owner runs in
only one of them: the Coral Instance that holds the Run's claim. Under
[ADR 0018](0018-run-owner-coordinates-each-run.md), a Coral Instance that does not host the Run's Run Owner answers
`OwnedElsewhere`. So with two Coral Instances behind a load balancer, about half of these requests fail. Reef holds one
connection to one Coral Instance, so every request it sends for a Run owned by the other Coral Instance fails.

This decision makes these requests work on every Coral Instance:

- The Coral Instance that receives a request puts it in the Run's **Run Inbox**. Every Run's Run Inbox is a set of
  rows in one table in the Coral database.
- The Coral Instance that owns the Run reads the Run Inbox and gives the request to the Run Owner.
- The Run Owner records the outcome and deletes the row in the same chunk commit.
- The receiving Coral Instance answers once the request is Recorded.

This amends ADR 0018 in the places named below. Epic [#2676](https://github.com/withcoral/coral/issues/2676) carries
the implementation.

## The shape

- **Run Inbox.**
  - **One row per decided thing.** Each outside Run Request is identified by what it decides: the Approval Request
    for a decision, the Run for a cancellation. While a row waits, a second insert for the same thing changes
    nothing. Once the row is deleted, the record keeps the first answer: a later request for the same thing is a
    repeat, and the owner answers it from the record.
  - **The insert.** The receiving Coral Instance checks the caller's access as it does today. A caller who may not
    see the Run or the Approval Request gets not-found. The instance then inserts the row in a short transaction. A
    snapshot check confirms the Run is visible and not terminal. The transaction takes no lock on the Run row. If the
    Run is terminal, the request is answered from the record, as today.
  - **The signal.** The insert's commit sends the change signal.
- **Coral Instance.**
  - **Its id.** Each Coral server process generates a random id each time it starts, and passes it to `RunModule`.
  - **On the Run row.** The Run row records the id of the Coral Instance that holds its claim. The id is written with
    the claim: at acceptance, and at every takeover.
  - **Names.** The id is not a host name or a pod name. Those appear only in logs and traces.
- **Delivery.**
  - **The Run Inbox reader.** Each Coral Instance has one Run Inbox reader, always subscribed to the change signal.
    It reads the Run Inbox rows of every Run whose claim this Coral Instance holds. It hands each row to that Run's
    Run Owner.
  - **When it reads.** When the change signal fires, at most once every 250 ms. Also when it subscribes, after a
    signal outage, and every 15 s as a backstop.
  - **A local owner.** When the Run Owner runs in the receiving Coral Instance, that instance also wakes its Run
    Inbox reader at once, in memory, so it does not wait for the signal. The wake-up carries nothing. The request
    still arrives through its row.
  - **One path.** Every outside request takes this path, whether the owner is local or not.
- **Recording.**
  - **Applying a request.** A request read from the Run Inbox joins the Run Owner's one order, with Program requests,
    Invocation outcomes, deadlines and wake-ups. The owner applies it as it applied the in-memory request before.
  - **Repeats.** A request can be delivered more than once. A request changes nothing if a request for the same
    Approval Request or Run is already Applied or Recorded.
  - **Deletion.** The owner deletes every row it applies, in its next chunk commit. That includes a repeat that
    changes nothing. Once that commit lands, the row is not delivered again. If the owner stops before it lands, the
    row stays and is delivered again.
  - **Deadlines.** The owner applies deadlines and decisions in its one order, in its pure `apply`, as before. A
    deadline's timer decides when its message is applied. A decision or a cancellation applied first, on the owner's
    clock past the deadline, finds the deadline passed and loses to it. So a decision applied after its Approval
    Request's deadline loses to the expiry, and its requester gets the recorded expiry. Delivery can lag by up to
    250 ms, or up to the 15 s backstop during a change-signal outage. So a decision made just before the deadline can
    end as expired. The Dispatch Fence checks an approved request's deadline at dispatch, and it records no
    expiry.
  - **Late rows.** A row may be inserted just as the Run ends. It stays until retention deletes the Run. No reader
    reads the rows of a terminal Run.
- **The answer.**
  - **The wait.** The receiving Coral Instance waits until the request is Recorded. Then it answers as it does today.
  - **A decision** waits up to 5 s.
  - **A cancellation** waits up to the shutdown grace plus a settle margin (5 s + 1 s = 6 s today), counted from when
    the request arrives. It waits first for the cancellation to be Recorded, then for the Run to end. If the
    cancellation is Recorded but the Run has not ended in time, the answer is the Run as recorded, as today.
  - **Past the bound.** A request that is still not Recorded gets the answer "not yet recorded". That answer is
    retryable. A retry finds the row still waiting, the request Recorded, or the Run ended without it. In the last
    case the answer comes from the record.
  - **`OwnedElsewhere`.** It is retired.
- **On the wire.**
  - **gRPC.** "Not yet recorded" is `UNAVAILABLE` with the reason `RUN_REQUEST_NOT_YET_RECORDED`.
  - **MCP.** The tool result is an error with the code `run_request_not_yet_recorded`. Sometimes a User answers an
    Action Prompt during `exec` or `wait`, and the decision gets "not yet recorded". Coral then reports no error, as
    it does for a prompt whose action already ended. The Run update that follows shows the recorded state.
  - **Reef.** Reef retries once. It then shows a short status until the activity stream shows the outcome.
- **Recovery.** This covers the expired-lease sweep.
  - **Order.** A recovered Run Owner applies the Run's passed timers in deadline order, then the requests in its Run
    Inbox in insert order. Then it ends the Run. The Run ends with the termination it recorded or found. Without
    one, it ends `timed_out` past its deadline, else `interrupted`.
  - **A waiting cancellation.** Suppose a cancellation was waiting in the Run Inbox when the owning Coral Instance
    died. The Run then ends `canceled`, not `interrupted`, unless the Run deadline passed first. The timers come
    first, so the Run then ends `timed_out`.
  - **A waiting decision.** It is recorded unless its Approval Request's deadline passed first, when the expiry
    stands. Either way its Invocation does not start: the terminal chunk ends it.
  - **The rule.** Every request that reached the Run Inbox before the owning Coral Instance died is applied before
    the sweep ends the Run. Any request kind added to the Run Inbox later follows the same rule.
  - **An owner task that panics.** Its supervisor still ends the Run at once in one chunk, as ADR 0018 says, and
    does not read the Run Inbox. A request waiting then is answered from the record. A waiting cancellation ends with
    the Run `interrupted`.
- **The seam.** The Run Inbox sits behind a Rust trait (an interface) in `coral-run`.
  - **Its two halves.** One half submits a request and returns its answer. The other half is the stream of requests
    addressed to Runs this Coral Instance owns.
  - **What any implementation promises.**
    - Requests are addressed by Run, not by Coral Instance.
    - There is one request per decided thing, and the first wins.
    - Each request is delivered at least once.
    - A request is deleted only once it is Recorded.
  - **The table implementation.** It is injected at bootstrap. The answer is read from the Approval Request or the
    Run as recorded. The chunk commit that records a request deletes its row.
  - **A broker implementation.** A broker can replace the table if it keeps these promises. It acknowledges each
    message to the broker only after the chunk commit that records it. It sends the answer back to the receiving
    Coral Instance itself.

## What this amends in ADR 0018

- **Contract line 4** becomes: "No Event after `run.ended`. While the Run is nonterminal, only the owner writes
  Run-scoped rows, with one exception: the Coral Instance that receives an outside Run Request inserts it into the
  Run's Run Inbox. While the Run is nonterminal, only the owner deletes that row."
- **Contract line 8** predicted this change, and nothing needs reversal. The in-process mailboxes for outside
  requests become the Run Inbox. The Run row records the Coral Instance that holds the claim.
- **Run Request.** Outside requesters still answer at Recorded. `OwnerLost` is now returned only to Program requests.
  An outside request waits in the Run Inbox, so it survives the owner's death.
- **Recovery.**
  - A recovered owner applies the Run Inbox between the passed timers and "finish the recorded termination" or
    "interrupt". The supervisor's one-chunk interruption after an owner task panics is unchanged.
  - "No process touches a Run with a live lease it does not hold" gains one exception: any Coral Instance may insert
    into that Run's Run Inbox.
- **Rollout.**
  - Every outside Run Request goes into the Run Inbox, whichever Coral Instance it reaches. None is answered
    `OwnedElsewhere`.
  - "Concurrent processes in regular operation are out of scope" becomes: several Coral Instances may own Runs and
    serve outside Run Requests against one Postgres database at once. Other single-process assumptions stay out of
    scope (see Out of scope).
- **"Rolling updates are supported; scale-out is not".**
  - Once every Coral Instance runs this release, a decision or a cancellation sent during the Shutdown Drain reaches
    the draining owner from any Coral Instance. A Run waiting on a person no longer ends `interrupted` just because
    the decision reached another Coral Instance.
  - Scale-out is still not supported. The self-host guides keep `replicas: 1` until every single-process assumption
    is resolved.
- **The title still holds.** The database still holds no Run work queue.
  - The Run Inbox holds only outside Run Requests. A Run has a few such rows at most, at the pace of a person or an
    agent.
  - Program requests still reach the owner from its Program, never through the Run Inbox. The Run Executor map
    ([#2629](https://github.com/withcoral/coral/issues/2629)) decides how they travel once a Program runs outside the
    Coral Instance. The Run Ledger and the Dispatch Fence are still written only in chunk commits.
  - No lock on the Run row orders a Run's work, as `hold_run` did.

## Rollout

The migrations are expand-only: a new table, and a nullable `holder_instance_id` column on the Run row. While a
deployment rolls out this release, some Coral Instances still run the previous release. A Coral Instance on the
previous release never reads the Run Inbox:

- A Coral Instance on this release may put a request in the Run Inbox of a Run that a previous-release instance owns.
- That request waits until the Run ends, usually at the end of that instance's Shutdown Drain.
- The requester gets "not yet recorded", and a retry reads the record.

In the other direction, a previous-release instance still answers `OwnedElsewhere` for a Run owned elsewhere.

This is the degradation ADR 0018 already accepts. The difference is that the requester hears "not yet recorded" only
after the wait bound. No requester is told a request was Recorded when it was not.

## Out of scope

- **MCP in-band elicitation.**
  - On the 2025-11-25 protocol, Coral asks an MCP client in-band while one tool call waits. That session lives in one
    Coral Instance, so the client must keep reaching that same instance. This ADR does not change that.
  - The MCP server is expected to move into its own process and call Coral's API. Deciding, canceling and answering
    prompts all go through `RunModule`, so they use the Run Inbox whatever the transport.
- **Account Connection Request endings.**
  - They keep waking parked Invocations through the change signal. That already works across Coral Instances on
    Postgres: ending a request writes an Administrative Lifecycle Event with `execute_change`, and that commit sends
    the signal.
  - They are the next candidate to move into the Run Inbox. That move would widen the Run Inbox beyond outside Run
    Requests.
- **The self-host guides.** They keep `replicas: 1` until no single-process assumption remains.

## Rejected

- **Forwarding to the owning Coral Instance over an internal endpoint.** It needs addressable instances and
  authentication between them. It loses the request when the owner dies during the call.
- **A message broker now** (RabbitMQ, Redis, NATS).
  - The database records the outcome, so a broker would be a second source of truth beside it.
  - Every self-host would have to run another stateful service.
  - Desktop would need an in-process stand-in.
  - These requests are few and need no instant push.
  - The seam keeps a broker possible later.
- **A Postgres queue extension.** It adds an extension dependency, has no SQLite equivalent, and adds nothing the
  table lacks at this volume.
- **A proxy that picks a Coral Instance by hashing a key such as the Task identifier.** Every deploy or scale event
  moves Runs to different instances, so the Run Inbox is still needed. A proxy could only cut cross-instance hops.
- **A change subscription per Run Owner.** Each Run Owner would read the Run Inbox every time the change signal fires.
  That is one read per live Run instead of one per Coral Instance.
- **Keeping the in-memory path when the owner is local.** It gives two behaviors, and a cancellation that does not
  survive the owner's death.
- **Answering when the row is inserted.** The answer would come before the request is Recorded, so Reef and MCP
  clients could not act on it.
- **A stable Coral Instance name, so a restarted container recovers its Runs at once.** It is safe only if two live
  Coral Instances can never share the name.
- **Marking rows consumed instead of deleting them.** The Approval Request and the Run already record who decided what,
  and when.

## Consequences

- **The upgrade path.** If Run Inbox reads prove costly in measurements, the upgrade is a targeted notification: a
  `NOTIFY` on a channel named after the owning Coral Instance. The owner's contract does not change.
- **During a change-signal outage.** Delivery falls back to the 15 s backstop. A request for a Run owned by another
  Coral Instance may answer "not yet recorded" until the signal returns.
- **Proof.**
  - Rust domain tests on both backends: two `RunModule`s with different Coral Instance ids share one database.
  - A Reel deployment variant: two Coral Instances on Postgres behind a round-robin proxy.
    - Reef decides and cancels a Run owned by the other Coral Instance, over its one connection.
    - An MCP client on the 2026-07-28 protocol answers an approval through the other Coral Instance.
    - An Account Connection Request completed on the other Coral Instance wakes a parked Invocation.
  - The load harness gets no new cell for this.

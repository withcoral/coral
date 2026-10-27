---
status: accepted
---

# One Run Owner per Run writes the Run Ledger; the database holds no Run work queue

A Program Run is coordinated by one in-process **Run Owner**: an actor that exists from Run acceptance until the Run is
terminal and is the only writer of that Run's **Run Ledger**. Every Run-scoped request reaches its mailbox as a **Run
Request** or a wake-up. The owner applies them in arrival order in memory, builds chunks, and commits each chunk in one
short transaction predicated on the Run's execution claim id and the ledger head it was built on. Admit, wait, settle,
decide and cancel happen in owner memory. Nothing holds a database connection while a Program waits for a person, a
credential refresh or an upstream call.

This replaced a `hold_run` lock on the Run row that served as the Run's work queue, Event ledger and authority fence at
once. [Map #2389](https://github.com/withcoral/coral/issues/2389) holds the reasoning and rejected alternatives for each
part. Outside requests reach the owner through the Run Inbox
([ADR 0025](0025-outside-run-requests-reach-the-run-owner-through-the-run-inbox.md)).

## Why

An admission burst, amplified by round-trip latency, queued one Run's parallel Invocations for pooled connections
behind `hold_run`, and sign-in, readiness and other Workspaces waited in the same pool. The cost scaled with calls ×
round trip: 30 approval-gated calls took 13.3 s to become visible at +5 ms of latency and 45.3 s at +20 ms. Every
surveyed system with prefix-complete per-entity order has one logical history writer per entity and fences a stale
writer in the predicate of the history write itself. A lock without a write fence is the shape the evidence rejects.

## The shape

- **Run Owner.** Created in the acceptance transaction, which also writes its claim, and retired at termination. It owns
  Event persistence and the Run's lifecycle: the Dispatch Fence, the pre-send terminal check, deadlines, wait prechecks
  and every outcome decision. Decryption and the upstream call run in the dispatch it starts and report back through
  its mailbox. Every re-claim mints a new claim id, and that id in every commit predicate fences a stale owner.
- **Run Request.** Admit, Emit, Log and Finish come from the Program; Decide Approval and Cancel come from outside. A
  request is Applied (decided in memory), then Recorded (its chunk committed), then Settled (what it started has ended).
  The Program shares the owner's fate, so it is answered at Applied; outside requesters are answered at Recorded. A
  request that decides something is identified by the entity it decides (an Approval Request, a Run, an Invocation):
  the first wins, and a repeat gets the recorded answer. Timers share the mailbox, so the clock never decides state
  outside the owner.
- **Run Ledger.** The Run row's `chunk_sequence` is the head. The owner commits whenever it is free, not on a timer. A
  cap on Applied but unrecorded output bounds what an owner crash can lose; it never throttles admission. Every chunk
  commit renews the worker lease. A fenced owner acts on its commit's evidence: a claim mismatch is definitive, a head
  mismatch under a matching claim means the chunk landed, and no answer means reread and resolve.
- **Run Output Budget.** Exact, decided at apply time against the owner's in-memory total.
- **Dispatch Fence.** Still a held transaction, but one per batch of ready Invocations, merged into the next chunk
  commit. Run-scoped facts come from owner memory, and the database predicate keeps only the claim and the Invocation's
  `pending` state. Authority facts are read under shared locks, so concurrent fences do not serialize and an authority
  writer waits for the fences in flight. Decryption runs outside the fence.
- **Fan-out Limit.** Only a bounded number of one Run's Invocations dispatch at once. Admission stays eager, and
  approval and account waits hold no slot, so every Approval Request is visible at once.
- **Wake-ups.** The change signal stays content-free. An owner subscribes only while an Invocation is parked on a
  connection request; a periodic snapshot precheck of parked waits covers everything else. A wake-up is a hint, never
  evidence.
- **Recovery.** The worker lease is ownership: no process touches a Run whose live lease it does not hold, except to
  insert into its Run Inbox. An expired-lease sweep, run at boot before any listener binds and periodically after,
  gives each abandoned `running` Run an owner. That owner applies the Run's passed timers in deadline order, then the
  requests waiting in its Run Inbox, then writes one chunk that finishes the recorded termination or interrupts the
  Run. Coral never resumes a Run. When an owner task ends without retiring, as a panic does, its supervisor takes over
  the claim and writes the same one-chunk interruption.
- **Rollout.** A deploy may briefly run an old and a new process side by side. On shutdown a process reports not
  ready, refuses new Runs and waits the Shutdown Drain for its live Runs, then interrupts the rest. Whatever does not
  commit is swept once its lease expires.
- **Pool.** One pool, with no connection classes or reserved capacity. A Run holds at most one connection at a time.

## The contract every slice keeps

1. Events are prefix-complete per Run: once a sequence is visible, every lower one is committed. Gapless numbering is
   not required.
2. The Run Output Budget is exact. A Run never exceeds it by a byte.
3. Dispatch starts only after one authoritative Dispatch Fence against current authority, policy, account and approval
   facts. A revocation that commits first always wins; a change committed after the recorded dispatch start does not
   stop that dispatch. A cached decision never authorizes.
4. No Event after `run.ended`. While the Run is nonterminal, only the owner writes Run-scoped rows, with one
   exception: the Coral Instance that receives an outside Run Request inserts it into the Run's Run Inbox. Only the
   owner deletes that row.
5. Every commit that writes the Run Ledger is predicated on the claim id and the head. Never the clock.
6. An Approval binds one exact Invocation; approvals stay independent decisions. The first durable decision wins.
7. The owner code is identical on SQLite and Postgres; backend-specific idioms live in `coral-db`.
8. Nothing here assumes one process: the fence is in the write predicate and the owner is found by its claim.

## Rejected

- A shared durable head allocator with owner renumbering, or two Event streams merged at read time: optimistic appends
  degrade to one winner per round trip under the burst.
- A versioned decision in place of a held fence: a snapshot compare plus a separate compare-and-set is unsafe under
  READ COMMITTED.
- A fence transaction separate from the chunk commit, and decryption inside the fence.
- Lazy admission, an admitted-count cap, and approximate output accounting.
- Resuming a Run after restart; recovering lazily on read; taking over a live lease at boot, which would interrupt the
  other process of a rolling deploy; taking over a claim on demand, which would turn a decision into an interruption.
- Connection classes, a second pool, or reserved permits.

## Consequences

- **Freshness is not bounded.** A reader of Recorded state can trail a flooding Program by what the database can write,
  in order and never wrong. A time bound would reintroduce the per-Event wait.
- **An owner crash loses** Applied but unrecorded output, bounded by the cap, and leaves the outcome of recorded
  dispatches Indeterminate. A lost Run is never resumed, so output can be lost but never duplicated.
- **Readers never read owner memory.** `wait`, Reef and the activity stream read Recorded state, and show a countdown
  beside a recorded deadline rather than deriving state from `now`.
- **Migrations are expand-only across one release,** because a new process migrates at boot while the old one still
  serves.
- **Request work shares the pool with Run Ledger work.** Reconsider reserved permits in the one pool if the sign-in or
  readiness probes exceed their budgets while every Run transaction is short.

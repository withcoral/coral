---
status: accepted
---

# One Run Owner per Run writes the Run Ledger; the database holds no Run work queue

A Coral Program Run is coordinated by one in-process **Run Owner**: an actor that exists from Run acceptance until the
Run is terminal and is the only writer of that Run's **Run Ledger**. Every Run-scoped request reaches its mailbox as a
**Run Request** or a wake-up; the owner applies them in arrival order in memory, builds chunks, and commits each chunk
in one short transaction whose predicate is the Run's execution claim id and the ledger head it was built on. Once a Run
is claimed, only two transitions wait for the database: the chunk commit and the **Dispatch Fence**, which rides inside
a chunk commit; the claim itself is written in the acceptance transaction, renewed by every commit, and taken over only
by recovery: the expired-lease sweep or the owner's supervisor, both below. Admit, wait, settle, decide and cancel are
owner memory. Nothing holds a connection while a Program waits for a human, a credential refresh, or an upstream call.

This replaces the shape where a `hold_run` row lock on the Run row served as the Run's work queue, serial Event ledger
and live authority fence at once, with compare-and-set counters (`next_event_sequence`, `accumulated_output_bytes`) as
the real invariant underneath. The decision was charted on
[map #2389](https://github.com/withcoral/coral/issues/2389), whose decision records hold the reasoning and rejected
alternatives for each part; this record states the shape and the contract every implementation slice keeps.

Amended by [ADR 0025](0025-outside-run-requests-reach-the-run-owner-through-the-run-inbox.md): outside Run Requests
reach the Run Owner from any Coral Instance through the Run Inbox. That ADR names the contract lines and bullets it
changes.

## Why

The incident that motivated this decision was an admission burst amplified by round-trip latency, not steady-state wait or human decision
volume: one Run's parallel Invocations queued pooled
connections behind `hold_run`, and sign-in, readiness and other Workspaces waited in the same pool. After the incident mitigation the
start burst remained, scaling with calls × round trip and nothing else
(30 approval-gated calls take 0.7 s to become visible at
+0 ms, 13.3 s at +5 ms, 45.3 s at +20 ms; 101 calls at +20 ms self-interrupt on the literal pool timeout). Two surveys found that every system with prefix-complete per-entity order
has one logical history writer per entity and fences a stale writer in the predicate of the history write itself; a
lock without a write fence is the shape the evidence rejects.

## The shape

- **Run Owner**. One per nonterminal Run, created in the acceptance transaction, which also writes its claim, and
  retired at termination. It safeguards Event persistence and the Run's lifecycle: the fence, the pre-send terminal
  check, deadlines, wait prechecks and every outcome decision. Decryption and the upstream call run in the dispatch it
  starts and report back through the mailbox. Nothing outside the owner writes its memory. The claim is the only
  compare-and-set outside a chunk commit; every chunk commit carries the claim-and-head predicate, every re-claim mints
  a new claim id, and the stale-owner fence is that id in every commit predicate.
- **Run Request**. Admit, Emit, Log and Finish come from the sandbox host; Decide Approval and Cancel from outside.
  Stages: Applied (decided in memory), Recorded (chunk committed), Settled (what it started has ended). Shared fate
  decides who may act on Applied: the sandbox dies with the owner and answers at Applied, because every later effect of
  the Run is a later owner commit that carries everything applied before it; outside requesters answer at Recorded. No
  caller tokens: a request that decides something is identified by the entity it decides (Approval Request, Run,
  Invocation), first wins, a repeat gets the recorded answer; Emit and Log have no identity and no repeat, since the
  Program shares the owner's fate. `OwnerLost` means outcome unknown, reread. Timers and coalesced wake-ups share the
  mailbox. The clock never decides state outside the owner.
- **Run Ledger**. The Run row's `chunk_sequence` is the head; every chunk that writes increments it, and Event numbering
  stays data. Commit-when-free after draining the mailbox, no timer; a chunk closes at 1000 Events or 1 MiB of output;
  the Applied cap on unrecorded output bytes (default 8 MiB, bootstrap-configured, validated at ≥ 2 × the chunk bytes
  cap) makes Program requests answer at Recorded only while it is exceeded, so the cap bounds loss and memory, never the
  admission burst. Every chunk commit renews the worker lease; an idle tick renews it at half a lease. A fenced owner
  stops on its commit's evidence, by failure class: claim mismatch is definitive; head mismatch under a matching claim
  means the chunk landed; no answer means reread the head under the claim and resolve to landed, recommit as built, or
  corruption. Suspect after one lease without a commit; self-interrupt after three; retries end only on success or claim
  mismatch.
- **Run Output Budget**. Exact, zero overshoot, decided at
  apply time against the owner's in-memory total; `accumulated_output_bytes` is written as a plain value per chunk under
  the claim-id fence. Evidence on exceed keeps today's shape and both failure codes.
- **Dispatch Fence**. Still a held transaction, reshaped: one fence per batch of
  ready Invocations, merged into the next chunk commit, which writes no Event and increments the head. `FOR SHARE` on
  Postgres so fences share and an authority writer waits for in-flight fences; `BEGIN IMMEDIATE` on SQLite. Run-scoped
  facts (claim, termination, deadline, Approval state, connection waits) come from owner memory with one clock sample;
  the database predicate keeps only the claim id and the Invocation's `pending` state. One fact loader that takes the
  lock mode and one evaluator serve both the fence and the parked-wait precheck. The fence reads credential evidence
  under its locks and parks a refresh wait there; decryption runs outside, per dispatch, after the pre-send check and
  never re-fences. The terminal chunk waits for a fence in flight, so no Event follows `run.ended`.
- **Fan-out Limit**. At most 32 Invocations of one Run
  `dispatching` at once, deployment-wide and bootstrap-configured. Admission stays eager; excess ready calls wait as
  `pending` and dispatch in readiness order; approval and account waits hold no slot, so every Approval Request is
  visible at once. No admitted-count cap, rate, or per-Workspace limit.
- **Wake-ups**. The change signal stays content-free. An owner
  subscribes only while an Invocation is parked on a connection request and, per coalesced wake after a 1 s gap, reads
  just its own pending requests. A 15 s periodic snapshot precheck per owner with parked Invocations is the ceiling for
  everything else and replaces the 5 s poll and the 30 s held recheck. A wake-up is a hint, never evidence.
- **Recovery**. The worker lease is ownership: a Run belongs to the process whose owner holds its live lease, and no
  process touches a Run with a live lease it does not hold. An expired-lease sweep recovers the rest: every `running`
  Run with no local owner whose lease has lapsed, or that has no claim and was accepted more than one lease ago (the
  previous release claims a moment after acceptance; the new one claims in the acceptance transaction, so a fresh
  unclaimed Run is the hot process's, not abandoned), gets an owner with passed timers queued in deadline order, then
  "finish the recorded termination" or "interrupt"; that owner writes one chunk and retires. The sweep runs first at
  boot, before any listener binds, and then every half lease for the life of the process, so a Run whose process died
  ends within one lease and one sweep. Coral never resumes a Run. A local owner whose lease lapsed is not swept: it
  continues when its next commit shows a matching claim, and a takeover by another process reaches it as a claim
  mismatch. An owner task that ends without retiring while the process lives (a panic unwinds; nothing aborts the
  process) is caught by its supervisor through the task's join handle, which takes over the claim and writes the same
  one-chunk interruption.
- **Rollout**. A deploy may run one hot process and one cold process until the routing flip, with no routing drain.
  The cold process's boot sweep leaves the hot process's Runs alone. On shutdown the hot process reports not ready,
  refuses new Runs, and waits the **Shutdown Drain** (bootstrap-configured) for its live Runs to finish on their own;
  at the end of the drain it sends interrupt to every owner still live and waits a bounded grace for their commits.
  Whatever does not commit keeps its lease and the cold process's sweep ends it after expiry. A Run Request that
  reaches a process with no local owner for a nonterminal Run is answered `OwnedElsewhere`, a retryable answer, until
  the Run is terminal; then it is answered from the durable record. Reads are unaffected: they read Recorded state.
  Concurrent processes in regular operation are out of scope.
- **Pool**. One pool, no connection classes, no reserved
  capacity. A Run holds at most one connection at a time, for one chunk commit.

## The contract every slice keeps

1. Events are prefix-complete per Run: once a sequence is visible, every lower one is committed. One writer, serial
   chunk commits, snapshot reads. Gapless numbering is not required.
2. The Run Output Budget is exact. A Run never exceeds it by a byte.
3. Dispatch starts only after one authoritative Dispatch Fence against current authority, policy, account and approval
   facts. A revocation that commits first always wins; a change
   that commits after the recorded dispatch start does not stop that dispatch. A cached decision never authorizes.
4. No Event after `run.ended`. No writer other than the owner touches Run-scoped rows while the Run is nonterminal.
5. Every commit that writes the Run Ledger is predicated on the claim id and the head. Never the clock.
6. An Approval binds one exact Invocation; approvals stay independent decisions. The first durable decision wins.
7. SQLite and Postgres parity: the owner code is identical on both backends; backend-specific idioms live in `coral-db`.
8. Nothing decided here needs reversal for multi-process Coral: the fence is in the write predicate, the owner is found
   by the claim, in-process mailboxes become a durable inbox behind the same seam, and recovery adds a holder-instance
   column on the claim then, not now.

## Rejected

- A durable head as a shared allocator with owner renumbering, and two streams merged at read time: optimistic
  appends degrade to one winner per round trip under the burst; no surveyed system merges at read time.
- A versioned decision in place of a held fence: a snapshot compare plus a separate compare-and-set is unsafe
  under READ COMMITTED, so the write predicate was the blocker, not version coverage. No new version columns.
- A separate fence transaction, an overlap fence beside the chunk commit, and decryption inside the fence.
- Lazy admission and an admitted-count cap. Approximate output accounting with bounded overshoot.
- Resuming a Run after restart; a lazy on-read recovery rule; taking over a live lease at boot (the cold process
  would interrupt the hot process's Runs); stop-before-start as a deployment requirement; takeover on demand, where
  the process that receives a request steals the Run's claim (a decision would interrupt instead of decide).
- A scope-keyed change subscription demultiplexed in `coral-db`: the upgrade path if the harness shows the
  content-free signal ringing too often; the owner's contract would not change.
- Connection classes, a second pool, or reserved permits.

## Consequences

- **Request work shares the pool with Run Ledger work.** Reconsider reserved capacity (permits in the one pool; a
  second pool stays rejected on SQLite parity and readiness honesty) if the owner-shape harness shows the `sign_in` or
  `ready` probes exceeding their budgets at the default pool while every Run transaction is short. The harness cell with
  more concurrent Runs than pool connections exists so that evidence can exist.
- **Freshness is not bounded.** A consumer reading Recorded state can be behind a flooding Program by what the
  database can write, in order and never wrong. A time bound would reintroduce the per-Event wait.
- **The loss set of an owner crash** is Applied-but-unrecorded output and results (at most the Applied cap plus one
  chunk in flight) plus upstream effects of recorded dispatches whose outcome becomes Indeterminate. Same loss set as
  today's crash mid-dispatch; a lost Run is never resumed, so bytes can be lost but never duplicated.
- **Readers never read owner memory.** `wait`, Reef, the approval page, required actions and the activity stream read
  Recorded state from a snapshot, as today, and show a countdown beside a recorded deadline rather than deriving state
  from `now`.
- **Measurement before and after.** The load harness (PR #2392) is the comparison tool: the admission burst, `begin`
  and readiness during the burst, admit p95 and the lock-wait ceiling should move; the emit-to-visible numbers from
  the cadence spike must hold behind `RunModule` with V8, the fence and the probes.
- **Rolling updates are supported; scale-out is not.** A rollout must not crash or lose information; it may degrade.
  What degrades: a Run that does not finish within the Shutdown Drain ends `interrupted` at the hot process's shutdown;
  a cancel or decision for such a Run that reaches the cold process answers retry until the Run is terminal, so a Run
  waiting on a human during the drain ends interrupted unless the decision reaches the hot process; a Run whose process
  crashed takes up to one lease and one sweep to end. The orchestrator's termination grace must exceed the Shutdown
  Drain plus the commit grace; the hosted deployment and the self-host guides must say so before boot recovery ships.
- **Migrations are expand-only across one release.** The cold process migrates at boot while the hot process still
  serves, so a migration must keep the previous release's queries valid: add columns and tables, keep constraints the
  old code satisfies, and drop in a later release. The Run Ledger head is added beside `next_event_sequence`, which
  stays written at acceptance until the release after the one that stops reading it.
- **Every observable commit notifies.** On Postgres each chunk commit calls `pg_notify` and holds the notify lock
  through commit, as every Event write does today; chunking reduces the count, and the fence slice measures what
  remains beside the `FOR SHARE` delta.
- **Out of this decision.** The first-time `operation_access_rules` row race (an absent row locks nothing);
  sign-in's own serialisation on the Organization authority guard; pool size, acquire timeout and pool metrics
  (#2391); Reef's activity stream and `wait` subscriptions.

## Evidence

Throwaway prototypes on the `wayfinder/447-assets` branch: the ledger-writers state model, the owner-loop state
model with its burst table, and the chunk-cadence Rust spike. The load harness and its baseline on
today's shape are in PR #2392. None of them prove integration with the real engine, authorization or both
backends; the implementation slices under #2378 do.

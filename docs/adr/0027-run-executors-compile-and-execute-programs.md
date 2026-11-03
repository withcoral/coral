---
status: accepted
---

# Run Executors compile and execute Coral Programs outside the Coral process

Every V8 isolate that compiles or runs Principal-authored source now runs in a **Run Executor**, a process apart from
Coral's control plane. The Coral process starts no V8 isolate. A Run Executor's resource use and failures stay with the
Runs it holds, and Runs can execute on another runtime for a hosted offering. A Run Executor is the same `coral` binary
as Coral. The in-process V8 engine stays only as a test engine.

A Run Executor dials Coral and takes Runs when it has room. A Run that has not started waits in the database, held by no
Coral Instance, and any Coral Instance hands it out. The Run Executor numbers its Program's requests and sends them on
one stream per Run; the Coral Instance that holds that stream commits them in chunks, each predicated on the Run's
head. There is no Run Owner: this ADR supersedes [ADR 0018](0018-run-owner-coordinates-each-run.md), whose Run Ledger,
head, chunk predicate, budgets, enforcement point and contract it keeps, and
[ADR 0025](0025-outside-run-requests-reach-the-run-owner-through-the-run-inbox.md), whose Run Inbox it replaces with a
wake row. [Map #2629](https://github.com/withcoral/coral/issues/2629) holds the reasoning and the options not chosen for
each part, and its tickets hold the limits and names. A proof of concept on a second process, measured by ADR 0018's
load harness, kept the Run Owner's burst performance before the owner was removed.

## Where a Run Executor runs

| Deployment | Run Executor | Memory | CPU |
| --- | --- | --- | --- |
| Desktop (macOS, Linux, Windows), one container | Child processes of `coral`, started on demand | V8 heap limits only | A lower OS priority than Coral |
| Docker Compose, Kubernetes | Its own container, dialing Coral at one address | The orchestrator's limit, sized above the heap limits it holds | A lower CPU weight than Coral, never a quota |
| A remote runtime | Behind a bridge: a Run Executor in its own container that drives the runtime | The lower of Coral's limit and the runtime's | The lower of the Run CPU Budget and the runtime's |

Every Run Executor dials Coral. A Coral Instance starts local children unless the operator turns them off, and admits
Run Executors it did not start on every deployment. A local child runs as Coral's OS user with an empty environment, so
it isolates resource use and failures, not access. A Run Executor in its own container also isolates access: it has no
ingress, no volume, no secret but its credential, and no egress except to Coral and DNS. A bridge also reaches its
runtime.

## The link

- **The Run Executor opens it.** One HTTP/2 connection to Coral's one address, whatever stands behind it: a load
  balancer, a Kubernetes Service, a Compose service name, or loopback for a local child. gRPC, plaintext inside a
  Compose network or a cluster and TLS-terminated in front of Coral where it leaves them. The Run Executor never learns how many Coral
  Instances there are, and Coral keeps no list of them. Every stream the Run Executor opens rides its current
  connection, and every call carries its credential as bearer metadata.
- **Coral listens for Run Executors on a listener of its own**, with the rules of its other listeners. It closes each
  connection after a maximum age: its HTTP/2 GOAWAY stops new streams on it, open Run streams finish where they are, and
  the next streams open on a new connection, which may land on another Coral Instance. Coral moves one stream by ending
  its side of it cleanly, with an OK status and no message; the Run Executor reopens that stream at once on its current
  connection. Any other end means a redial with back-off.
- **Keepalive on both sides.** A peer that stops answering its keepalive closes the connection.
- **The protocol level is checked on every stream open.** A side that sees a level more than one apart closes that
  stream and logs both levels; Coral keeps nothing per connection. A message that breaks the protocol ends the stream
  that carried it, not the link: on a Run stream the Run records Interrupted; on the pull stream the Runs go on.
- **Two streams.** The pull stream, one per Run Executor, is the first stream on every connection. A Run stream, one
  per Run, is the Run's handle. The Run Executor serves nothing.

## The pull stream

- **Hello and Welcome.** The Run Executor opens the pull stream once its compiler is warm, with its random id, its
  protocol level, its free room and its limits. Coral answers with its own level and the Coral Instance's id, and writes
  the Run Executor's lease row.
- **Room is a count the Run Executor reports.** It reports its free units again each time one frees. Coral subtracts
  each unit it sends, a Run or a compile, and stops at zero. A report and a take can cross; the Run Executor then holds
  one unit above its room for a moment and its next report corrects it. Room is the Run Executor's own count of what it
  is willing to hold, not a limit Coral enforces: nothing is refused or put back for room.
- **The lease follows the pull stream.** The Coral Instance that holds a Run Executor's pull stream renews its lease on
  a period shorter than the lease; nothing else renews it. When the stream closes, for any reason, the holder stops
  renewing; the lease is the Run Executor's grace to reopen the stream on any Coral Instance.
- **Retiring is a report of zero.** A Run Executor that must stop reports no room, honors what crossed the report,
  keeps the pull stream open while its Runs finish, closes it after its last Run ends, and exits. Coral retires a local
  child that has held no Run and no compile for an idle period by closing the child's stdin, the pipe its token came
  down, which stays open until then; the child treats a closed stdin at any time as that order.
- **Compiles ride it.** A compile is work: it goes only to a Run Executor with room, and holds one unit until it is
  answered.

## The queue and the take

- **Acceptance writes the Run, its package, its time limit and the `run.started` Event**, which records the acceptance,
  not the start; Reef already shows it as execution accepted. It writes no claim, no deadline, no start mark and no
  holder, and no Coral Instance holds a waiting Run in memory. A waiting Run reads `running`; Reef shows it waiting to
  start. One queue, oldest first, across Coral Instances and Workspaces.
- **The room report is the ask, and Coral holds it.** Each Coral Instance runs one take loop for all its pull streams.
  The change signal marks it dirty; it runs a take at most once per short period, and once a longer period while any
  pull stream has room. Each take picks the pull stream with the most room and, in one statement, claims the oldest
  waiting Runs up to that room and a cap, each with its own claim and its take time, skipping rows another take holds.
  Coral sends them as one message. The loop repeats while Runs wait and room remains, so a burst spreads across Run
  Executors and Coral Instances. A Run Executor never polls, and never reaches the database.
- **A taken Run that never starts goes back to waiting.** The sweep clears the claim of a Run with no start mark and a
  take older than a confirmation period, predicated on the claim and the missing start mark; the Run keeps its place. A
  late load from the first Run Executor fails the start's claim predicate and is answered that the Run is not held. No
  Program code has run. Coral logs each put-back with the Run Executor's id and tracks nothing else.
- **Coral never refuses a Run for capacity.** With no room anywhere and work waiting, a Coral Instance that runs local
  children starts another; an orchestrator scales Run Executors in their own container. A Coral Instance's readiness
  checks only its database.

## The Run stream

- **The same open for a first open and a reconnect.** The Run Executor opens the Run's stream on its current connection
  with the Run's id, its claim and its level. Coral reads the Run row and answers by it: a held Run with no start mark
  gets the package (the script, the source map, their hash, the value limits, the linked snapshot ids); a started Run
  gets the highest request number committed; a terminal Run, or one whose claim does not match, is told that it ended, or that it is not held.
- **The start.** After the package the Run Executor loads the script into a fresh isolate and says so. The Coral
  Instance that holds the stream rebuilds the Run's actor from the row, runs the authority recheck with it, records the
  start mark, the deadline, the Run Executor's id and itself as the stream's holder in one chunk, and sends the deadline
  and the Run CPU Budget. Only then does the Program run. A lost authority fails the Run with `run_authority_lost`, and
  no Program code runs. A Run **starts** when that chunk commits; its deadline runs from its start.
- **Requests are numbered by the Run Executor, from one per Run.** Each carries an Invoke, an Emit, a Log or the
  Finish. Coral answers an Invoke at Applied with the result, a rejection, an upstream error or a fatal. Nothing answers
  an Emit or a Log: the Run Executor checks the value against the package's value limits, counts the Run Output Budget
  and fails the Run when it is exceeded, and pauses the Program's next request while more than the Applied cap is
  unconfirmed; Coral confirms at commit. Every kind of Run Executor settles Emit and Log the same way.
- **The committed watermark.** After every chunk commit Coral sends the highest request number committed. The Run
  Executor drops what it covers and keeps the rest for a resend.
- **Finish, Terminate and the end.** Finish carries the Final Value, an absent value, a failure, host terminated, the
  memory limit, the CPU budget, the output budget, or the deadline reached, which the Run Executor sends when it ends a
  Program at the Run's deadline. A recorded termination reaches the Run Executor as Terminate with its reason; the Run
  Executor stops the Program uncatchably, discards the isolate and sends Finish. Coral's last message on every Run
  stream says how the Run ended, or that it is not held; a request for a Run that ended is not a violation.
- **A Run Executor keeps a disconnected Run until the Run's deadline.** It keeps redialing, and the Program keeps
  running up to the Applied cap. It ends a Program only at the Run's deadline, when Coral terminates it, or when a
  reconnect answers that the Run ended. It never reuses an isolate or resumes a Program.

## The Run Ledger and its writers

What stands from ADR 0018: the Run row's head is the Run Ledger's head, and every chunk that writes the ledger is
predicated on the Run's claim and the head it was built on. The Run Output Budget is exact. The Policy Enforcement Point
is one held transaction per batch of ready Invocations, merged into the next chunk, reading authority facts under shared
locks. The Fan-out Limit bounds one Run's dispatches in flight, and admission stays eager. A fenced writer acts on its
commit's evidence: a claim mismatch is definitive, a head mismatch under a matching claim means another writer landed
first, and no answer means reread. Readers never read a loop's memory. One pool, no reserved capacity.

What changes: a nonterminal Run has three writers, not one. The loop that holds the Run's stream commits the Program's
requests, its deadline and its parked waits as transient work of the open stream. The Coral Instance that receives an
outside request, a decision on one of the Run's Approval Requests or a cancellation, commits it directly. The sweeper
writes what no stream decided in time. Every one of them writes under the head, so the first commit wins at every step:
requests are keyed by their number, an Invocation's steps by its state, a decision by the thing it decides. A loop that
loses a head race rereads the row and the watermark and drops the requests at or below it before it retries, so a
request another Coral Instance was still committing cannot land twice. Every chunk commit stamps the Run row with its
time, so a reader can show how long a Run has been silent.

## Reconnect

A started Run survives the loss of its link and of its Coral Instance. The Program never stops. The Run Executor redials
Coral's one address and opens the Run's stream again on whichever Coral Instance it lands on; that Coral Instance reads
the row, answers the highest request number committed, or that the Run ended, and the Run Executor resends everything
above it, in order. A repeat carries the same number and lands once; an Emission or a Log now has an identity.

- **What a dead Coral Instance loses: the calls in flight on it.** The reconnecting Coral Instance settles each
  `dispatching` Invocation that has no result at once, with a compare-and-set on the Invocation's state; a result that
  lands later fails its predicate and is dropped. The Invocation ends `indeterminate`, and the Program gets a
  `ToolError` with code `upstream_incomplete`: the state every tool can already produce once its request was sent. The
  Run goes on. No list of live Coral Instances, no instance lease, no heartbeat.
- **The loop is rebuilt from the ledger.** A `pending` Invocation re-enters its wait or the enforcement queue from its
  rows; the Fan-out slots are the count of `dispatching` Invocations; the output total is the committed total with the
  resent requests applied on top. The reconnect's write records the new holder.
- **The Shutdown Drain finishes dispatches, not Runs.** At its start the Coral Instance reports not ready, answers no
  ask and accepts no new stream. It keeps each open Run stream until that Run has no dispatch in flight on it, then
  ends its side of the stream cleanly; the Run Executor reopens it at once and the Run goes on elsewhere. At the drain's
  length it closes the rest. Nothing in the drain records Interrupted, and a rolling deploy loses nothing.
- **The word is reconnect.** A Run Executor reconnects a Run, and the Run's stream moves. Coral never resumes a Run: no
  Program is recreated and no isolate is snapshotted.

## The clock, the leases and the sweeper

- **The clock decides only inside a fenced write, from two places.** The loop that holds a Run's stream keeps the Run
  deadline timer, each Approval Request's deadline and the periodic precheck of parked waits, as transient work rebuilt
  from the rows at a reconnect. One sweeper covers every Run with no stream and backs up a hung loop. Both compare a
  recorded deadline with their own clock and write under the Run's head, so a late request and a passed deadline keep
  the order the head gives them. The Run Executor's own end at the deadline reaches Coral as a Finish, recorded by the
  same rule. Readers show the recorded state beside the deadline, never a state derived from `now`.
- **A termination is written first.** Whoever decides it writes the terminal chunk in one transaction that ends the
  Run's open Invocations and releases the claim; the loop relays Terminate after. No dispatch executes after that write:
  the enforcement point's predicates need the Invocation `pending` and the Run `running`, a result that lands on an
  ended Invocation is dropped, and every later request is answered that the Run ended. A dispatch already sent is not
  aborted upstream. There is no grace period.
- **An approval counts if it was decided before its deadline.** The enforcement point compares two recorded times and
  reads no clock; the Run's own deadline bounds the dispatch. The loop's timer, or the sweeper, writes `expired` for an
  Approval Request whose deadline passed undecided. A connection request's expiry is unchanged: Account-owned, recorded
  by a holder when it reads it.
- **A lease per Run Executor.** A row per Run Executor records its id, its kind and its lease. The kind is derived from
  its credential, a stdin token making a local one. The Run row records which Run Executor holds the Run; liveness is
  the Run Executor's row, and there is no lease per Run.
- **The sweep interrupts a lapsed Run Executor's started Runs**, each under the Run's head and a predicate that the lease
  is still the value it read, so a Run Executor that reopened its pull stream in between fails every one of those
  writes. Its unstarted Runs go back to waiting. A Run Executor that reopens later hears on each Run stream that the Run
  ended, and terminates the Program. The sweeper deletes a lapsed row that holds no Run. The Coral Instance that started
  a local child and sees it exit runs the same write for that child's Runs at once.
- **One sweeper at a time, elected by a lease row.** Every Coral Instance tries the row on a period; the one whose
  update lands sweeps that pass and renews on each pass, and another takes over when it dies. The first pass runs at
  boot before the listeners bind. Each pass, bounded, oldest first: `running` Runs past their deadline, `pending`
  Approval Requests past theirs, taken Runs that never started, Run Executors past their lease. On SQLite the boot pass
  ends the Runs that local children held, since the one Coral Instance is their parent.

## Outside requests and the wake row

- **Whoever receives an outside request commits it.** A decision on an Approval Request or a cancellation is written
  by the Coral Instance that receives it, under the Run's head, and is Recorded at that commit. There is no Run Inbox.
- **The same transaction writes one wake row, keyed by the Run, with no payload.** Every commit on a `running` Run by a
  transaction that is not the Run's own loop raises it: a decision, a cancellation, the sweep's writes. The loop's own
  chunks raise none, and the put-back of an unstarted Run raises none. There is at most one per Run. It is a hint, never
  evidence.
- **One reader per Coral Instance.** It subscribes once to the content-free change signal and, on each ring, with a read
  gap and a backstop, reads the wake rows whose Run records this Coral Instance as the stream's holder and hands each to
  the loop registered for that Run. A row whose loop is not registered is left where it is. A writer on the same Coral
  Instance wakes the reader at once. The per-Run loops never subscribe to the signal.
- **What the loop does on a wake.** It reads the Run row. If the head is what it last wrote, nothing happened. If the row
  is terminal, it sends Terminate. Otherwise it rereads the Approval Requests it holds parked waits for, matched by
  their Invocation: an approved one moves its Invocation to the enforcement queue; a declined one has its Invocation
  already ended, and the waiter reports it to the Program. The loop deletes the wake row in its next chunk commit, which
  asserts the head, so a write that lands in between fails it and no raise is lost. The reconnect's write deletes the
  Run's wake row too.
- **The other waiters keep the content-free signal**: the take loop, the compile relay and a parked connection request,
  one subscription each per Coral Instance. The content-free signal stays the only signal in `coral-db`: no table name,
  row identity or payload crosses it.

## Compiling

Each Run Executor keeps one warm Program Compiler, warmed before it says hello, with the compiler's existing limits. A
compile travels down the pull stream and its answer comes back on it; it goes to the pull stream with the most room on
the Coral Instance that needs it. A Coral Instance that holds no pull stream, or none with room, writes the call to a
relay row that any Coral Instance with room, itself included, picks up, serves and answers through the database. Coral
Instances keep talking only through the database. A compile that no Run Executor can serve yet waits for one: no Run
Executor linked is a state every rollout passes through. The compile's own time limit runs once a Run Executor serves
it; a compile that waits past a bound fails as a missing compiler fails today, and Runs already accepted wait. With
local children on, a compile with no child running starts one and waits for it. A compile carries no authority, and a Run Executor cannot call Coral back
during one. The compiler has its Run Executor's release, so a global takes three release steps.

## Resources

The V8 heap limit per Run stays the graceful end: only that Run fails, with the memory-limit failure. An OS memory limit
ends the whole Run Executor; the sweep interrupts its started Runs once its lease lapses and puts its unstarted Runs
back. The **Run CPU Budget** counts CPU time on the Run's own thread. It defaults to the Run's time limit, from its start
to its deadline, so it ends a Run only when an operator sets a lower one. A Run over it fails with its own CPU-limit
failure, and the Run Executor's other Runs go on. The budget needs no `unsafe` code, which the workspace forbids.
Windows has no safe call for a thread's CPU clock, so Coral refuses a lower budget there. A compile runs outside any
budget, with its own time limit.

## Authority

Coral admits a Run Executor by its credential, never by the network it dials from. A link that crosses a network is
private only as far as that network is. A Run Executor has the authority of the Runs it holds, and nothing more.

- One operator-held token, named in config by the environment variable that holds it, two during a rotation; Coral
  checks a verifier in a set. A local child presents a token Coral wrote to its stdin at start, valid while the child
  runs. Every Run Executor is the same to Coral; its id is self-declared and for operators only.
- Coral checks each request as it does for an in-process Program: the Principal's authority and policy on each
  Invocation, then approvals, the Policy Enforcement Point, the Run Output Budget and the Log limits. It takes no
  identity from a Run Executor, and no message carries a Workspace, Principal, Task, account or credential. A compiled
  package gains no authority.
- A credential Coral stops accepting refuses new dials; open connections last to their age, open Run streams finish,
  and the Run Executor retires.
- What a Run Executor reports about itself only steers what it is given. Source locations are for display only.
- Coral refuses to boot when the Run Executor listener is bound off loopback with no credential configured, or when
  local children are off with none, since nothing could be admitted. A Run Executor opens a health-only listener on
  request, and no port otherwise.
- The link is operator infrastructure, like the database URL, so it does not go through `coral-outbound`.

## Versions

One protocol level, an integer in the `coral` source, covers the messages and the Program Runtime Surface. A release
that changes either raises it by exactly one. Each side accepts a peer whose level equals its own or differs by one,
checked on every stream open. No code asks for the peer's level, so any Run can go to any Run Executor, and Run
Executors roll apart from Coral.

- **Two steps for a message or field**: one release teaches both sides to read it, and the next starts to send it. A
  removal goes in reverse.
- **Three steps for a global**, because the compiler has its Run Executor's release: the compiler accepts Programs that
  use a new global two releases after the Run Executor gains it, and stops two releases before the Run Executor drops
  it. Only a removed global matters, and the Run then fails as a Program that uses a missing global.
- The rule applies from the release in which Coral accepts dial-ins from Run Executors it did not start. Before it,
  every Run Executor is a child of the same binary.

## The seams

- **The Run stream is Coral's seam.** The loop that serves a Run stream has two adapters: the gRPC stream from a Run
  Executor, and an in-process stream that the test engine drives. On the Run Executor's side, Coral's existing Program
  engine and host interfaces stay: the Run Executor runs V8 behind them, and its host adapter sends the requests.
- **The compiler client** has one interface and two adapters, in process for tests and over the pull stream in
  production. Program Admission and Saved Function save share it.
- **The local child supervisor** starts children when work waits and no pull stream has room, and retires idle ones. It
  is the only code that knows a Run Executor's process.

## A remote runtime

Proven on paper for Cloudflare's Worker Loader, and not built. A runtime that cannot accept a gRPC stream sits behind a
bridge: a Run Executor in its own container that dials Coral, compiles, maps source locations, keeps its room as a
count, and runs each Run's isolate on the runtime. Its messages to the runtime are private to it, and its release
carries both sides. [Prove a remote Run Executor on paper](https://github.com/withcoral/coral/issues/2640) lists the
checks to run before a bridge is built.

- A bridge settles Emit and Log as every Run Executor does, within the Applied cap.
- For each Run, the lower of Coral's limit and the runtime's wins, except that a runtime that cannot apply a lower limit
  keeps its own. CPU and memory fail as on any Run Executor, and a Run cut at the runtime's maximum Run duration records
  Interrupted. Coral logs the limits a bridge reports, with a warning when one is below its own.
- A bridge says it loaded a Run after the runtime has loaded the package. A deploy or update of the runtime ends its
  Runs in flight as Interrupted.
- An allow-list test names every global of the Program Runtime Surface, and runs against each kind of Run Executor. On
  Workers, `Date.now()` does not advance while code runs, and no harness changes that.

## Telemetry

A Run Executor's telemetry reaches an exporter only through Coral.
[Trace a Run Executor's work and export its telemetry](https://github.com/withcoral/coral/issues/2705) decides what
Coral exports and amends this ADR.

## The contract every slice keeps

1. Events are prefix-complete per Run: once a sequence is visible, every lower one is committed. Gapless numbering is
   not required.
2. The Run Output Budget is exact. A Run never exceeds it by a byte.
3. Dispatch starts only after one authoritative enforcement check against current authority, policy, account and
   approval facts. An approval counts if it was decided before its deadline. A revocation that commits first always
   wins; a change committed after the recorded dispatch start does not stop that dispatch. A cached decision never
   authorizes.
4. No Event after `run.ended`. While the Run is nonterminal, its Run-scoped rows are written only by the loop that
   holds its stream, the Coral Instance that receives an outside request for it, and the sweeper, each under the head.
5. Every commit that writes the Run Ledger is predicated on the claim and the head. Never the clock.
6. An Approval binds one exact Invocation; approvals stay independent decisions. The first durable decision wins.
7. The code is identical on SQLite and Postgres; backend-specific idioms live in `coral-db`.
8. Nothing here assumes one process: the fence is in the write predicate, and a Run is found by its row.

## Consequences

- **A Run starts a little later.** The take, the package and two round trips sit between acceptance and admission. The
  proof measured the gap at a fifth of the Run's time under the heaviest concurrent cell, and parity everywhere else.
- **A Run can wait for room without end**, and its deadline does not run while it waits. An MCP client's Run timeout
  counts from the Run's start.
- **When no Run Executor is linked, a new Program waits for one**, and its caller's `start` waits with it, up to a
  bound. Reef, sign-in and every other MCP tool keep working, and Runs already accepted wait.
- **A dead Coral Instance loses only the calls in flight on it.** The Program sees `upstream_incomplete` for each; the
  Applied cap bounds what the Run Executor holds for a resend.
- **A lost Run Executor ends every Run it started**, within a lease and a sweep period, and its unstarted Runs wait for
  another. Its room bounds how many.
- **An outside request is Recorded at the receiver's commit.** No requester hears "not yet recorded".
- **Freshness is not bounded.** A reader of Recorded state can trail a flooding Program by what the database can
  write, in order and never wrong.
- **Request work shares the pool with Run Ledger work.** Reconsider reserved permits if the sign-in or readiness probes
  exceed their budgets while every Run transaction is short.
- **Migrations are expand-only across one release**, because a new process migrates at boot while the old one still
  serves.
- **A local child shares Coral's OS user and, in one container, its memory limit.** The guides run Run Executors in
  their own container on Docker Compose and Kubernetes, with a stop timeout as long as the longest Run.
- **The same Program can pass on one Run Executor and fail on another** whose limits are lower.
- **On Windows, an operator cannot lower the Run CPU Budget.**

## Rejected

- **A Run Owner built for pull.** It would pay the take, the package and the start mark too, and keep one Coral
  Instance as the only writer of a Run whose Program lives elsewhere. ADR 0018 rejected optimistic appends because they
  "degrade to one winner per round trip under the burst"; the proof raced a per-Run loop and an independent committer
  on every decision and stayed within the parity rule on every cell.
- **Coral opening the links**, to a configured address or a local child's port. A Run Executor behind NAT, a customer's
  machine and a hosted fleet are all reachable only if the Run Executor dials.
- **Polling, or a held request kept only so work can be pushed.** The room report is the ask; a Run Executor with no
  room sends nothing, and the take loop reads Coral's own database on a period.
- **Room as a grant Coral spends and the Run Executor refills.** A count can drift by one unit for a moment and needs
  no arithmetic on both sides.
- **A Retiring message.** Coral cannot tell a full Run Executor from a retiring one, and does not need to.
- **The Run Inbox, forwarding to the owner's instance, or a message broker.** With no owner, whoever receives an outside
  request commits it; a wake row is enough.
- **Every loop rereading its Run row on the content-free signal**, and **a signal that names the Run**: one costs a read
  per held Run per gap, the other carries a row identity across the boundary the content-free signal keeps.
- **A Postgres advisory lock to elect the sweeper.** A holder whose node vanishes keeps the lock until Postgres notices
  the dead peer; the lease row's bound is Coral's own.
- **A reconnect window of the Run Executor's own**, or ending a Program because a lease lapsed. The Run's deadline is
  the one bound.
- **A new claim at reconnect as a fence.** It would drop every result the old Coral Instance was about to commit.
- **A code of its own for the lost answer.** `upstream_incomplete` is the state every tool already has.
- **Mutual TLS or a credential per Run Executor now.** One operator-held token admits the first epic's Run Executors; a
  credential per Run Executor waits for a hosted fleet.
- **The same build on both sides of a link.** A rolling update would split capacity by build.
- **Measuring memory to place Runs.** A count is simpler, and the guides size memory for it.
- **Not ready without a Run Executor.** An outage would also take down Reef, sign-in and MCP.
- **The compiler in the Coral process, or in its own container beside each Coral Instance.** The Coral process starts
  no V8 isolate, and a compiler container would add a service to every guide.
- **Snapshotting a parked isolate to resume it elsewhere.** Coral never resumes a Run.

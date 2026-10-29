---
status: accepted
---

# Run Executors compile and execute Coral Programs outside the Coral process

Every V8 isolate that compiles or runs Principal-authored source now runs in a **Run Executor**, a process apart from
Coral's control plane. The Coral process starts no V8 isolate. A Run Executor's resource use and failures stay with the
Runs it holds, and Runs can execute on another runtime for a hosted offering. A Run Executor is the same `coral` binary
as Coral. The in-process V8 engine stays only as a test engine.

The Run Owner of [ADR 0018](0018-run-owner-coordinates-each-run.md) stays in the Coral Instance that holds the Run's
claim. [Map #2629](https://github.com/withcoral/coral/issues/2629) holds the reasoning and the options not chosen for
each part, and its tickets hold the limits and names.

## Where a Run Executor runs

| Deployment | Run Executor | Memory | CPU |
| --- | --- | --- | --- |
| Desktop (macOS, Linux, Windows), one container | Child processes of `coral`, started on demand | V8 heap limits only | A lower OS priority than Coral |
| Docker Compose, Kubernetes | Its own container, shared by every Coral Instance, found through one DNS name | The orchestrator's limit, sized above the heap limits it holds | A lower CPU weight than Coral, never a quota |
| A remote runtime | Behind a bridge: a Run Executor in its own container that drives the runtime | The lower of Coral's limit and the runtime's | The lower of the Run CPU Budget and the runtime's |

`coral` runs local children only when no Run Executor address is configured. The two never mix. Coral resolves every
address behind that name, opens one link to each, and resolves the name again periodically. A local child runs as
Coral's OS user with an empty environment, so it isolates resource use and failures, not access. A Run Executor in its
own container also isolates access: it has no egress, no volume and no secret. A bridge reaches only its runtime.

## The shape

- **The link.** gRPC, and Coral is always the client. A link is one HTTP/2 connection, and a peer that stops answering
  its keepalive closes it.
  - The Run Executor first reports its protocol level, its random id, its room, whether its Emits and Logs wait for an
    answer, and any limits of its own. It can later report that it is retiring.
  - Each Run gets one bidirectional Run stream, which is its handle. Coral offers the Run on it with the package and
    the Run's limits, and the Run Executor takes it or refuses it (busy, retiring). The Program's requests then flow in
    order, and Coral answers them in that order and can terminate the Run.
  - A local or own-container Run Executor waits for the answer to each Emit and Log, as an in-process Program does.
  - Compile calls use the same service and link.
  - A local child listens on loopback and proves its link with a one-time token that Coral gives it at start. It takes
    only the first connection, and Coral never reconnects to it.
  - A message that breaks the protocol closes the link, and each of its Runs records Interrupted.
- **Fate.** The Run Owner decides every outcome and keeps every timer: the Run deadline, the grace period, and the
  approval and connection deadlines. A Run Executor times only the Run CPU Budget and a backstop at the deadline plus
  the grace period. It reports how a Program ended and never names an outcome.
  - A Program ends when its Run Owner ends, so it continues on an Applied answer. Each Run stream is bound in Coral to
    its owner's claim. A recovered owner mints a new claim, and the old Program's requests are answered that the Run
    ended.
  - A Run **starts** when a Run Executor takes it, and stays bound to that Run Executor. Before its start, a failed
    offer tries another Run Executor. After it, a lost link or a Run stream that ends before the Program does records
    Interrupted. A recorded cancellation stands, and a Run past its deadline records TimedOut.
  - Every stop terminates the Program and arms the grace period, in every phase. The Run Executor ends the Program
    uncatchably and discards the isolate when its link closes, when Coral terminates the Program, or at the backstop.
    It never reuses an isolate or resumes a Program.
- **Placement.** A Run Executor decides its own room: a count of Runs held, executing or parked. It takes each Run it
  has room for and refuses the rest as busy. Coral offers a Run first to the Run Executors on which it placed the
  fewest Runs.
  - Coral never refuses a Run for capacity. A Run that has not started waits in one queue per Coral Instance, oldest
    first, and only the head is offered. Authority is checked again before each offer.
  - A Run's deadline runs from its start. Coral records the start, the deadline and the Run Executor's id together. The
    id is for operators only.
  - Coral starts a local child for a compile when none runs, and another when every child refuses a Run. It closes the
    link of a child that has held no Run and no compile for an idle period. An orchestrator scales Run Executors in
    their own container.
  - A Coral Instance's readiness checks only its database.
- **Shutdown.** A Coral Instance keeps its Shutdown Drain. At its start, it releases the leases of its Runs that have
  not started, and another Coral Instance's sweep queues them. After the drain it stops its live Runs, closes its links
  last, and does not wait for its children. A Run Executor never ends its own Runs: when it is told to stop, or after
  it misses a Run's grace period, it retires, takes no new Run, and exits when its last Run ends. A local child exits
  as soon as its link closes, so Coral ends a child by closing its link.
- **Compiling.** Each Run Executor keeps one warm Program Compiler, warmed before it reports in, with the compiler's
  existing queue and limits. Coral sends each compile call to a random linked Run Executor that is not retiring. A busy
  one moves the call on, and when every one is busy the call fails as a full compiler queue fails. A lost Run Executor
  fails the call it holds.
  - With no local child running, a compile starts one and waits for it within the caller's compile time.
  - A compile fails at once, as no Program Compiler is available, only while Coral backs off from starting local
    children, or when a configured address has no link. Runs that Coral has already accepted wait in their queue.
- **Resources.** The V8 heap limit per Run stays the graceful end: only that Run fails, with the memory-limit failure.
  An OS memory limit ends the whole Run Executor, and its Runs record Interrupted. The **Run CPU Budget** counts CPU
  time on the Run's own thread. It defaults to the Run's time limit, from its start to its deadline, so it ends a Run
  only when an operator sets a lower one. A Run over it fails with its own CPU-limit failure, and the Run Executor's
  other Runs go on. The budget needs no `unsafe` code, which the workspace forbids. Windows has no safe call for a
  thread's CPU clock, so Coral refuses a lower budget there. A compile runs outside any budget, with its own time
  limit.

## Authority

A Run Executor has the authority of the Runs it holds, and nothing more.

- Coral checks each request as it does for an in-process Program: the Principal's authority and policy on each
  Invocation, then approvals, the Dispatch Fence, the Run Output Budget and the Log limits. It takes no identity from a
  Run Executor.
- An offered Run and a compile call carry no Workspace, Principal, Task, account or credential. A compiled package
  gains no authority, and a Run Executor cannot call Coral back during a compile.
- What a Run Executor reports about itself only steers placement. Source locations are for display only.
- Every link that crosses a network assumes a trusted network. TLS and authentication on the link can come later, with
  no change to the messages.
- A Run Executor address is operator infrastructure, like the database URL, so the link does not go through
  `coral-outbound`.

## Versions

One protocol level, an integer in the `coral` source, covers the messages and the Program Runtime Surface. A release
that changes either raises it by exactly one. Each side of a link accepts a peer whose level equals its own or differs
by one. No code asks for the peer's level, so any Run can go to any linked Run Executor, and Run Executors roll apart
from Coral.

- **Two steps for a message or field**: one release teaches both sides to read it, and the next starts to send it. A
  removal goes in reverse.
- **Three steps for a global**, because the compiler has its Run Executor's release: the compiler accepts Programs that
  use a new global two releases after the Run Executor gains it, and stops two releases before the Run Executor drops
  it. A Run that waits through a Coral rolling update can still meet a Run Executor one level further from its
  compiler. Only a removed global matters, and the Run then fails as a Program that uses a missing global.
- The rule applies from the release that lets an operator configure a Run Executor address. Before it, every Run
  Executor is a child of the same binary.

## The seams

- **Execution and compiling.** Coral's existing Program engine and compiler interfaces are the seam. Their adapters
  cross the link, and the Run Executor serves each Program request through a host adapter that crosses it back.
- **Link sources.** Placement gets links from one interface with two adapters: the configured address and local
  children. Placement never knows which one it uses.
- **Partitioning by Workspace** is not built. When it comes, placement narrows its candidates, for Runs and for
  compiles, by the Run's Workspace.

## A remote runtime

Proven on paper for Cloudflare's Worker Loader, and not built. A runtime that cannot accept a gRPC stream sits behind a
bridge: a Run Executor in its own container that compiles, maps source locations, keeps its room as a count, runs the
backstop, and runs each Run's isolate on the runtime. Its messages to the runtime are private to it, and its release
carries both sides. [Prove a remote Run Executor on paper](https://github.com/withcoral/coral/issues/2640) lists the
checks to run before a bridge is built.

- A bridge reports that its Emits and Logs do not wait. A Program then sees an Emit settle later, a Log that Coral
  rejects after the call has returned is dropped, and a Program can run ahead of Coral. Coral keeps the Applied cap by
  not reading that Run's stream, and HTTP/2 flow control holds the bridge back.
- For each Run, the lower of Coral's limit and the runtime's wins, except that a runtime that cannot apply a lower limit
  keeps its own. CPU and memory fail as on any Run Executor, and a Run cut at the runtime's maximum Run duration records
  Interrupted. Coral logs the limits a bridge reports, with a warning when one is below its own.
- A bridge takes a Run after the runtime has loaded the package, and refuses it as busy when the load fails. A deploy
  or update of the runtime ends its Runs in flight as Interrupted.
- An allow-list test names every global of the Program Runtime Surface, and runs against each kind of Run Executor. On
  Workers, `Date.now()` does not advance while code runs, and no harness changes that.

## Telemetry

A Run Executor's telemetry reaches an exporter only through Coral.
[Trace a Run Executor's work and export its telemetry](https://github.com/withcoral/coral/issues/2705) decides
what Coral exports and amends this ADR.

## Consequences

- **A Run starts a little later.** The link adds 0.03–0.08 ms to a start, and 7–27 µs to each local Emit or Log. The
  first need after a local child's idle exit waits 0.1–0.3 s for a new child and its compiler.
- **A Run can wait for room without end**, and its deadline does not run while it waits. An MCP client's Run timeout
  counts from the Run's start.
- **When no Run Executor can be reached, new Programs fail admission.** Reef, sign-in and every other MCP tool keep
  working.
- **A lost Run Executor ends every Run it holds.** Its room bounds how many.
- **A local child shares Coral's OS user and, in one container, its memory limit.** The guides run Run Executors in
  their own container on Docker Compose and Kubernetes. The guides give a Run Executor a stop timeout as long as its
  longest Run plus the grace period, so `docker compose down` or a node drain can wait that long.
- **The same Program can pass on one Run Executor and fail on another** whose limits are lower.
- **On Windows, an operator cannot lower the Run CPU Budget.**

## Rejected

- **The Run Owner's mailbox as the seam.** Its messages carry database work and in-process handles.
- **Our own framing, or WebSocket on every link.** HTTP/2 gives each Run its own flow control, and gRPC already ships.
- **The same build on both sides of a link.** A rolling update would split capacity by build.
- **A heartbeat or lease on the link.** A closed link is the liveness signal.
- **Measuring memory to place Runs, or a placement window that fails the Run.** A count is simpler, and the guides
  size memory for it.
- **Not ready without a Run Executor.** An outage would also take down Reef, sign-in and MCP.
- **A socket pair or a named pipe for a local child.** It needs fd passing and Windows-only code on both sides.
- **The compiler in the Coral process, or in its own container beside each Coral Instance.** The Coral process starts
  no V8 isolate, and a compiler container would add a service and a second kind of link to every guide.
- **Snapshotting a parked isolate to resume it elsewhere.** Coral never resumes a Run.

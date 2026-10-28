---
status: accepted
---

# Run Executors compile and execute Coral Programs outside the Coral process

Every V8 isolate that compiles or runs Principal-authored source now runs in a **Run Executor**, a process apart from
Coral's control plane. The Coral process starts no V8 isolate. A Run Executor's resource use and failures stay with the
Runs it holds, and Runs can execute on another runtime for a hosted offering. A Run Executor is the same `coral` binary,
started as `coral run-executor`. Today's in-process V8 engine stays only as a test engine behind `ProgramEngine`.

The Run Owner stays in the Coral Instance that holds the Run's claim, and ADR 0018 stands, amended in three sentences
(below). [Map #2629](https://github.com/withcoral/coral/issues/2629) holds the reasoning and the options not chosen for
each part.

## Where a Run Executor runs

| Deployment | Run Executor | Memory | CPU |
| --- | --- | --- | --- |
| Desktop (macOS, Linux, Windows), one container | Child processes of `coral`, started on demand | V8 heap limits only | `nice +10`, or `BELOW_NORMAL` on Windows |
| Docker Compose, Kubernetes | Its own container, shared by every Coral Instance, found through one DNS name | The orchestrator's limit, sized above the heap limits it holds | A lower CPU weight than Coral, never a quota |
| A remote runtime | Behind a bridge: a Run Executor in its own container that drives the runtime | The lower of Coral's limit and the runtime's | The lower of the Run CPU Budget and the runtime's |

`coral` runs local children only when no `run_executor_address` is configured. The two never mix. A local child runs as
Coral's OS user with an empty environment, so it isolates resource use and failures, not access. A Run Executor in its
own container also isolates access: it has no egress, no volume and no secret.

## The shape

- **The link.** gRPC, and Coral is always the client. A link is one HTTP/2 connection, with a PING every 10 s and a
  close after 20 s without an answer.
  - A `Link` stream carries Hello (the protocol level, the Run Executor's random id, its room, its Emit and Log mode,
    its maximum Run duration and any fixed limits) and later Retiring.
  - Each Run gets one bidirectional Run stream, which is its handle. Coral sends Start: the package inline with its
    SHA-256, the Run's limits, the Run id for operator logs, and an optional W3C trace context. The Run Executor answers
    Accepted or Refused (busy, retiring). Then it sends Invoke, Emit, Log and Finish, numbered from 1, and Coral sends
    Answer and Terminate. Coral answers by number, in number order.
  - Compile calls use the same service and link.
  - A local child binds a loopback port and proves its link with a one-time token that Coral writes to its stdin. It
    closes its listener after the first connection, and Coral never reconnects to it.
  - A message that breaks the protocol closes the link, and each of its Runs records Interrupted.
- **Fate.** The Run Owner decides every outcome and keeps every timer: the Run deadline, Grace, and the approval and
  connection deadlines. A Run Executor times only the Run CPU Budget and a backstop at the deadline plus Grace from
  acceptance. Finish reports how the Program ended, with today's `ExecutionCompletion` or the CPU-budget end, and never
  names an outcome.
  - A Program ends when its Run Owner ends, so it continues on an Applied answer. Each Run stream is bound in Coral to
    its owner's claim. A recovered owner mints a new claim, and the old Program's requests get "Run ended".
  - A Run binds to its Run Executor when it is Accepted. Before that, a failed start tries another Run Executor. After
    it, a lost link or a Run stream that ends without Finish records Interrupted. A recorded cancellation stands, and a
    Run past its deadline records TimedOut.
  - Every stop sends Terminate and arms Grace, in every phase. The Run Executor ends the Program uncatchably and
    discards the isolate when its link closes, on Terminate, or at the backstop. It never reuses an isolate or resumes a
    Program.
- **Placement.** A Run Executor decides its own room: a count of Runs held, executing or parked, 128 by default. It
  answers Start with Accepted or Refused (busy). Coral tries the Run Executors on which it placed the fewest Runs.
  - Coral never refuses a Run for capacity. A Run waits in one queue per Coral Instance, oldest first, and only the head
    tries to start. Authority is checked again before each Start.
  - The Run deadline starts when a Run Executor accepts the Run. Coral writes the start mark, the deadline and the Run
    Executor's id together. The id is for operators only.
  - Coral starts more local children when every child refuses, and closes the link of a child that held no Run and no
    compile for 5 minutes. An orchestrator scales Run Executors in their own container.
  - A Coral Instance's readiness checks only its database.
- **Shutdown.** A Coral Instance keeps its Shutdown Drain. At its start, it releases the leases of its waiting Runs,
  and another Coral Instance's sweep queues them. After the drain it stops its live Runs, closes its links last, and
  does not wait for its children. A Run Executor never ends its own Runs: on SIGTERM, or after it misses a Run's Grace,
  it retires, takes no new Run, and exits when its last Run ends. A local child exits as soon as its link closes, so
  Coral ends a child by closing its link.
- **Compiling.** Each Run Executor that runs locally or in its own container keeps one warm Program Compiler, warmed
  before Hello, with today's queue and limits. Coral sends each compile call to a random linked Run Executor that is not
  retiring. Busy moves the call on, and a lost Run Executor fails it. With no linked Run Executor, `exec` and
  `save_function` fail at once with today's codes and "No program compiler is available. Try again later." Runs already
  accepted wait.
- **Resources.** The V8 heap limit per Run stays the graceful end: only that Run fails, with `memory_limit_exceeded`. An
  OS memory limit ends the whole Run Executor, and its Runs record Interrupted. The **Run CPU Budget** counts CPU time
  on the Run's own thread. It defaults to the Run deadline, so it ends a Run only when an operator lowers it with
  `execution_cpu_seconds`. A Run over it fails with `cpu_limit_exceeded`, and the Run Executor's other Runs go on. The
  workspace forbids `unsafe` code, so a V8 interrupt every 100 ms reads the Run thread's own CPU clock. Windows has no
  safe call for that clock, so Coral refuses `execution_cpu_seconds` there. A compile runs outside any budget, with its
  own 2 s limit.

## Authority

A Run Executor has the authority of the Runs it holds, and nothing more.

- Coral checks each request as it does today: the Principal's authority and policy on each Invoke, then approvals, the
  Dispatch Fence, the Run Output Budget and the Log limits. It takes no identity from a Run Executor.
- Start and compile calls carry no Workspace, Principal, Task, account or credential. A compiled package gains no
  authority, and a Run Executor cannot call Coral back during a compile.
- Hello only steers placement. Source locations are for display only.
- Every link that crosses a network assumes a trusted network. TLS and authentication on the link can come later, with
  no change to the messages.
- A Run Executor address is operator infrastructure, like the database URL, so the link does not go through
  `coral-outbound`.

## Versions

One protocol level, an integer in the `coral` source, covers the messages and the Program Runtime Surface. A release
that changes either raises it by exactly one, and a CI check compares it with the last release. Each side of a link
accepts a peer whose level equals its own or differs by one. No code asks for the peer's level, so any Run can go to any
linked Run Executor, and Run Executors roll apart from Coral.

- **Two steps for a message or field**: one release teaches both sides to read it, and the next starts to send it. A
  removal goes in reverse.
- **Three steps for a global**, because the compiler has its Run Executor's release: the compiler accepts Programs that
  use a new global two releases after the Run Executor gains it, and stops two releases before the Run Executor drops
  it. A Run that waits through a Coral rolling update can still meet a Run Executor one level further from its
  compiler. Only a removed global matters, and the Run then fails as a Program that uses a missing global.
- The rule applies from the release that adds `run_executor_address`. Before it, every Run Executor is a child of the
  same binary.

## The seams

- **Execution.** Coral's `ProgramEngine` gains an adapter that runs each Program over a Run stream and turns each
  incoming request into a call on today's `RunProgramHost`. The Run Executor runs today's executor with a `ProgramHost`
  adapter that sends each call over the stream.
- **Compiling.** `ProgramEngine` and `SavedFunctionCompiler` keep their traits. Their adapters call one compiler client,
  with one call for each compiler request that exists today and the time left in the caller's 2 s. Errors cross as a
  typed list (busy, timed out, diagnostics, memory limit, unavailable) and map to today's codes.
- **Link sources.** Placement gets links from one interface with two adapters: the configured address and local
  children. Placement never knows which one it uses.
- **Where the code lives.** The protocol, the Run Executor and Coral's side of the link are one crate,
  `coral-run-executor`. `coral-run` and `coral-functions` depend on it, and `coral-app` wires it.
- **Partitioning by Workspace** is not built. When it comes, placement narrows its candidates, for Runs and for
  compiles, by the Run's Workspace.

## A remote runtime

Proven on paper for Cloudflare's Worker Loader, and not built. A runtime that cannot accept a gRPC stream sits behind a
**bridge**: a Run Executor in its own container that compiles, maps source locations, keeps its room as a count, runs
the backstop, and runs each Run's isolate on the runtime. Its messages to the runtime are private to it, and its release
carries both sides.

- Its Hello says that Emit and Log do not wait. A Program then sees `coral.emit` settle later, a Log that Coral rejects
  after the call has returned is dropped, and a Program can run ahead of Coral. Coral keeps the Applied cap by not
  reading that Run's stream, and HTTP/2 flow control holds the bridge back.
- For each Run, the lower of Coral's limit and the runtime's wins: CPU fails with `cpu_limit_exceeded`, memory with
  `memory_limit_exceeded`, and a Run cut at the runtime's maximum Run duration records Interrupted. Coral logs Hello's
  limits, with a warning when one is below its own.
- The bridge answers Accepted after the runtime has loaded the package, and Refused (busy) when the load fails. A deploy
  or update of the runtime ends its Runs in flight as Interrupted.
- An allow-list test names every global of the Program Runtime Surface, and runs against each kind of Run Executor. On
  Workers, `Date.now()` does not advance while code runs, and no harness changes that.
- Before a bridge is built, check: a 905 s hold from a Durable Object into its Dynamic Worker; whether host binding
  calls count as subrequests; the error past 128 MB; whether aborting a Durable Object ends its Dynamic Worker at once;
  whether `globalOutbound: null` blocks an outbound `WebSocket`; whether code generation from strings stays blocked
  during a request.

## Telemetry

A Run Executor has no egress, so its telemetry reaches an exporter only through Coral. Coral is to export four metrics,
Runs waiting, oldest wait, Runs held and capacity, and a span for each compile.
[Trace a Run Executor's work and export its telemetry](https://github.com/withcoral/coral/issues/2705) builds them and
amends this ADR.

## Changes to ADR 0018

- **Run Request**: "The Program shares the owner's fate" becomes "The Program ends when its owner ends".
- **Recovery**: a recovered owner queues a Run that no Run Executor has accepted, instead of interrupting it. No Program
  code has run, so nothing is resumed.
- **Rollout**: at the start of the Shutdown Drain, a Coral Instance releases the leases of its waiting Runs.

## Consequences

- **A Run starts a little later.** The link adds 0.03–0.08 ms to a start, and 7–27 µs to each local Emit or Log. The
  first need after a local child's idle exit waits 0.1–0.3 s for a new child and its compiler.
- **A Run can wait for room without end**, and its deadline does not run while it waits. MCP's `run_timeout_ms` counts
  from acceptance by a Run Executor.
- **With no Run Executor, new Programs fail admission.** Reef, sign-in and every other MCP tool keep working.
- **A lost Run Executor ends every Run it holds.** Its room bounds how many.
- **A local child shares Coral's OS user and, in one container, its memory limit.** The guides run Run Executors in
  their own container on Docker Compose and Kubernetes. `docker compose down` and a node drain can wait up to 915 s for
  a busy Run Executor.
- **The same Program can pass on one Run Executor and fail on another** whose limits are lower.
- **On Windows, an operator cannot lower the Run CPU Budget.** Windows runs only Desktop, where the default applies.
- **macOS has no Rust job in CI.** Its process-group and priority code shares the POSIX path that the Linux tests
  cover.

## Rejected

- **The Run Owner's mailbox as the seam.** Its messages carry database work and in-process handles.
- **Our own framing, or WebSocket on every link.** HTTP/2 gives each Run its own flow control, and gRPC already ships.
- **The same build on both sides of a link.** A rolling update would split capacity by build.
- **A heartbeat or lease on the link.** A closed link is the liveness signal.
- **Measuring memory to place Runs, or a placement window that fails the Run.** A count is simpler, and the guides
  size memory for it.
- **Not ready without a Run Executor.** An outage would also take down Reef, sign-in and MCP.
- **A socket pair or a named pipe for a local child.** It needs fd passing and Windows-only code on both sides.
- **The compiler in the Coral process, or in its own container beside each Coral Instance.**
- **Snapshotting a parked isolate to resume it elsewhere.** Coral never resumes a Run.

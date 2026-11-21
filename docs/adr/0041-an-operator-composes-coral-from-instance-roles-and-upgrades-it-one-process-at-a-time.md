---
status: accepted
---

# An operator composes Coral from Instance Roles and upgrades it one process at a time

On a self-hosted Postgres deployment, an operator chooses which **Instance Roles** each `coral` process runs: the core
role, the MCP role, the auth role and the Run Executor role. The operator runs several processes of any role at once,
and replaces them one at a time for an upgrade while the deployment keeps serving. So each role is sized for its own
load, and a crash or a restart of one role does not stop the others. The public and the private listeners can then sit
in different networks, which is a side effect and not a goal.

Before this, one `coral` process opened every listener and nothing turned one off. The self-host guides described one
Coral Instance, stopped for each upgrade.

The design targets the model of [ADR 0027](0027-run-executors-compile-and-execute-programs.md), in which a Run is found
by its row and no Coral Instance is the only one that can serve it. It holds for the Community Edition: no part of it is
an Enterprise Feature ([ADR 0033](0033-enterprise-code-lives-in-ee-and-runs-only-in-the-enterprise-edition.md)). SQLite
and Desktop keep one process. [Map #3138](https://github.com/withcoral/coral/issues/3138) holds the reasoning and the
options not chosen for each part, and its tickets hold the names, the limits and the error texts.

## The four roles

- **A role is its listeners and what they serve.** The core role serves the Coral gRPC API, admits Run Executors and
  does Coral's part of every Run. The MCP role serves MCP requests. The auth role is the authorization server. A Coral
  Instance opens a listener only when it has that listener's role.
- **The core role holds both gRPC listeners.** The listener that Run Executors dial needs nearly the same modules and
  keys as the listener of the gRPC API, so the Run Executor link is not a role.
- **The Run Executor is a role**, because an operator deploys it as a service like the others. Run Executors perform
  it, each in a process of its own, as ADR 0027 requires. A Coral Instance that has the core role and the Run Executor
  role starts them as its local children. A process with the Run Executor role alone is a Run Executor that dials
  Coral.
- **The Run Executor role is selected with the core role or alone.** Coral refuses a selection that has it with the MCP
  or the auth role and without the core role. A Coral Instance starts and admits its local Run Executors where the Run
  Executor listener is, and a Coral Instance with only the MCP role that kept Run Executors of its own would share its
  restarts and its memory limit with the Programs.
- **The core role also does the work that no request starts.** At start it installs the Providers that Coral defines and
  updates the Built-in Provider's catalog. It runs the periodic passes and the sweeper. It does every search task: the
  index writes, the reconcile pass, the Embedder and its model. A Coral Instance without the core role does none of it
  and loads no model. It reads Universal Search from the database alone. Every deployment runs the core role, so an
  operator has nothing more to deploy.
- **Only the sweeper is elected.** The sweeper is the pass that covers what no Run stream covers. It ends a Run that is
  past its deadline, expires an Approval Request that is past its deadline, puts a taken Run that never started back to
  waiting, and interrupts the started Runs of a Run Executor whose lease has lapsed. The section "The clock, the leases
  and the sweeper" of [ADR 0027](0027-run-executors-compile-and-execute-programs.md) defines it. A lease row elects one
  Coral Instance with the core role to run it at a time. Every Coral Instance with the core role runs each other pass
  and each search task, and each of them stays correct when two Coral Instances run it at the same time. A new periodic
  pass gets an election only when a second run costs or breaks something.

[Name the Instance Roles and what each one serves](https://github.com/withcoral/coral/issues/3145#issuecomment-6912697006),
[Decide what the Run Executor role does in a process without the core role](https://github.com/withcoral/coral/issues/3149#issuecomment-6942562345),
[Decide which Coral Instance does the work no request asks for](https://github.com/withcoral/coral/issues/3147#issuecomment-6922008544)
and
[Decide who writes the search index and who loads the embedding model](https://github.com/withcoral/coral/issues/3148#issuecomment-6923122623)
hold the detail.

## Selecting roles

- **The roles are selected where the process starts, not in the config file.** One list, `--roles` on the command or
  `CORAL_ROLES` in the environment, names them: `core`, `mcp`, `auth` and `run_executor`. Every Coral Instance reads the
  same `config.toml`, which names no role. A process accepts the whole file and opens only the listeners of its roles.
  It reads every public URL in the file, and so one role learns the public URL of another. A role in the file would give
  each process its own copy of a config whose public URLs, database and keys must agree.
- **No role named means all four.** This is the default, and it is what a `coral` process did before this decision, so
  a deployment with one process changes nothing.
- **One setting selects all four roles**, so `runtime.local_run_executors` is removed: the Run Executor role beside the
  core role says the same thing. A process with the core role and without the Run Executor role needs
  `listeners.run_executor.token_env`, because no Run Executor could connect to it otherwise.
- **On SQLite, or without `[auth]`, the core, MCP and auth roles stay in one process.** Coral refuses a selection that
  separates them. The Run Executor role stays selectable there.
- **A role that lacks its config stops the process.** A process that started only the roles whose config is present
  would report healthy while a role the operator asked for does not run.
- **Two processes of one role need nothing from Coral.** The operator sets the number of processes and brings the load
  balancer. Coral adds no balancer, no discovery and no list of its processes. No listener needs stickiness, and
  balancing each connection is enough:
  [Decide when each role is ready and how a Coral Instance stops during an upgrade](https://github.com/withcoral/coral/issues/3317#issuecomment-7004739413)
  says what the balancer must do.

[Prototype how an operator selects Instance Roles: config, CLI and a Compose sketch](https://github.com/withcoral/coral/issues/3146#issuecomment-6921452131)
holds the detail.

## What a process holds

- **A Coral Instance needs only the secrets that its roles use, and it ignores the others.** So an operator can give
  each role its own secrets, or give every service the same ones. Every Coral Instance needs the database URL. Only the
  core role needs the credential vault key, what the Secret Stores read and the Run Executor tokens. Only the auth role
  needs the identity provider's client secret and the session signing key. Every Coral Instance still reads and writes
  the whole database.
- **Only the auth role holds the session signing key.** It publishes its public keys as a key set. A Coral Instance
  that has the core or the MCP role and not the auth role fetches the key set and keeps it in memory. It fetches from
  an address of the auth role inside the deployment when the operator sets one, and from the auth role's public URL
  otherwise. It keeps the last key set for as long as the auth role does not answer, so an outage of the auth role does
  not stop the requests that already carry a token.
- **Coral compares nothing between processes at start.** No check tells two processes that their vault keys, their
  signing keys or their settings differ. The docs state the rule and name each symptom. A stored check value would stop
  a deployment whose key is lost from starting with a new one.
- **The session signing key, a public URL and the `[auth]` settings change with every Coral Instance stopped.** Other
  settings can differ while the processes restart one at a time.
- **Each Coral Instance has its own config folder.** On Postgres nothing in it must outlive the container.

[Decide what every process in a split must share, and how it gets it](https://github.com/withcoral/coral/issues/3150#issuecomment-6943678096)
holds the detail.

## Ready, and stopping

- **A Coral Instance gives one readiness answer, the same on every listener.** It is ready when every listener of its
  roles is open, the database answers and has every migration of its plan, it holds a key set or has the auth role, and
  it is not stopping. The search model, a linked Run Executor and the identity provider stay outside the answer. A Coral
  Instance that waits for one of these conditions answers alive and not ready, and it answers a request as unavailable.
- **A listener whose task ends stops the process**, whatever its role, so the orchestrator starts it again.
- **The Shutdown Drain is the whole stop, and it follows the work, not a clock.** At the stop signal a Coral Instance
  reports not ready at once. It finishes the dispatches in flight of its Runs, as ADR 0027 says, and the requests in
  flight on its listeners, and it closes when nothing is in flight. With no work it takes no time. One setting limits
  the whole length, and no setting adds a time to keep serving.
- **The operator's balancer stops sending to a process before that process gets the stop signal.** Nothing tells Coral
  that a balancer has stopped sending, so Coral does not cover a request that arrives after the close.
- **`coral-web` stops by the same rule.**

[Decide when each role is ready and how a Coral Instance stops during an upgrade](https://github.com/withcoral/coral/issues/3317#issuecomment-7004739413)
holds the detail.

## Postgres

- **One setting sizes the pool of every Coral Instance, whatever its roles.** Each Coral Instance also holds one
  connection that listens for changes. So each one costs Postgres up to `pool_size + 1` connections. A Run Executor on
  its own and `coral-web` open none. Coral checks nothing against the connection limit of Postgres.
- **Coral connects directly to Postgres.** It has no second database URL for a connection pooler. Coral is not tested
  behind a pooler, and a pooler in transaction mode does not work.

[Decide how a split sizes Postgres: the pool, the change-signal connection and the one channel](https://github.com/withcoral/coral/issues/3318#issuecomment-7006476640)
holds the detail.

## What Coral does not add

- **Coral adds no way for a process to see the other processes of its deployment.** It keeps no record of them and
  shows no page of them. The docs say what a user and the operator see when a role has no process.
- **One `[outbound]` holds for every Coral Instance.** No role has a policy of its own. An address of one of Coral's own
  processes that the operator writes in `config.toml` is not under `[outbound]`.
- **Coral adds no label for the roles to its telemetry.** The operator sets resource attributes on each service.

[Decide what an operator sees at the edges of a split: a missing role, the outbound policy, telemetry and `runtime.local_run_executors`](https://github.com/withcoral/coral/issues/3319#issuecomment-7007046278)
holds the detail.

## Rolling upgrades

A Postgres deployment with more than one Coral Instance upgrades one process at a time, whether or not it separates the
roles. A deployment with one process still upgrades by a restart.

- **Coral names no window of versions.** A change is compatible: a Coral without it keeps working beside a Coral that
  has it, however old the first one is. A change that cannot be compatible is a breaking change, and it is staggered
  across releases. One release makes every Coral able to do without the old thing or to read the new one, and a later
  release makes the break. A promise stated as "the next version only" or "the last N versions" limits what an operator
  can skip, so Coral makes none.
- **Only a Coral Instance with the core role updates the database, at start.** One lock covers a migration run, and an
  operator runs no separate migration step. A Coral Instance without the core role changes nothing. It is not ready
  until the database has every migration of its plan.
- **An older Coral starts and keeps working on a database that a newer Coral has updated.** v1.9.0 is the first version
  that does.
- **Between two versions, Coral refuses to start for one kind of break only: a removal in the database.** Coral orders
  two builds by the steps of their migration plans, because a version does not order two builds of `main`. A removal
  names the step of the change that stopped using what it removes. The repository's check accepts the removal only when
  a release has that step, so no release holds both halves of a stagger. On Postgres, Coral refuses an upgrade that
  skips the step, with no override, and it names the release to start first.
- **For every other break, the stagger is a rule for the author, and Coral checks nothing about the other version at
  start.** This covers a stored value, an encoding that two versions both read (the sealed sign-in cookie, a token, the
  key set, a page token, a stored encoding) and a setting. No build carries a compatibility level.
- **A Coral shows a stored value that it does not know as unknown, where it only shows the value.** A new value there
  is not a breaking change. Where a Coral decides on a value, such as a Run's state, a new value is staggered.
- **The gRPC API changes by protobuf's rules.** A method or a field that goes away is marked `deprecated` in one
  release, then deleted and reserved in a later one. `coral-web` and Coral exchange no version.
- **A migration does not make the running Corals wait.** Where Postgres has a form that blocks writes and a form that
  does not, a migration uses the form that does not.
- **A new setting has a default, and a setting that goes away leaves in one release.** That release does not start on a
  config that still holds the key, because Coral refuses any key that it does not know. This checks the operator's
  config, and it compares no versions.
- **The order of an upgrade**: `coral` processes in any order, `coral-web` after every Coral Instance with the core
  role, and Run Executors at any time, under the protocol level of ADR 0027. A newer Coral Instance without the core
  role is not ready until one with the core role has updated the database. The operator finishes one upgrade before the
  next one starts: the database shows that a Coral with a step has started, not that every older Coral has stopped.
- **The version that an upgrade started from starts again on the database as it is.** On Postgres it needs no restore.
  The rules above give this, and Coral has nothing built for it: a removal waits for a release, a break outside the
  database is staggered, and that version already ran beside the newer one. A version older than that one is not
  promised. The docs still say to restore the backup, and the way back has no test of its own.
- **A contributor rule gets a check only where a tool that the repository already runs can read it.** Those are the
  migration check, Squawk and Buf. Coral writes no checker of its own, and every other rule stays with the author. The
  "Rolling upgrades" section of the root `AGENTS.md` holds the rules.
- **Separate roles can ship before rolling upgrades do.** Until then the docs say to stop every process for an upgrade,
  and afterwards they name the first release that supports a rolling upgrade.

[Decide which version skew a rolling upgrade must tolerate](https://github.com/withcoral/coral/issues/3151#issuecomment-6948966389),
[Decide how migrations run during a rolling upgrade](https://github.com/withcoral/coral/issues/3152#issuecomment-6985190362)
and
[Decide the last rules of a rolling upgrade: rolling back, search revisions, and where each contributor rule is enforced](https://github.com/withcoral/coral/issues/3320#issuecomment-7014323154)
hold the detail.

## The proof

Correctness proves that a role scales out. There is no load target.

- The Postgres run of the product journeys runs on a deployment with two processes of every role, one role in each
  process, and each process holds only the secrets of its roles.
- An upgrade suite starts that deployment on the newest release and replaces one process at a time with the build
  under test, while a Run waits across the upgrade. It fails on any "unavailable" answer.
- Every other selection and rule has one test at its owner.

[Decide what evidence proves the split, two of every role, and a rolling upgrade](https://github.com/withcoral/coral/issues/3153#issuecomment-6985767170)
holds the detail.
[Prototype the Docker Compose, Kubernetes and upgrade pages for a split](https://github.com/withcoral/coral/issues/3321#issuecomment-7015504910)
holds the outline of the operator's pages.

## Consequences

- Every commit that an observer can see sends one signal, and every Coral Instance hears it. Postgres commits those
  transactions one at a time. More Coral Instances add wake-ups for each commit and do not add commits each second.
  Coral promises no rate.
- On a large backlog, the Embedders of several Coral Instances with the core role embed the same rows. Each write checks
  staleness, so this costs CPU only ([ADR 0035](0035-universal-search-adds-a-vector-list-from-a-bundled-cpu-model.md)).
  Embedders that divide one backlog are not part of this design.
- A change to how Coral indexes or embeds is compatible: search rows are derived, and any Coral builds them again.
  While two versions run, they may do each other's work again, and the version that stays finishes it. When a release
  changes the text that Coral indexes, each version writes again the search rows of Private and Shared Tools that the
  other one wrote, on every reconcile pass, and both embed those rows again. When a release changes the embedding model
  or the text that it embeds, each version overwrites the vectors of the other one. No search fails. A result ranks by
  words alone where its vector is from the other version.
- A change to the Built-in Provider's catalog is compatible. The catalog of every Workspace follows the Coral that
  started last, forward or back, as [ADR 0021](0021-admin-operations-as-a-built-in-provider.md) decides.
- Each Coral Instance with the core role keeps its own answer to the release check, so the notice of a new release can
  differ between page loads for some hours.
- A Coral Instance that runs its own Run Executors ends their Runs when it stops. A deployment that must lose no Run in
  an upgrade runs each Run Executor as a process with the Run Executor role alone.
- Nothing in Coral reports that a role has no process. A Program that waits to its limit because no Run Executor takes
  it leaves one warning in the log.
- An upgrade from a version older than the first release that supports a rolling upgrade stops every process first.
- A deployment with more than one Coral Instance that has the core role sets the credential vault key in the
  environment. Without it each of them creates its own key, and an Account that one seals fails on another.

## Considered options

- **A `roles` key in `config.toml`.** Rejected: each process then carries its own copy of the config, and a Run
  Executor reads no `config.toml`.
- **A role for the Run Executor link.** Rejected: the two gRPC listeners need nearly the same modules and keys.
- **A Coral Instance with the MCP role and without the core role that starts Run Executors of its own.** Rejected: it
  would be a second kind of supervisor with a fixed number of children, and its restart would end their Runs.
- **Every process holds every key.** Rejected: an operator could not narrow what a role holds.
- **The public half of the signing key in `config.toml`, or a key set that expires.** Rejected: the first adds a step
  for the operator, and with the second an outage of the auth role longer than the expiry stops the other roles.
- **One readiness answer for each role.** Rejected: the conditions are the same for every role, and an orchestrator
  keeps one answer for each container.
- **A set time that a Coral Instance keeps serving after it reports not ready.** Rejected: the stop follows the work.
- **A second database URL for a pooler.** Rejected: a second URL alone does not make a pooler work.
- **A telemetry label for the roles that Coral sets.** Rejected: the operator's tooling already labels a service.
- **A window of versions: the next version only, or a floor that each release names.** Rejected: both state the promise
  as a limit on what an operator can skip.
- **One compatibility level that every build carries, for every break.** Rejected: migration steps already order two
  builds in the database, and gRPC and protobuf already say how an API changes.
- **Every Coral Instance updates the database, or a separate migrate command.** Rejected: a Coral Instance without the
  core role could then be the one that migrates, and a command adds a step to every upgrade.
- **Checks that Coral writes itself for stored values and settings.** Rejected: a rule gets a check only where an
  existing tool can read it.

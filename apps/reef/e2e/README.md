# Reel

Reel is Coral's standalone tool for recording and reviewing browser-test scenarios,
not a feature of Reef. One real-browser scenario produces automated verification and watchable evidence.
This suite is separate from Reef's fast Vitest tests, architecture tests and
Storybook/Chromatic. It uses Vitest, Playwright Chromium and optional ffmpeg.

## Run and review

Run the Linux container on macOS or Linux with Node 24+ and Docker or Podman.
The image builds Coral, Reef, Chromium, ffmpeg and the fixtures. No host certificates,
network aliases, browser dependencies or administrator networking setup are required.
From the repository root:

```sh
npm ci --prefix apps/reef

# Main suite (default): genuine Coral sign-in, real Coral and production Reef, SQLite.
npm run test:e2e --prefix apps/reef
# The same product scenarios, with a disposable Postgres database.
npm run test:e2e --prefix apps/reef -- --database postgres
# Small suites for deployment differences, not repeated product journeys.
npm run test:e2e --prefix apps/reef -- --suite local-mode
npm run test:e2e --prefix apps/reef -- --suite desktop
npm run test:e2e --prefix apps/reef -- --suite multi-instance
# Remaining arguments reach Vitest unchanged; select a file or test name.
npm run test:e2e --prefix apps/reef -- providers/catalog/
E2E_FILM=1 npm run test:e2e --prefix apps/reef -- shell/navigation
npm run test:e2e --prefix apps/reef -- --help

# Also verify assertion/timeout cleanup, playable WebM fallback, and viewer safety.
REEL_SELF_CHECK=1 npm run test:e2e --prefix apps/reef

npm run e2e:serve --prefix apps/reef
# Open http://127.0.0.1:9324/ and select a scenario.
```

### One product suite, plus small tests for the differences

| Suite            | What it proves                                                                                           | Discovery                               |
| ---------------- | -------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| `main` (default) | Provider, Account, Workspace, Agent, activity, identity and permission journeys, with genuine sign-in    | `scenarios/main/**/*.test.ts`           |
| `local-mode`     | No sign-in ceremony, implicit Owner Membership, lifecycle fences, loopback access and parity scenarios   | `scenarios/local-mode/**/*.test.ts`     |
| `desktop`        | The packaged Desktop app: its supervisor, its window, background, quit and relaunch                      | `scenarios/desktop/**/*.test.ts`        |
| `multi-instance` | Two Coral Instances on one Postgres database: a pending request through either one reaches the Run Owner | `scenarios/multi-instance/**/*.test.ts` |

`main` is the default; `--suite main` selects it explicitly. `local-mode` means the
no-sign-in deployment, not tests running on your machine.

`--database sqlite` is the main suite's default. `--database postgres` changes its
storage configuration, not its scenario selection. Both run all discovered product
files. Database selection is unavailable for the focused suites: local and Desktop
use SQLite, and multi-instance uses Postgres.
Single-User and multi-User stories belong together. Provider OAuth is ordinary
product coverage, separate from the Coral sign-in used to arrange actors.

Every suite runs Reef's bundled server, `server.mjs`, as Docker and Desktop ship it.
Every deployment ships the same Reef build. `local-mode` runs it with sign-in disabled,
as Desktop does, so it tests Desktop's Reef without Electron.

`--suite desktop` stages the runtime and Desktop's Reef, packages the unpacked
Linux app, and launches it in Electron for each session. Each scenario owns a home
folder, so Desktop's data lands in it through `HOME` and `XDG_CONFIG_HOME`; the app
has no test hook. Playwright reaches Electron through Node's inspector, which the
shipped fuses turn off, so the runner turns on that one fuse in its disposable
package. Every other fuse stays as shipped. Electron contexts take no `baseURL`, so
the harness resolves a relative `page.goto` against the window's Reef origin.
`withDesktopApp` resolves with the app's exit, so a scenario can tell a clean Quit
from the harness's fallback kill. The system browser is a test boundary:
`recordSystemBrowser` replaces `shell.openExternal` in the main process and
records each URL, and a separate Playwright browser plays the person's browser. A native
question is one too: `answerDialogs` replaces `dialog.showMessageBox` and records each
question. The suite needs a display: the container
provides Xvfb. It proves only what the Electron host adds. A scenario that would
pass without Electron belongs in `local-mode`. Desktop's runtime completes Provider
OAuth on the fixed port 8779, so one Desktop runs at a time per network namespace.
A native run collides with an installed Desktop; each container run has its own.

`E2E_DESKTOP_EXECUTABLE` names an installed app for the desktop suite to drive
instead; the runner then builds nothing. Turn on the inspector fuse in that copy
first. The Desktop on Windows workflow drives the installed Windows app this way,
with `npm run test:e2e:native`. On Windows the suite does not rely on variables to
move Desktop's profile, so each scenario uses the real
`%APPDATA%\com.withcoral.app`. Setup refuses to start if that folder exists. Each
scenario deletes what the previous one left, and the last one's data stays. Run
it only on a disposable machine.

Each command creates and prints **one run directory** for its selected suite and
database. Only the selected suite appears in the run; an unrequested suite is not
reported as passed. Vitest name filters can report other tests in selected files as skipped. Missing prerequisites fail setup, rather than skipping tests.
Separate commands always get separate run identities. Direct links have the form
`http://127.0.0.1:9324/<run-id>/index.html`; scenario pages are linked from there.
The landing page and run pages group scenarios by their source folders, with nested
folders and scenarios indented. Folder headings show outcome counts for their full
subtree. Folders sort by name; failures and incomplete scenarios lead within each
folder. Each scenario retains its own outcome, target, duration and evidence link.
Run history adds readable dates, commit/dirty state and film/fast mode;
harness self-checks live in a separate disclosure because they include deliberate
failures. Refresh to discover newly finalized evidence. Each row is one actual
Vitest test scenario, identified independently of its name. Its outcome and total
elapsed time come from the runner after assertions and hooks finish; session
durations are never summed to manufacture a test duration. A started test without
a final outcome stays visibly incomplete. Skipped tests and failures before any
browser opens still have scenario results.

Open a scenario to see its source, whole-test failure, and recordings in start
order. Actor, recording label and setup/interaction purpose are separate fields;
repeated logins retain separate recording numbers. Retries/repeats keep their
recordings under separate attempts, with the runner's final outcome on the parent.
Recording pages provide compact named screenshot thumbnails and inline source.
Recordings autoplay muted with playback controls and inline mobile playback.
Use **Playback speed** to select 0.1×, 0.25×, or 1× (normal). Changing speed
does not restart playback or change the recording. Step times are
since session start, **not video seek positions**. Source snapshots may contain
other tests from the same file. Raw metadata, source, video and traces remain
available as secondary downloads. Inspect traces locally from `apps/reef` with:

```sh
npx playwright show-trace e2e/runs/<run>/<target>/<scenario>/sessions/<session>/trace.zip
```

Fast mode records without artificial pauses. Film mode uses 400 ms Playwright
slowdown and 1.5 seconds after each successful step. Neither replaces readiness
checks. Playwright records the page viewport, **not** native browser chrome,
Desktop windows or other popup pages.

Serve the HTML through `e2e:serve`, rather than opening it as a local file. The
standalone viewer lives in `e2e/viewer`, outside Reef's app and routes. It renders
Wax on the server using the shared static-build tooling. There is no SPA or Reef
browser runtime. A small, hash-authorized script enables playback-speed buttons;
navigation, source disclosure, and native video controls work without JavaScript.
Each generated page stores a content-addressed Wax stylesheet snapshot beside its
evidence, including fonts and any SSR layout styles. Archived pages therefore keep
their styling when Wax changes. The landing page compiles once per process and reads
current run metadata on each request. The CSP permits only the exact trusted
playback script, not scripts from evidence or arbitrary inline code. Exact-host
and file-containment checks remain in force; inline styles are not permitted.
Both themes follow the browser's system preference.

To apply the current Wax design to already-recorded runs (including pages created
before stylesheet snapshots), regenerate their presentation without rerunning the
tests or changing recorded media/source:

```sh
npm run e2e:refresh-viewer --prefix apps/reef
```

Restart `e2e:serve` after changing viewer code; its renderer is compiled once.

## View over a tailnet

Remote viewing is opt-in. On the machine running the viewer, use its exact
Tailscale HTTPS origin (replace the example hostname with your own):

```sh
E2E_RESULTS_ORIGIN=https://reef.example.ts.net:9324 \
  npm run e2e:serve --prefix apps/reef
# In another terminal; this command changes Tailscale configuration, not the viewer.
tailscale serve --bg --https=9324 http://127.0.0.1:9324
# Open https://reef.example.ts.net:9324/ from your tailnet.
```

The viewer still binds only to `127.0.0.1`. It accepts the local address and the
exact configured host and port, not other tailnet names or forwarded-host headers.
Use one canonical HTTP(S) origin, optionally ending in `/`, without credentials,
a path, query, fragment or wildcard. This setting does not configure TLS or
Tailscale; HTTPS terminates at Tailscale Serve. Omit it to retain localhost-only
Host checks. Restart the viewer when changing settings.

**The viewer has no authentication.** Anyone permitted by your tailnet policy can
read all retained evidence through Serve. Review that policy before enabling access;
the hostname check is DNS-rebinding protection, not authentication. Do not use
Tailscale Funnel or expose the viewer publicly. When finished, stop the viewer and
remove just this Serve listener with `tailscale serve --https=9324 off`.

## Environment ownership

All suites run real Coral plus the production Reef server in private temporary
state. The main suite uses SQLite or Postgres and genuine Coral sign-in.
Local and Desktop checks use SQLite and the implicit Organization Owner. They use
loopback port zero. The product fixture reserves ephemeral loopback ports before constructing
OAuth origins, releases each reservation just before its owning child binds, and
fails startup if another process claims the port. It never falls back to that
other server. It discovers only the child processes' reported
addresses and waits for MCP and Reef `/readyz` (including Reef's real private gRPC
health call). It never attaches to a conventional port or reads developer Coral,
Postgres, auth or telemetry settings. Local mode uses the actual implicit
Organization Owner and normal authorization, not an injected bypass.

Declaration-only Provider fixtures use HTTPS placeholder hosts and make no outbound
calls. On the main suite, discovery and dispatch fixtures advertise
`http://sidecar:<ephemeral-port>`.
The container network resolves `sidecar` to this container's private address, not loopback.
Authenticated Coral explicitly selects `private_networks = "http-and-https"`.
Loopback stays denied, so rejection scenarios retain independent negative controls.
Local Coral keeps its default policy, which reaches loopback services and denies
private networks, so local and Desktop fixtures listen on loopback.

No hosted credentials are needed. Dummy Provider credentials use filesystem
storage inside disposable fixture state,
never the OS keychain. Do not seed tables directly or add production test controls.

Teardown stops Reef before Coral, escalates a stuck shutdown to SIGKILL, and removes
state only after the processes stop. Logs remain with the run. Each browser session
has a fresh context and unique artifact directory, including multiple sessions or
retries. Scenarios run serially until resource isolation supports parallelism.

## Container ownership and HTTPS acceptance

`test:e2e` builds and runs `container/Dockerfile`. Each invocation owns a unique
container and network. Browser, application, identity provider, fixture listeners
and (when selected) Postgres all run inside it. No daemon socket is mounted, no
host ports are published, and no host networking mode is used. The browser's OAuth
hops resolve the same fixture hostname as Coral; MCP streaming and cancellation
use ordinary direct connections, without a reverse proxy, except on the
multi-instance suite.

Cargo registry/git/target and npm build caches survive image rebuilds. Reef builds
inside each run. The cost is a Linux build even on macOS; native builds cannot be reused. Linux
container evidence proves the Linux Electron app, not native macOS, Windows or
installer behavior.
For focused native checks, install native build/browser dependencies and use
`npm run test:e2e:native --prefix apps/reef -- --suite local-mode`. A native
`--suite desktop` run packages and launches the Linux app, so it needs Linux, a
display and Electron's libraries, unless `E2E_DESKTOP_EXECUTABLE` names an installed
app.
The native process runner also supports explicit disposable attached local entry
checks with `E2E_BASE_URL` and `E2E_ALLOW_ATTACHED=1`. It does not provision host trust.

At every container start, `container/network.ts` runs the real guarded Rust client
and Chromium against `http://sidecar:5000`, trusted `https://sidecar:5443`, and
untrusted `https://sidecar:5444`. HTTP must work only with the selected permission;
HTTPS must verify its certificate even with HTTP enabled. The main
`providers/setup/network-policy` scenarios also publish and invoke Providers over
HTTP and both HTTPS certificate cases using a real signed-in User and Workspace MCP.
`providers/setup/consent-navigation` separately verifies private HTTP browser consent
and reuse while Coral permits only private HTTPS requests, including a real TLS token
exchange. It rejects an HTTP hostname mapped to a public IP and an HTTPS metadata
literal before browser navigation. The public DNS probe entry exists only inside
the disposable container. This storage-independent control owns a separate SQLite
instance on either product suite. Browser screening is issuance-time evidence, not
a guarantee about browser DNS or subsequent redirects; see ADR 0009.

The entrypoint generates two short-lived CAs. Only one enters the container's
native and Chromium trust stores. Keys and trust disappear with the container;
none is mounted from, installed on, or exported to the host. The existing fixture
TLS preflight remains: explicit TLS settings need complete, readable files and a
certificate matching the advertised host. Product certificate rejection is tested
at the real connection, not by treating preflight rejection as transport evidence.

Artifacts write directly to `apps/reef/e2e/runs/<timestamp-uuid>/` while tests run.
The runner prints each host path before starting the container. Refresh the viewer
to inspect completed recordings and current scenario outcomes without waiting for
shutdown. Do not delete an active run directory. Each container mounts only its own
final run directories; a harness self-check has a separate directory from product
scenarios. Recordings, traces, screenshots, logs and source snapshots survive failed
runs and interruption without moving to another path. Signals stop the owned container;
its init process reaps children. The runner removes its container and network on normal
completion or failure. An uncatchable host kill can leave resources named
`coral-reel-<uuid>`; its evidence remains at the printed run paths.
Inspect and recover only that invocation's resources.
Never prune unrelated work. Podman disables SELinux labeling for this test container
rather than relabeling host artifact directories; no host CA or interface changes occur.

Supported host options are `REEL_CONTAINER_ENGINE` (`docker` or `podman`),
`REEL_SELF_CHECK=1`, and `E2E_FILM=1`. Other developer credentials, database URLs,
fixture settings and TLS-disable variables do not cross into the container.
Viewer settings remain `E2E_RESULTS_PORT` and `E2E_RESULTS_ORIGIN` as described above.

### Remove an older host-managed setup deliberately

The removed native HTTPS fixture setup is not automatically uninstalled. Stop its native E2E runs.
Use the last implementation in an isolated historical worktree solely to inspect
and uninstall its owned resources:

```sh
git worktree add --detach /tmp/coral-fixture-uninstall 22674f1288f6d5b1760c925535efa04cc25b85a5
cd /tmp/coral-fixture-uninstall
./dev e2e-fixtures status
# Review the reported address and certificate ownership before proceeding.
./dev e2e-fixtures uninstall
```

Use the same OS user and original `E2E_FIXTURE_HOME`, if customized. Uninstall
refuses active runs and removes only recorded owned aliases and exact CA trust.
It may request administrator authorization. If ownership is uncertain or removal
fails, retain the setup records and inspect the reported resources manually;
do not delete whole keychains, NSS stores or unrelated interface addresses.
Remove the historical worktree after successful cleanup. This is a deliberate
removal path, not a maintained provisioning or compatibility interface.

## Product on Postgres

`npm run test:e2e --prefix apps/reef -- --database postgres` runs the same discovered
product files. The entrypoint starts an isolated Postgres database inside the run
container. The native process runner passes its explicit disposable URL through
`scripts/postgres.ts`; deleting the container removes all database storage.
No User session is relabeled as an Agent. Separate Coral/Reef processes and genuine
OAuth stay identical to SQLite. Connection strings remain outside artifacts.
The storage-independent default-policy negative control always owns a separate
SQLite instance; it never shares the product target's database or Organization.

`make postgres-tests` remains the Rust repository gate and owns its own disposable
database through `scripts/postgres.ts`. Repository tests alone do not prove product
journeys. Native macOS and Windows 11 Desktop checks remain separate acceptance work.
The restart scenario for boot recovery is separate, open work.

## Several Coral Instances

`--suite multi-instance` runs two Coral Instances on one disposable Postgres
database behind a round-robin proxy. It proves that an outside request made
through one Coral Instance reaches the Run Owner on the other, when the record
does not already answer it
([ADR 0025](../../../docs/adr/0025-outside-run-requests-reach-the-run-owner-through-the-run-inbox.md)).
It signs in as the main suite does. Postgres is the suite's only database, so it
takes no `--database`.

- The proxy fronts Coral's gRPC, MCP and auth listeners. Reef, MCP clients and
  browser sign-in reach Coral only through it. Both instances advertise the
  proxy's origins as their public URLs.
- It balances each request, not each connection. Reef keeps one HTTP/2
  connection to Coral, so a per-connection balancer would never move Reef's
  requests between instances.
- A request in a 2025-11-25 MCP session goes to the instance that issued the
  session, which holds it in memory. A 2026-07-28 request has no session.
- The instances share the configuration, the session signing key and the
  credential key (`[credentials] key_env`). Only their listener ports and state
  roots differ. The second instance starts once the first is ready, so it never
  reads a half-migrated database. Each writes `coral-1.log` or `coral-2.log`.

A scenario steers the proxy with `coralInstances()` from `support/instances.ts`:

- `startOn(i, start)` starts a Run through instance `i`. The instance that
  accepts a Run holds it.
- `holder(run)` reads the Run row's `holder_instance_id` with `psql` and returns
  the instance's index. Coral shows an instance's id only there, so the harness
  learns each id from the first Run started through that instance.
- `on(i, act)` sends every request outside an MCP session to instance `i` while
  `act` runs.
- `during(act)` returns the requests forwarded meanwhile, each with its client
  connection, the instance that served it and its gRPC status.
- `expectNoUnavailable(requests)` fails on any HTTP 503 or gRPC UNAVAILABLE.

Each cross-instance journey is one file in `scenarios/multi-instance/`: an Account
connected through one instance continues a Run that the other owns; Reef cancels
such a Run over its one connection; Reef approves a call on one; and an MCP client
on 2026-07-28 answers an approval prompt through the instance that does not own
the Run. A native run needs `psql` on `PATH`; the container has it.

## Authenticated multi-User fixture

The main suite boots the maintained `oidc-provider` package in memory,
with per-run keys and synthetic accounts. Its development-adapter warning is
intentional: this test IdP must disappear at teardown. It is not a Coral auth
replacement and must never be exposed outside loopback.

Reef and Coral run their normal OAuth/OIDC paths: discovery, authorization,
PKCE, signed ID-token validation, User provisioning,
Coral code/token exchange, and genuine Reef session cookies. Loopback HTTP uses
explicit registered resources and trusted clients. `listeners.web.public_url`
names the actual reserved Reef origin for human navigation; loopback HTTP does not
acquire the HTTPS metadata client's derived audience or consent-bypass trust.
Local-mode fixtures likewise reserve Reef's port before Coral starts and pass its
actual origin as `CORAL_LOCAL_WEB_URL`. Without sign-in, they also pass the loopback
Provider OAuth callback that Coral prints at startup, the listener Desktop uses, as
`CORAL_UPSTREAM_OAUTH_CALLBACK_URL`. Provider secrets are explicit child bootstrap
values and Coral's signing key is a private file in disposable state.

For scenarios that require authenticated actors (for example, under `scenarios/main/workspaces/`):

```ts
import { test } from 'vitest'
import { createActors } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'

test('A management journey', async (context) => {
  const { owner, alice, bob } = await createActors(context)
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('Open Reef as Alice', async () => {
        await page.goto('/')
        await page.getByRole('button', { name: 'Open account menu', exact: true }).waitFor()
      })
    },
    { actor: alice.label, storageState: alice.storageState },
  )
})
```

- `owner` logs in as the exact configured bootstrap Organization Owner subject.
  `alice` and `bob` are fresh eligible identities provisioned as Organization
  Members, not Workspace Members. There is no role claim or test grant bypass.
- `newUser(context, label)` plus `loginActor(context, user)` creates additional
  Users. `signIn(page, user)` drives a fresh browser's actual login flow when
  login itself is the recorded behavior. Setup logins are also recorded, with
  actor labels, then normal scenarios reuse their genuine storage state.
- `subject` is the upstream OIDC subject; `userId` is Coral's separate, stable
  User identity. Repeated login retains `userId`; another User gets a different
  one. Do not use the upstream subject as a Workspace Membership's User ID.
- An Actor includes a `reef`-audience bearer obtained through a second genuine
  PKCE OAuth client, using the same IdP session. This enables private management
  setup without decrypting Reef cookies or importing Reef server internals.
  `management.ts` exposes resource-specific Workspace, Membership, Agent, and
  Registration clients. Test bindings are generated from the canonical proto files into the
  ignored `e2e/generated/` directory by `npm run e2e:proto:gen` (automatically
  run by E2E and typecheck). These clients and the fixture are not part of the
  production build; `e2e/` is excluded from production image contexts; only the test image includes it.
- Keep tokens and storage state in memory, never in result metadata or checked-in
  files. **Playwright traces of login and token exchange do contain disposable
  credentials**. Token acquisition leaves the recorded page on Reef; the login
  recording also shows the clearly labeled synthetic identity provider.
- Test finalizers remove synthetic IdP accounts; global teardown removes the
  entire Coral state. Removing an IdP fixture account is not Coral User
  disablement and does not revoke an already-issued Coral credential.

The fixture tests prove distinct/stable authenticated identities, real private
credential acceptance versus unauthenticated rejection, independent Reef sessions,
logout isolation, and successful eligible login with unverified email while exact
email grants remain unavailable. Colliding verified addresses preserve separate
identities and reject grants without candidate disclosure. A committed request replay
after email reassignment cannot select a new target. The management scenarios also
prove that an Organization Owner has no Workspace access without a Membership and can
assign an Owner from the organization Workspaces page, one-submit Membership
creation, role changes and removal/leave, concealed unrelated Workspaces,
Member-only view models, denied forged actions, and Agent lifecycle. Agent probes
use the real Registration credential at the MCP listener, not a User session
labeled as an Agent. No hosted account, invitation, or live WorkOS credentials are
required. Arrange additional prerequisites through real APIs, never database
seeding or test-only production routes.

## Writing a scenario

Keep all scenarios together, separated first by suite:

```text
scenarios/
  main/
    accounts/
    activity/
    agents/
    identity/
    organization/
    providers/
    shell/
    topology/
    workspaces/
  local-mode/
    parity/
  desktop/
  multi-instance/
```

Group `scenarios/main/` by the product behavior being proved:

- `shell/`: navigation, breadcrumbs, scrolling, and browser-shell behavior.
- `identity/`: sign-in, stable identity, and independent sessions.
- `organization/`: Organization Membership administration and removal.
- `workspaces/`: Workspace lifecycle, authority, and Membership management.
- `agents/`: Agent and Registration lifecycle.
- `providers/`: Provider lifecycle and authority, with `setup/`, `catalog/`,
  `policy/`, and `updates/` for focused scenarios.
- `accounts/`: scoped Account lifecycle, authority, renewal, and connection requests.
- `activity/`: execution history and its navigation.
- `topology/`: what a composed deployment does not expose.

Keep authority tests beside their feature. Place OAuth and MCP scenarios by the
behavior they prove, not the protocol fixture they use. Keep cross-feature journeys
intact under their primary product area. Shared scenario definitions live beside
their entry files; `support/` contains harness code, fixtures, and helpers.

Every `*.test.ts` under `scenarios/main/` belongs to the main suite on both databases.
New or renamed files need no registry edit. Keep the few deployment-difference
checks in `scenarios/local-mode/`, `scenarios/desktop/` and `scenarios/multi-instance/`; do not add capability tags
or per-database lists. `configuration.test.ts` retains the formerly shared assertions;
`oauth-configuration.test.ts` retains the formerly local-only configuration assertions.

### Run one scenario in both topologies

A parity scenario asserts the same domain facts in the remote and local topologies.
Its shared definition is a plain `.ts` module beside its main entry file. The main
entry and a `scenarios/local-mode/parity/` entry each register it with the same
variants. The shared assertions are the parity catalog; do not compare runs.

```ts
// scenarios/main/accounts/agent-account.ts
export async function agentAccount(context: TestContext) {
  await withOwner(context, async ({ page, target, step, accessToken, approveConsent }) => {
    // Pass accessToken to management clients; it is undefined locally.
    // approveConsent() replaces a click on the fixture's approval link.
  })
}
```

`withOwner` in `support/topology.ts` holds how the Owner acts in each deployment.
On the main suite it records a genuine Owner sign-in. Locally it uses the implicit
Organization Owner. Facts about the composed stack, such as its public origins,
come from the target. `approveConsent()` approves fixture Provider
consent and returns to Reef. Remote Reef's callback does that itself. Locally, Coral's
loopback listener completes the sign-in, and the browser opens the page that Reef
named in `X-Coral-Authorization-Return-To`, as Desktop does.
Assert identity through Coral, such as the Workspace's `currentUser`, not a signed-in
Actor. Each source snapshot appends the shared definitions its entry imports, so the
evidence shows the journey that ran.

The local topology runs every Workspace-local Provider and Provider Account journey through `parity/`.
Journeys that need a second User, such as Workspace authority or Organization
Membership, cannot apply without sign-in and stay in `main`.

Use a product-relative path to select a file, such as `workspaces/lifecycle`, or a
folder, such as `providers/catalog/`, with either database.

For a scenario in a product directory under `scenarios/main/`:

```ts
import { test } from 'vitest'
import { withOwnerSession } from '../../../support/actors.ts'

test('A named human journey', async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    await step('Open Coral', async () => {
      await page.goto('/')
      await page.getByRole('navigation', { name: 'Coral', exact: true }).waitFor()
    })
    // Use actor.accessToken for private management setup.
    // programRunner uses this browser User's real Workspace OAuth grant by default.
    // Perform the behavior under test through the UI and assert domain facts.
  })
})
```

The scenario owns actions, meaningful readiness checks, assertions and resource
cleanup. The harness owns browser lifecycle, tracing groups, numbered screenshots,
pacing, video and metadata. `page` is ordinary Playwright; no workflow DSL or
browser request mocks are necessary. Use locators/response conditions, not global
`networkidle` or scenario sleeps. API prerequisites must not perform the behavior
that the recording claims to exercise through the UI.

`withOwnerSession` records a genuine Owner login, then opens the interaction with
that Actor's storage state. It never silently falls back to the implicit local User.
For multi-actor and identity scenarios, pass an Actor's genuine Playwright
`storageState` and safe label to `withBrowserSession`, as above. The label
only names review evidence; it does not select authority. Optional `label` and
`purpose: 'setup' | 'interaction'` identify the recording's role. For example,
`{ actor: 'Alice', label: 'Login', purpose: 'setup' }` records a login without
creating another scenario. Ordinary browser sessions default to a Reef interaction.

Browser actions/navigation have finite timeouts. Vitest's per-test finish hook
closes and finalizes tracked resources even if the callback hangs on a promise
that does not observe cancellation. Normal callbacks close their session promptly
so multi-actor tests do not accumulate idle recordings and live browsers. A later
assertion or cleanup failure belongs to the parent scenario; completed recordings
retain their own outcomes. A closed page cannot produce a later failure screenshot.
The custom Vitest reporter owns scenario identity, final result and total timing;
`withBrowserSession` owns only its uniquely identified child recording. Artifact
attempts are bounded and independent;
a screenshot/trace failure cannot prevent browser shutdown or replace an original
assertion/timeout. A green recorded test requires video, trace and browser cleanup.
Only optional MP4 conversion can fail without failing an otherwise passing test;
`session.webm` is preserved. Each command checks ffmpeg once before target setup. If it
is missing or cannot start, one setup notice explains the WebM-only fallback;
recordings do not repeat conversion attempts or warnings. If an available ffmpeg
later fails to convert a recording, that recording still gets its normal warning. The self-check deliberately removes ffmpeg from its
configuration and decodes passing, failing and timeout recordings in Chromium.
It also checks nested sessions, duplicate labels/names, retries, skips, failures
before recording and failures after sessions close, including cleanup hooks.

Hard process kills, OS crashes and failures before browser launch cannot promise
complete artifacts. For interruption during startup, inspect each target's
`<run>/<target>/target.json` and its `coral.log` / `reef.log`. Target setup and
cleanup failures stay visible alongside successful scenarios from other targets. Do not use `test.concurrent` with the initial serial harness.

## Scope and next journeys

`shell/navigation` proves sidebar preference persistence and missing-page recovery
with genuine sign-in. Focused `scenarios/local-mode/` checks prove no-login entry and implicit
Owner behavior; neither replaces the product Workspace/Agent journeys.
`scenarios/local-mode/parity/` reruns shared journeys in the local topology:

- The Provider/Account journeys: protected discovery and shared promotion, Account
  Connection Request continuation, an Agent Account with its Agent bearer, and a
  failed shared update.
- `topology/absent-surfaces`: the MCP listener serves only
  `/workspaces/{workspace_id}/mcp` and `/mcp`, with the tool set `crates/coral-mcp` pins, and
  no public listener serves a private gRPC method. Audience rejection stays tested in `crates/coral-grpc`.
- `activity/binary-output`: a binary Final Value reaches an MCP client as an image
  block and shows unchanged on Reef's Run Status Page.

Maintained management recordings:

- `workspaces/lifecycle`: Workspace creation, the workspace MCP URL copied from the sidebar
  header, the settings dialog over the Overview (shareable URL, closing replaces the history entry),
  stable identity across rename, archive cancellation/confirmation, configuration
  lockout, restoration, sidebar Workspace creation, tree expansion without
  navigation, direct links, and browser Back/Forward. The sidebar tree is the only
  Workspace listing.
- `workspaces/first-workspace`: a User without a Workspace creates one from the
  home empty state, copies `/mcp` from the empty Activity, and the first MCP
  call replaces that empty state live.
- `workspaces/authority`: an Organization Owner denied without a Membership, Owner
  assignment from the organization Workspaces page, one-submit
  exact-email grants, designated Memberships, concealed unrelated Workspaces,
  restricted Member views and forged-action rejection, and leaving a Workspace.
- `workspaces/member-selection`: ambiguity with dialog/input retention and safe
  correction; committed replay after email reassignment; obsolete URL normalization.
- `workspaces/membership-dialog-lifecycle`: live archive/restoration and demotion
  while Add member is open, including navigation refresh after rejected mutations.
- `workspaces/membership-actions`: the last Owner cannot leave or change their role
  to Member; removing another person keeps the administrator in the Workspace.
- `shell/management-scroll`: wheel/keyboard scrolling in the settings dialog and the
  real shell, long Members and Agent pages, mobile drawer navigation/focus, settings
  from the drawer, and its nested archive dialog.
- `agents/lifecycle`: one-time issuance, credential rotation in the key dialog
  with a client setup that authenticates, no credential recovery on reload, Agent and Registration disablement, expiration,
  and destructive cleanup, checked against real MCP authentication.
- `providers/lifecycle-journey`: the same credential-free Provider journey on
  authenticated SQLite and Postgres. Add through Reef, discover and execute through
  MCP, distinguish independent Providers, change exact policy, rename and Update,
  disable/restore, then remove/recreate without inheriting identity or policy.
  Observe the retained original Run after removal.
- Other `providers/` and `accounts/` scenarios: stale mutations, failed
  Updates, unavailable Operations, exact lost-response recovery, secret-free
  responses, protected MCP discovery, OAuth callbacks/rejected codes, external-browser
  consent, application-secret rotation, member authority and exact Workspace
  concealment. Scoped Accounts replace the old inventories. Disposable upstream
  fixtures live in `support/provider-fixtures.ts`.

Keep later journeys with the work that owns them:

- **Organization administration:** Organization role/disablement screens and last-enabled-Owner protection.
- **Scoped Accounts:** each scoped Account feature owns its journeys. The integrated
  cutover scenarios compose their existing assertions and own joint Update/offboarding
  races, rather than adding alternative product flows.
- **Activity:** real MCP Run, required action in Reef, approve/decline, terminal outcome
  and upstream dispatch count.
- **Program authoring and evidence:** Program authoring, diagnostics/results, history and concealment.
- **Boot recovery:** the restart scenario, which interrupts started and unstarted Runs
  alike.
- **Desktop:** the `desktop` suite proves Desktop's first launch, Provider consent
  hand-off, crash restart, and quit and relaunch, on Linux. It also proves that Coral
  serves with no window, that a second launch opens the window, and that Quit stops it. The Desktop on Windows
  workflow runs it against the installed Windows app. macOS gets a manual
  click-through, and Windows 11 a manual visual pass.

## Evidence and CI

`runs/<timestamp-uuid>/<target>/<scenario-id>/` contains the runner-owned
`scenario.json`, source snapshot and scenario page. Each
`sessions/<session-uuid>/` beneath it contains its own `result.json`, source
snapshot, video, `trace.zip`, numbered screenshots and optional `failure.png`.
The session metadata references its parent scenario ID and records an explicit
sequence, attempt, actor, label and purpose. No grouping is inferred from names.
The reporter owns the command's `run.json`: commit, dirty-checkout flag, selected
target names, timing and film mode. Each target owns its own `target.json` with
setup/cleanup state and process IDs, plus its own process logs. Target setup never
rewrites shared run metadata or another target's logs.
Skipped tests that never open a session have scenario metadata but no browser
artifacts. This unreleased format does not reconstruct parent test outcomes from
old session-only recordings; discard those runs and record new evidence.

Everything under `runs/` is gitignored. **Treat all evidence as sensitive**, even
with synthetic data: traces include requests, cookies, tokens and source; videos
may show one-time credentials; logs and screenshots can expose private content.
The viewer binds only to loopback, rejects unrelated Host headers and serves only
files inside the results root. Remote viewing requires the explicit origin setting
above and a reviewed tailnet access policy. Do not publish evidence without review.
Local evidence is not automatically pruned; delete old run directories when no
longer needed.

The opt-in **Reel** GitHub Actions workflow builds the real stack, runs the
harness self-check and records the selected suite. Its default is `main` on
SQLite; select Postgres to run the same journeys with that database. It uploads artifacts even after failures,
with seven-day retention, **only while the repository is private**. Public-repo
uploads are deliberately disabled; the final automatic CI matrix is later work.
No recordings or traces are published to Pages or a public trace viewer.

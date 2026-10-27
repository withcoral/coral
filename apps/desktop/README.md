# Coral Desktop

Coral Desktop supervises two local processes and shows Reef in a window:

- the `coral` runtime, as a child process
- Reef's server, in an Electron utility process, listening on loopback HTTP

The window loads Reef's loopback URL. Desktop has no preload script and no
product logic of its own. [ADR 0012](../../docs/adr/0012-desktop-hosts-reef-as-a-loopback-server.md)
records why.

## Ports

Every listener binds `127.0.0.1`.

| Listener                | Port                                                                       |
| ----------------------- | -------------------------------------------------------------------------- |
| Runtime MCP             | Allocated on first launch and kept: agent clients store the MCP URL.       |
| Reef                    | Allocated on first launch and kept: Run Status Page links name Reef's URL. |
| Runtime gRPC            | Picked at each launch. Only Reef calls it.                                 |
| Provider OAuth callback | Fixed at 8779 by the runtime's Desktop defaults.                           |

When a kept port is in use, Desktop asks before it uses a new port. After a new
MCP port, reconfigure your agent clients. After a new Reef port, Run Status Page
links from earlier agent runs stop working.

## Background

Agent clients store Coral's MCP URL, so Coral must run when no window is open.
Desktop itself stays running for that; the runtime is not an OS service.
[ADR 0014](../../docs/adr/0014-desktop-serves-in-the-background.md) records why.

- Closing the last window keeps Coral running. This is not a setting. The
  first time, Desktop says so in a dialog and keeps `toldStillRunning` in
  `desktop.json`.
- On macOS, Coral is in the Dock and the app switcher only while a window is
  open. With no window, the menu bar item is the way to open or quit Coral.
  A Coral that could not make its menu bar item stays in the Dock.
- Start at login starts Desktop with no window. macOS and Windows register a
  login item. Linux writes `coral-app.desktop` in the XDG autostart folder,
  which names the AppImage; a package that is not an AppImage has no such
  setting, and nor has a development build.
- Start at login is a checkbox in the tray menu and in the Coral menu on macOS,
  the File menu elsewhere.
- On macOS, Quit in the Coral menu (Command+Q) only closes the windows, so it
  does not stop Coral. Quit in the Dock menu and Q in the app switcher
  (Command+Tab) do the same. Quit Coral in the menu bar item, or Quit
  Completely in the Coral menu or the Dock menu, stops Coral. So does a
  logout, a restart or a shutdown. On Windows and Linux, Quit stops Coral.
- The Dock menu on macOS also offers Restart Coral, and the update item in a
  package that can update itself.
- The tray item, the menu bar item on macOS, offers Open Coral, Start at login
  and Quit Coral. On Windows a click on it opens the window. Stock GNOME shows
  no tray item.
- Launching Coral again while it runs opens the window.

## Lifecycle

- Quit stops Reef, then closes the runtime's stdin. The runtime treats that as
  a stop request (`CORAL_STOP_ON_STDIN_CLOSE=1`) and finishes its work. Desktop
  kills a process that has not exited after 10 seconds. The OS closes the
  stdin too when Desktop itself dies, so the runtime never outlives Desktop.
- When the runtime exits unexpectedly, Desktop stops Reef, shows the failure,
  and starts both again. When Reef exits alone, Desktop restarts only Reef.
  A restart that does not become ready in time counts as another attempt.
  After three restarts within a minute, Desktop waits for the user to choose
  Restart.
- A runtime that refuses to start is not retried. The one exception is exit
  code 75: another runtime still holds the state directory, for example one
  that is still finishing its work, so Desktop tries again until it is ready
  or the start times out.
- Provider consent opens in the system browser. When Reef answers a form with a
  consent redirect marked `X-Coral-Authorization-Return-To` (ADR 0009), Desktop
  opens the redirect's target in the system browser and sends the window to the
  marked page on Reef instead. A device sign-in form targets a new tab, which
  Desktop runs as a hidden window. That window closes after the hand-off. If
  Reef answers it with a page, the window shows that page.

## Development

```shell
npm install --prefix apps/desktop
npm install --prefix apps/reef
npm run dev --prefix apps/desktop
```

`npm run dev` builds the runtime with Cargo, then starts Electron. Electron
starts that runtime and Reef's Vite dev server, with sign-in disabled.
Development runs on macOS and Linux. On Windows, package the app instead.
Development keeps its data in `<appData>/com.withcoral.app.dev`, apart from an
installed app.

The runtime completes provider OAuth on the fixed port 8779. A development
Desktop, an installed Desktop and a local `coral` runtime cannot run together:
the second one to start fails on that port.

## Packaging

```shell
npm run stage --prefix apps/desktop
npm run package --prefix apps/desktop
```

`npm run stage` puts two things in `resources/`, which the package carries
outside the asar:

- `coral/coral`: a release runtime, `coral.exe` on Windows. On macOS it is
  built for Apple silicon, so install the `aarch64-apple-darwin` Rust target.
  Pass `-- --runtime <path>` to stage a binary you built.
- `reef/`: Reef's bundled `server.mjs` and its client files, built in a
  scratch directory. It is the same Reef build that Docker ships. The bundle
  carries every dependency, so the package has no `node_modules`.

`npm run package` builds the main process and runs electron-builder for this
platform: an Apple silicon (arm64) dmg and zip on macOS, an AppImage on Linux,
and an x64 NSIS installer on Windows. Pass electron-builder options after
`--`, for example `-- --linux --dir`. These packages are unsigned, name no
update feed and cannot update themselves.

The Windows installer installs for the current user, with no UAC prompt, into
`%LOCALAPPDATA%\Programs\Coral`. The 0.x app installs into the same folder,
so uninstall it first. On macOS both apps are `/Applications/Coral.app`, so
this app replaces the 0.x one there. On Linux every name differs
(`coral-app` against `coral-desktop`), so the two apps install side by side.
On every platform the two apps keep separate data folders. The Start Menu
entry is "Coral". Uninstalling keeps the user's data. Run the installer with
`/S` to install silently.

Desktop's icons use Reef's `public/favicon.svg` as their source. Run
`npm run icons --prefix apps/desktop` to regenerate the PNG, ICO and ICNS files,
and the macOS menu bar's template images (`trayTemplate.png`).
Desktop enlarges the mark by 35% within the same tile. The macOS icons add
transparent padding around the flat artwork.

The dmg window's background is `resources/dmg/background.svg`, light artwork
with the coral symbol from withcoral.com. After you change it, run
`npx electron scripts/render-dmg-background.mjs` in `apps/desktop` to write
`background.png` and `background@2x.png`. Keep `dmg.contents` in
`electron-builder.config.ts` on the arrow's ends.

The Reel `desktop` suite drives the packaged Linux app. The Desktop on Windows
workflow also drives the installed Windows app. See
[Reel's run guide](../reef/e2e/README.md).

## Releases

The Release workflow (`.github/workflows/release.yml`) builds signed release
packages and publishes them to the v1 update feed, GitHub releases in
`withcoral/coral-releases`. It never publishes to `withcoral/coral`, whose
releases the 0.x app polls: that app has another app id and data folder, and
would update itself into this one.

An installed app accepts only an update signed by the publisher in its
`app-update.yml`. Changing the Windows signing certificate's subject
(`AZURE_TRUSTED_SIGNING_PUBLISHER_NAME`) leaves installed apps on their version.

Each platform job stages and builds with no credentials. Then
`xtask release-desktop-package` packages with `CORAL_DESKTOP_RELEASE=1`:

- macOS: electron-builder signs every binary with the hardened runtime and
  JIT, then notarizes the app. The runtime needs JIT as much as Electron does:
  it runs Programs in V8, which aborts at startup without it. xtask verifies
  the signatures and the staple, and checks JIT on the app and the runtime.
- Windows: electron-builder signs the app, the runtime and the installer with
  Azure Trusted Signing. xtask verifies their Authenticode signatures.
- Linux: the AppImage is unsigned.

A release build fails when a credential is missing. xtask also checks that each
feed matches its packages and that the packed `app-update.yml` names the v1
feed. The workflow uploads every asset to a draft release and publishes it last.
The runtime and coral-web images ship in the same release. The workflow builds,
tests and signs them by digest, and the last job tags them just before it
publishes the draft, so no artifact is visible until every artifact passed.

The tag, `Cargo.toml` and `package.json` must name one version.
release-please moves them together. To cut a beta, merge a commit with the
footer `Release-As: 1.0.0-beta.1`, then merge the release PR. Only builds that
are themselves prereleases see prereleases, so you can test the update path
before the swap.

electron-builder signs and notarizes the app, and staples its ticket. The dmg
that carries it is not signed; Gatekeeper checks the app inside.

The workflow reads these from the `release` environment:

| Name                                                                                                                                                             | Kind     |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| `APPLE_CODESIGN_IDENTITY`, `APPLE_DEVELOPER_ID_CERTIFICATE_BASE64`, `APPLE_DEVELOPER_ID_CERTIFICATE_PASSWORD`                                                    | Secret   |
| `APP_STORE_CONNECT_API_KEY_ID`, `APP_STORE_CONNECT_API_ISSUER_ID`, `APP_STORE_CONNECT_API_KEY_P8_BASE64`                                                         | Secret   |
| `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`                                                                                                      | Secret   |
| `AZURE_TRUSTED_SIGNING_ENDPOINT`, `AZURE_TRUSTED_SIGNING_ACCOUNT_NAME`, `AZURE_TRUSTED_SIGNING_CERTIFICATE_PROFILE_NAME`, `AZURE_TRUSTED_SIGNING_PUBLISHER_NAME` | Variable |
| `RELEASE_APP_PRIVATE_KEY`                                                                                                                                        | Secret   |
| `RELEASE_APP_ID`                                                                                                                                                 | Variable |

The release GitHub App needs contents write access to `withcoral/coral-releases`.

## Updates

A release package checks the feed at launch and every four hours, and
downloads a new version in the background. When the download is ready, a
notification says so, and the update menu item becomes Restart to Update. The
item is in the Coral menu on macOS and in the Help menu elsewhere. While the
update waits, the Dock icon shows a badge on macOS while Coral is in the Dock,
as do Linux launchers that support badges. The tray menu starts with Restart to Update, the tray tooltip
names the version, and the macOS menu bar item shows a dot.

- Choose Restart to Update, or select the notification, to install now.
  Desktop stops the runtime and Reef, hands the update to the platform
  installer, and Coral starts again. If the hand-off fails, Coral starts
  again on the old version.
- Otherwise the update installs the next time you quit Coral, after Desktop
  stops the runtime and Reef. Desktop never restarts Coral by itself, so an
  update can wait a long time while Coral runs in the background.

## Data

Desktop keeps everything under `<appData>/com.withcoral.app`, which is
`%APPDATA%\com.withcoral.app` on Windows:

- `runtime/`: the runtime's state. Desktop rewrites `runtime/config.toml` on
  every launch.
- `desktop.json`: the kept MCP and Reef ports, the background notice, and the
  database, if you set one. See [Ports](#ports), [Background](#background) and
  [Database](#database).
- `logs/`: `coral.log`, `reef.log` and `desktop.log`, Desktop's own record of
  starts, failures and restarts. Help > Show logs opens this folder. A log
  that reaches 5 MB moves to `<name>.1.log`, which replaces the previous one.

On Windows, the runtime's owner-only file permissions have no effect. The data
folder relies on the access control list that it inherits from the user's
profile.

### Database

The runtime keeps its state in `runtime/coral.db`, a SQLite file. To use a
Postgres database instead, quit Coral and add a `database` section to
`desktop.json`. Keep the sections that are already there:

```json
{
  "ports": { "mcp": 58100, "reef": 58101 },
  "database": { "backend": "postgres", "url": "postgres://user:password@127.0.0.1:5432/coral" }
}
```

Desktop then writes `[database]` in `runtime/config.toml` and gives the runtime
the URL in `CORAL_DESKTOP_DATABASE_URL`, so the file never holds the password.
Desktop never writes this section. An invalid section stops Coral from
starting; it never falls back to SQLite. The runtime does not copy state
between the two databases, and `runtime/credentials.key` must stay with the
state that it sealed.

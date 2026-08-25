# Coral Desktop

Coral Desktop is an Electron shell around Coral UI, Coral's main user-facing app.

The main process bundles the Coral CLI and runs it as a supervised local
sidecar:

```shell
coral server
```

The sidecar owns the local gRPC/API runtime. In packaged builds the main
process serves Coral UI through React Router's server build over a custom
`coral-app://` scheme (no local network socket), serves static client assets from
the same scheme, points the window at it, and handles the native shell concerns:
app lifecycle, window and menu, and MCP client configuration.

The renderer never receives a Coral endpoint. Browser interactions reach Coral
through React Router loaders, actions, or resource routes, and the server build
uses `CORAL_ENDPOINT` to reach the supervised sidecar.

## Scope

- Electron shell running Coral UI in-window with a supervised Coral CLI sidecar
  (single-instance lock, graceful startup and shutdown)
- bundled Coral CLI sidecar staged into the app bundle
- Coral UI documents rendered in-process from the staged React Router server build,
  with static assets served over the custom `coral-app://` scheme and a strict
  Content-Security-Policy
- light and dark app icons on macOS, Linux, and Windows
- Coral MCP configuration from Desktop Settings for supported stdio-capable
  clients, including before they are installed
- macOS DMG and ZIP packaging via `electron-builder`, with a signed and
  notarized release mode
- Linux AppImage and deb packaging (x64), unsigned
- Windows NSIS installer packaging (x64), unsigned preview
- GitHub Releases update metadata for packaged desktop auto-updates on macOS and
  on the Linux AppImage
- macOS system theme support

## Development

```shell
npm install --prefix apps/desktop
npm install --prefix apps/coral-ui
npm run dev --prefix apps/desktop
```

The desktop dev command starts the Coral UI dev server first, then launches Electron
with `ELECTRON_RENDERER_URL` pointed at that server, so Coral UI's dependencies must
be installed too. The dev server receives `CORAL_DESKTOP_APP=1` as the single
desktop build marker and a fixed `CORAL_ENDPOINT` for server-side loaders. Coral UI
uses the marker directly for server-side route composition, while Vite compiles
only its boolean value into browser code. Electron waits for the sidecar before
loading the first document so initial SSR does not race the local Coral process.

In development, the sidecar is started through Cargo so the app uses the local
Rust code (run from the repo root):

```shell
cargo run --locked -p coral-cli -- server
```

## Build

```shell
npm run build --prefix apps/desktop
```

The build script compiles the release Coral binary, builds Coral UI in React Router
framework mode, stages Coral UI's server bundle under `apps/desktop/out/coral-ui-server/`,
and stages the Coral binary under `apps/desktop/resources/coral/` for Electron
packaging. The packaged app serves document, data, action, and route-discovery
requests through the staged server bundle; static assets are copied from
`apps/coral-ui/build/client/` into the app resources.

```shell
npm run package:dir --prefix apps/desktop
```

Use `npm run package:dmg --prefix apps/desktop` for the macOS drag-and-drop DMG.
Use `npm run package:mac --prefix apps/desktop` to build the release-shaped
universal macOS DMG and ZIP with updater metadata.

Use `npm run package:linux --prefix apps/desktop` for the x64 Linux AppImage and
deb. The deb needs `ar` (binutils) and `xz` on PATH — `fpm` shells out to both.
Linux packages are unsigned. The build writes `latest-linux.yml` for the AppImage,
which replaces its own image file on update; the deb belongs to dpkg, so
`deb.publish` is null (which keeps it out of that feed),
`desktopUpdatesSupported()` (`src/main/auto-update.ts`) returns false there, and
deb users install a new release themselves.

Use `npm run package:win --prefix apps/desktop` for the x64 Windows NSIS
installer. It must run on Windows: NSIS builds its uninstaller by executing the
freshly built installer, which off-Windows means wine. The installer is an
unsigned preview, so SmartScreen shows an "unrecognized app" warning; ship it
labelled as such. It installs per user under `%LOCALAPPDATA%` by default, needs no
UAC prompt, and lets the user pick a directory. The install mode page also offers
an all-users install, but `allowElevation: false` disables that choice unless the
installer already runs elevated. Windows ships no updater, so the
build writes no feed and no blockmap.

`CORAL_DESKTOP_RELEASE=1` selects release mode: it bakes the updater into the main
process, so only builds made with it check for updates. On macOS it also requires a
complete App Store Connect API key credential set, forces Developer ID signing,
enables the hardened runtime with minimal Electron entitlements, and notarizes the
app — Squirrel.Mac refuses to update an unsigned app. The AppImage needs no
signature, so a Linux release build takes no credentials. Windows rejects the flag.
Without it, packaging is deterministically unsigned and the updater is inert.

> Run the `--prefix apps/desktop` commands from the repo root. If you are already in
> `apps/desktop/`, drop the flag (e.g. `npm run package:dir`).

### Verifying a Linux update by hand

The AppImage update path only exists in a release build, so a cut release is the
only way to exercise it. Two steps of it have no test:

- **The relaunch.** `app.relaunch()` forks Electron's relauncher helper from the
  mounted AppDir, and that helper waits for this process to exit before it starts
  the new image — the same exit that makes the AppImage runtime unmount the AppDir
  the helper is running from. Check that the app actually comes back.
- **The destination.** Run the update once from an image named
  `coral-desktop-linux-x64.AppImage` (replaced in place) and once from a renamed
  `coral-desktop-<version>.AppImage` (new image written beside it, old one
  unlinked, `appimage-filename-updated` carries the destination). The app must come
  back in both.

# Coral Desktop

Coral Desktop is an Electron shell around Reef, Coral's main user-facing app.

The main process bundles the Coral CLI and runs it as a supervised local
sidecar:

```shell
coral ui --no-open --port 0
```

The sidecar owns the local gRPC-Web/API runtime. In packaged builds the main
process serves Reef through React Router's server build over a custom
`coral-app://` scheme (no local network socket), serves static client assets from
the same scheme, points the window at it, and handles the native shell concerns:
app lifecycle, window and menu, and MCP client configuration.

The renderer never receives a Coral endpoint. Browser interactions reach Coral
through React Router loaders, actions, or resource routes, and the server build
uses `CORAL_ENDPOINT` to reach the supervised sidecar.

## Scope

- Electron shell running Reef in-window with a supervised Coral CLI sidecar
  (single-instance lock, graceful startup and shutdown)
- bundled Coral CLI sidecar staged into the app bundle
- Reef documents rendered in-process from the staged React Router server build,
  with static assets served over the custom `coral-app://` scheme and a strict
  Content-Security-Policy
- light and dark app icons (macOS; Windows icon assets are staged for a future
  Windows target)
- Coral MCP configuration from Desktop Settings for supported stdio-capable
  clients, including before they are installed
- macOS DMG and ZIP packaging via `electron-builder`, with a signed and
  notarized release mode
- GitHub Releases update metadata for packaged desktop auto-updates
- macOS system theme support

## Development

```shell
npm install --prefix apps/desktop
npm install --prefix apps/reef
npm run dev --prefix apps/desktop
```

The desktop dev command starts the Reef dev server first, then launches Electron
with `ELECTRON_RENDERER_URL` pointed at that server, so Reef's dependencies must
be installed too. The dev server receives `CORAL_DESKTOP_APP=1` as the single
desktop build marker and a fixed `CORAL_ENDPOINT` for server-side loaders. Reef
uses the marker directly for server-side route composition, while Vite compiles
only its boolean value into browser code. Electron waits for the sidecar before
loading the first document so initial SSR does not race the local Coral process.

In development, the sidecar is started through Cargo so the app uses the local
Rust code (run from the repo root):

```shell
cargo run --locked -p coral-cli -- ui --no-open --port 0
```

## Build

```shell
npm run build --prefix apps/desktop
```

The build script compiles the release Coral binary, builds Reef in React Router
framework mode, stages Reef's server bundle under `apps/desktop/out/reef-server/`,
and stages the Coral binary under `apps/desktop/resources/coral/` for Electron
packaging. The packaged app serves document, data, action, and route-discovery
requests through the staged server bundle; static assets are copied from
`apps/reef/build/client/` into the app resources.

```shell
npm run package:dir --prefix apps/desktop
```

Use `npm run package:dmg --prefix apps/desktop` for the macOS drag-and-drop DMG.
Use `npm run package:mac --prefix apps/desktop` to build the release-shaped
universal macOS DMG and ZIP with updater metadata.

`CORAL_DESKTOP_RELEASE=1` selects release mode. It requires a complete App Store
Connect API key credential set, forces Developer ID signing, enables the hardened
runtime with minimal Electron entitlements, and notarizes the app. Without that
flag, packaging is deterministically unsigned.

> Run the `--prefix apps/desktop` commands from the repo root. If you are already in
> `apps/desktop/`, drop the flag (e.g. `npm run package:dir`).

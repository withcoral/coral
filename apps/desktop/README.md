# Coral Desktop

Coral Desktop is an Electron shell around Reef, Coral's main user-facing app.

The main process bundles the Coral CLI and runs it as a supervised local
sidecar:

```shell
coral ui --no-open --port 0
```

The sidecar owns the local gRPC-Web/API runtime. The main process serves the
built Reef client in-process over a custom `coral-app://` scheme (no local
network socket), points the window at it, and handles the native shell concerns:
app lifecycle, window and menu, and MCP client configuration.

## Scope

- Electron shell running Reef in-window with a supervised Coral CLI sidecar
  (single-instance lock, graceful startup and shutdown)
- bundled Coral CLI sidecar staged into the app bundle
- Reef client served in-process over a custom `coral-app://` scheme (no local
  network socket), with a strict Content-Security-Policy
- light and dark app icons (macOS; Windows icon assets are staged for a future
  Windows target)
- Coral MCP configuration for Codex and Claude Code — from the Settings page and
  the app menu — via `add-mcp`
- signed, notarized macOS DMG and ZIP packaging via `electron-builder`
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
be installed too.

In development, the sidecar is started through Cargo so the app uses the local
Rust code (run from the repo root):

```shell
cargo run --locked -p coral-cli -- ui --no-open --port 0
```

## Build

```shell
npm run build --prefix apps/desktop
```

The build script first builds the legacy `apps/ui/` bundle needed by the current
`coral ui` sidecar mode, then builds Reef in desktop SPA mode, compiles the
release Coral binary, and stages it under `apps/desktop/resources/coral/` for
Electron packaging.

```shell
npm run package:dir --prefix apps/desktop
```

Use `npm run package:dmg --prefix apps/desktop` for the macOS drag-and-drop DMG.
Use `npm run package:mac --prefix apps/desktop` to build the release-shaped
universal macOS DMG and ZIP with updater metadata.

Release builds set `CORAL_DESKTOP_RELEASE=1`, which requires Apple signing and
notarization credentials and fails if the app cannot be signed.

> Run the `--prefix apps/desktop` commands from the repo root. If you are already in
> `apps/desktop/`, drop the flag (e.g. `npm run package:dir`).

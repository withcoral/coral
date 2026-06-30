# Coral Desktop

Coral Desktop is the first production-ready Electron app foundation for
shipping Coral to customers.

The app bundles the Coral CLI and starts it as a local sidecar with:

```shell
coral ui --no-open --port 0
```

The Electron main process owns the native shell, app lifecycle, local app asset
serving, and MCP client configuration. The bundled sidecar owns the local
gRPC-Web/API runtime used by the app.

This iteration only supports the bundled sidecar runtime. A future iteration
will let the desktop app connect to a remote Coral CLI server instance instead
of always starting and managing a local sidecar.

## Current Scope

- macOS DMG packaging through `electron-builder`
- bundled Coral CLI sidecar staging
- local app renderer served from an ephemeral loopback HTTP port
- light and dark app icons, including macOS and Windows icon containers
- Settings page for configuring Coral MCP in Codex and Claude Code
- MCP client config updates delegated to `add-mcp`
- macOS system theme support for the app UI

## Later Work

- remote Coral CLI server connection mode
- CLI alias installation from the desktop app
- MCP connection testing and guided troubleshooting
- Claude Desktop configuration support
- onboarding for first-run source setup
- auto-update
- Developer ID signing and notarization
- Windows and Linux installer targets

## Development

```shell
npm install --prefix desktop
npm run dev --prefix desktop
```

The desktop dev command starts the React Router app dev server first, then
launches Electron with `ELECTRON_RENDERER_URL` pointed at that server.

In development, the sidecar is started through Cargo so the app uses the local
Rust code:

```shell
cargo run --manifest-path ../Cargo.toml --locked -p coral-cli -- ui --no-open --port 0
```

## Build

```shell
npm run build --prefix desktop
```

The build script first builds the legacy `ui/` bundle needed by the current
`coral ui` sidecar mode, then builds the React Router app in desktop SPA mode,
compiles the release Coral binary, and stages it under
`desktop/resources/coral/` for Electron packaging.

```shell
npm run package:dir --prefix desktop
```

Use `npm run package:dmg --prefix desktop` for the macOS drag-and-drop DMG.
Windows and Linux installer targets are intentionally not configured yet.

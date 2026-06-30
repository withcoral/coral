# Coral Desktop

Electron desktop prototype for Coral.

The app starts a bundled Coral CLI binary as a local sidecar with:

```shell
coral ui --no-open --port 0
```

Electron displays the top-level `reef/` React Router app. In production, the
main process serves the built Reef client from an ephemeral loopback HTTP port
so React Router can run with a normal browser origin. The Coral sidecar remains
responsible for the local gRPC-Web/API runtime.

The app also exposes desktop actions for installing a `coral` command and
configuring common MCP clients. The command installer follows the VS Code
pattern: it creates a PATH-visible symlink to an entrypoint inside the `.app`
bundle, `Contents/Resources/bin/coral`, which then execs the bundled CLI
binary. It does not edit shell startup files. MCP config updates are delegated
to `add-mcp` so client-specific file formats stay out of the Electron shell.

## Development

```shell
npm install --prefix desktop
npm run dev --prefix desktop
```

The desktop dev command starts the Reef React Router dev server first, then
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
`coral ui` sidecar mode, then builds Reef in desktop SPA mode, compiles the
release Coral binary, and stages it under `desktop/resources/coral/` for
Electron packaging.

```shell
npm run package:dir --prefix desktop
```

Use `npm run package:dmg --prefix desktop` for the macOS drag-and-drop DMG.
Windows and Linux installer targets are intentionally not configured yet.

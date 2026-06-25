# Coral Desktop

Electron desktop prototype for Coral.

The app starts a bundled Coral CLI binary as a local sidecar with:

```shell
coral ui --no-open --port 0
```

The renderer displays the sidecar-served Coral UI and exposes desktop actions
for installing a `coral` command and configuring common MCP clients. The command
installer follows the VS Code pattern: it creates a PATH-visible symlink to an
entrypoint inside the `.app` bundle, `Contents/Resources/bin/coral`, which then
execs the bundled CLI binary. It does not edit shell startup files. MCP config
updates are delegated to `add-mcp` so client-specific file formats stay out of
the Electron shell.

## Development

```shell
npm install --prefix desktop
npm run dev --prefix desktop
```

In development, the sidecar is started through Cargo so the app uses the local
Rust code:

```shell
cargo run --manifest-path ../Cargo.toml --locked -p coral-cli -- ui --no-open --port 0
```

## Build

```shell
npm run build --prefix desktop
```

The build script first runs the UI build, then compiles the release Coral
binary, and stages it under `desktop/resources/coral/` for Electron packaging.

```shell
npm run package:dir --prefix desktop
```

Use `npm run package:dmg --prefix desktop` for the macOS drag-and-drop DMG.
Windows and Linux installer targets are intentionally not configured yet.

# Coral Desktop

Electron desktop prototype for Coral.

The app starts a bundled Coral CLI binary as a local sidecar with:

```shell
coral ui --no-open --port 0
```

The renderer displays the sidecar-served Coral UI and exposes desktop actions
for installing a `coral` command and writing MCP stdio config files for common
clients.

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

Use `npm run package --prefix desktop` for installer artifacts.

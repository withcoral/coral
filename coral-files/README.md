# coral-files

`coral-files` indexes a local directory and exposes discovered files, extracted
content, normalized structure, chunks, and search over a localhost REST API.

This is intentionally standalone and is not wired into the main Coral workspace.

## Run

```sh
cargo run --manifest-path coral-files/Cargo.toml -- serve --root .
```

Equivalent default:

```sh
cargo run --manifest-path coral-files/Cargo.toml
```

The server binds to `127.0.0.1:8765` by default and writes its index to
`.coral-files/index.sqlite` under the indexed root.

## Coral source

The standalone Coral source spec lives at `coral-files.yaml`.

```sh
coral source lint coral-files/coral-files.yaml
coral source add --file coral-files/coral-files.yaml
coral source test coral_files
```

If the server is running on a non-default port, set the source input when adding
the source:

```sh
CORAL_FILES_BASE_URL=http://127.0.0.1:18766 \
  coral source add --file coral-files/coral-files.yaml
```

## API

- `GET /health`
- `GET /v1/files`
- `GET /v1/files/{id}`
- `GET /v1/files/{id}/content`
- `GET /v1/files/{id}/structure`
- `GET /v1/files/{id}/elements`
- `GET /v1/files/{id}/tables`
- `GET /v1/files/{id}/metadata`
- `GET /v1/files/{id}/chunks`
- `GET /v1/files/{id}/nodes`
- `GET /v1/files/{id}/headings`
- `GET /v1/files/{id}/sections`
- `GET /v1/chunks`
- `GET /v1/nodes`
- `GET /v1/headings`
- `GET /v1/sections`
- `GET /v1/search?q=...`

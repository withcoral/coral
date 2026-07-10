# OpenAPI tools

`openapi` is a small, standalone utility for producing a self-contained JSON
form of an OpenAPI descriptor. It follows the reachable `$ref` graph, loads
external YAML or JSON documents, and dereferences the result.

It is intentionally independent of Coral's runtime and source-spec crates.

## Usage

Run it from the repository root with a local descriptor:

```bash
cargo run --locked -p openapi -- hydrate path/to/openapi.yaml > openapi.json
```

The location may be a local path, a `file:` URI, or an HTTPS URL:

```bash
cargo run --locked -p openapi -- hydrate https://example.com/openapi.yaml > openapi.json
```

Successful output is pretty-printed JSON on standard output. Errors are
written to standard error and cause a non-zero exit status, so the command is
safe to use in scripts.

## What `hydrate` does

- Parses YAML or JSON descriptors.
- Loads reachable external `$ref` targets and follows nested references.
- Resolves JSON Pointer fragments, including percent-encoded pointer tokens.
- Dereferences the resulting document and prints it as JSON.
- Ignores unreferenced entries under the root document's `components`, so an
  unused broken external reference does not make hydration fail.

Only references reachable from the document are loaded. A missing document or
pointer target on a reachable path is an error.

## Fetch and file safety

`openapi` is deliberately conservative when resolving references:

- Network documents and network references must use HTTPS. HTTP and other URI
  schemes are rejected, and redirects from HTTPS to another scheme are refused.
- An HTTPS root document cannot load `file:` references.
- A local root document can load local files only from its own directory tree;
  references that escape that tree are rejected.
- Symlinked local descriptors and references are rejected.
- Root and external documents are each limited to 16 MiB. HTTPS requests time
  out after 30 seconds, and up to 32 external documents are fetched at once.

## Library API

The crate also exposes the hydration logic for Rust callers:

```rust
use openapi::hydrate_openapi_from_location;

let descriptor = hydrate_openapi_from_location("path/to/openapi.yaml")?;
println!("{}", serde_json::to_string_pretty(&descriptor)?);
# Ok::<(), Box<dyn std::error::Error>>(())
```

For descriptors already in memory, use `hydrate_openapi(bytes, base_uri)`. The
base URI must be an absolute `file:` or HTTPS URI so relative references can
be resolved.

## Development

Run the package tests with:

```bash
cargo test --locked -p openapi
```

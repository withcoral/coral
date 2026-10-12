# Changelog

## [1.0.0](https://github.com/withcoral/coral/compare/v1.0.0-beta.11...v1.0.0) (2026-10-12)


### ⚠ BREAKING CHANGES

* release Coral v1 ([#2321](https://github.com/withcoral/coral/issues/2321))

### Features

* release Coral v1 ([#2321](https://github.com/withcoral/coral/issues/2321)) ([24daeff](https://github.com/withcoral/coral/commit/24daeff0eebdf304ed28ed569be2b3c107c203a2))

## 1.0.0-beta.11 (2026-10-09)

### Features

* **recipes:** expand provider catalog with official sources
* **reef:** drive the operations table from the keyboard and filter it by policy
* **reef:** group catalog recipes by service with MCP and API buttons
* **reef:** name account outcomes by when you connect, and keep step actions in view
* **reef:** polish the providers page UI/UX
* **search:** score name and title matches apart from the body on SQLite

### Bug Fixes

* **auth:** scope the web client's consent skip to its login callback
* **catalog:** accept blank OpenAPI response descriptions
* **provider:** prefer Dynamic Client Registration when the metadata document resolves to non-public addresses
* **reef:** bind hosted session and CSRF cookies to Reef's host
* **reef:** name the outbound settings when Coral rejects a provider address
* **release:** tag images only when the whole release ships
* **task:** total a Task's context impact from its measured children

### Performance Improvements

* **catalog:** declare the HTTP result wrapper once per declaration
* **search:** abridge describe results to their required parts and a 2 KB budget

## 1.0.0-beta.10 (2026-10-06)

### Features

* **search:** improve phrase and Postgres word ranking, and make the Provider filter safe

### Bug Fixes

* **release:** read Windows signatures in the release job

## 1.0.0-beta.9 (2026-10-06)

### Features

* read the credential encryption key from a configured environment variable
* **reef:** confirm successful changes with a toast, not a banner
* **reef:** hide people management and describe accounts for you and your agents in a local deployment
* **workspace:** take Workspace authority only from a Workspace Membership

### Bug Fixes

* **desktop:** keep Coral running until Quit Completely, and out of the Dock with no window
* **reef:** bound Activity stream lifetime so proxy-stranded streams end
* **reef:** load the activity records a reader scrolls to before the first live snapshot
* **reef:** stop the GitHub MCP recipe implying OAuth scopes
* **upstream:** keep HTTP Operation path arguments within the declared path

## 1.0.0-beta.8 (2026-10-06)

### Features

* **mcp:** tell agents to save multi-Operation Programs as Saved Functions
* **reef:** add several workspace members at once from an organization picker
* **reef:** always show the overview's MCP setup

### Bug Fixes

* **reef:** stop the GitHub recipe implying OAuth scopes

## 1.0.0-beta.7 (2026-10-05)

### ⚠ BREAKING CHANGES

* **docker:** CORAL_SEED_CONFIG is removed. Set CORAL_CONFIG instead; it replaces config.toml on every start rather than only the first.

### Features

* **docker:** launch the SQL-free runtime and Reef in Docker
* **docker:** replace CORAL_SEED_CONFIG with CORAL_CONFIG
* **reef:** show the overview's MCP setup until your first call, with Claude Code sign-in
* **reef:** split an Agent's page, connect its MCP client, and hand off keys in one dialog

### Bug Fixes

* **reef:** restore the green alpha scales so light success text is readable
* **reef:** set light GreenAlpha step 6 to Radix's 32.16% opacity
* **v8:** type the coral host root during Saved Function authoring

## 1.0.0-beta.6 (2026-10-04)

### Features

* **desktop:** badge the Dock icon while an update waits to install

## 1.0.0-beta.5 (2026-10-04)

### Bug Fixes

* **desktop:** name Coral as the app's author and copyright holder

## 1.0.0-beta.4 (2026-10-04)

### Features

* **desktop:** download updates in the background, install on restart or quit

## 1.0.0-beta.3 (2026-10-04)

### Bug Fixes

* **desktop:** runtime JIT on macOS, and no show() on a closing window

## 1.0.0-beta.2 (2026-10-04)

### Bug Fixes

* **release:** validate the tag without fetching

## 1.0.0-beta.1 (2026-10-04)

### Features

* **desktop:** release signed, auto-updating builds

### Bug Fixes

* **release:** run release-please, and keep releases and downloads apart

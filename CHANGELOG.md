# Changelog

## [1.1.0](https://github.com/withcoral/coral/compare/v1.0.0...v1.1.0) (2026-10-15)


### Features

* **policy:** enable safe HTTP methods by default ([#2439](https://github.com/withcoral/coral/issues/2439)) ([4f9ea9c](https://github.com/withcoral/coral/commit/4f9ea9c897dcb5db20b9031146cda8aff36810f7))
* **reef:** archive and restore workspaces from the sidebar ([#2403](https://github.com/withcoral/coral/issues/2403)) ([5e02944](https://github.com/withcoral/coral/commit/5e02944a6f4ea373cca918ece1a71abb47f58013))
* **reef:** connect your account from a provider's settings ([#2364](https://github.com/withcoral/coral/issues/2364)) ([3d9ca44](https://github.com/withcoral/coral/commit/3d9ca44879bd8f385b0d8fbf9f984fc148457bb8))
* **reef:** delete a saved function from its drawer ([#2367](https://github.com/withcoral/coral/issues/2367)) ([c8ebee5](https://github.com/withcoral/coral/commit/c8ebee51ef5bbd4afd8bd65d747b6e6f91062481))
* **reef:** drive Activity records and their detail from the keyboard ([#2376](https://github.com/withcoral/coral/issues/2376)) ([e6d9d7f](https://github.com/withcoral/coral/commit/e6d9d7f95594a57ccd98edeea99c3cb2ec484d48))
* **reef:** filter the Agents and Members tables with one shared filter menu ([#2331](https://github.com/withcoral/coral/issues/2331)) ([db09bbc](https://github.com/withcoral/coral/commit/db09bbc4d1015e3afa61e93d37c23af3f65cbe10))
* **reef:** filter the organization workspaces table and assign several owners at once ([#2405](https://github.com/withcoral/coral/issues/2405)) ([9738fd7](https://github.com/withcoral/coral/commit/9738fd7e6046077dd5138faf2871a212cb24594c))
* **reef:** keep the Activity detail open while you pick another record ([#2375](https://github.com/withcoral/coral/issues/2375)) ([4979c38](https://github.com/withcoral/coral/commit/4979c386569abec830a24ba25c689cb5b8ab719f))
* **reef:** save provider settings changes in place ([#2360](https://github.com/withcoral/coral/issues/2360)) ([b084c9d](https://github.com/withcoral/coral/commit/b084c9d67608ce55231e5a553eaec9fcb6112186))
* **reef:** show your account as a settings page with clickable account rows ([#2330](https://github.com/withcoral/coral/issues/2330)) ([4a9e1cb](https://github.com/withcoral/coral/commit/4a9e1cbe6647a1332872ca278ee94baacf72b533))
* **reef:** start an expired provider setup again in one click ([#2368](https://github.com/withcoral/coral/issues/2368)) ([5bc48c5](https://github.com/withcoral/coral/commit/5bc48c5bd71b007797bbc3cfecfb8c0d2a3570e1))


### Bug Fixes

* **catalog:** ignore nullable outside OpenAPI 3.0 ([#2442](https://github.com/withcoral/coral/issues/2442)) ([e939b80](https://github.com/withcoral/coral/commit/e939b8035867091ed4aec804634820814449a255))
* **catalog:** project OpenAPI status ranges as named types ([#2425](https://github.com/withcoral/coral/issues/2425)) ([13dbb0a](https://github.com/withcoral/coral/commit/13dbb0a4762ae83725e988f9ddfb165f35027307))
* **reef:** list people who share a name or email as separate choices ([#2437](https://github.com/withcoral/coral/issues/2437)) ([6f219a0](https://github.com/withcoral/coral/commit/6f219a0c63f849c986237026763d59a05b221c24))
* **reef:** stop narrating what Coral does in interface copy ([#2332](https://github.com/withcoral/coral/issues/2332)) ([e74a2e0](https://github.com/withcoral/coral/commit/e74a2e0b8a127b506852e919f1e17ff61064fe41))
* **run:** make wait without a cursor wait for new events ([#2410](https://github.com/withcoral/coral/issues/2410)) ([1832e93](https://github.com/withcoral/coral/commit/1832e937bd94aa3cb43f9755fc72e78df5ad74cf))
* **v8:** declare console for Program type checking ([#2371](https://github.com/withcoral/coral/issues/2371)) ([dddcb7c](https://github.com/withcoral/coral/commit/dddcb7c45808880465812f5573fe701953f249b2))


### Performance Improvements

* **mcp:** trim tool descriptions to cut agent context cost ([#2370](https://github.com/withcoral/coral/issues/2370)) ([09790e1](https://github.com/withcoral/coral/commit/09790e155361244c4dedd704d7fb28be399f2da9))
* **search:** drop match evidence and default values from search hits ([#2408](https://github.com/withcoral/coral/issues/2408)) ([fb56642](https://github.com/withcoral/coral/commit/fb566423cac625ebd77406f488d66e81822161b8))

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

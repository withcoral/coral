# Changelog

## [1.2.0](https://github.com/withcoral/coral/compare/v1.1.0...v1.2.0) (2026-10-16)


### Features

* **catalog:** share named TypeScript types across a Provider ([#2482](https://github.com/withcoral/coral/issues/2482)) ([256e37d](https://github.com/withcoral/coral/commit/256e37d892d6aef0a4a8518984e5f1748ba1fbad))
* **desktop:** run the runtime on a Postgres database named in desktop.json ([#2485](https://github.com/withcoral/coral/issues/2485)) ([96465a0](https://github.com/withcoral/coral/commit/96465a0864a204d61819edcbbf58fa0373f9837f))
* **reef:** calmer providers table and a flush app shell ([#2462](https://github.com/withcoral/coral/issues/2462)) ([ffe279c](https://github.com/withcoral/coral/commit/ffe279c1be9070ed12b85102cb748c05a7d3e87a))
* **reef:** show search indexing failures on a Provider and its Operations ([#2504](https://github.com/withcoral/coral/issues/2504)) ([6cffed0](https://github.com/withcoral/coral/commit/6cffed0da46a057c70e9eea13cba733c7f5aa6ef))
* **search:** trace each reconcile pass and its steps ([#2456](https://github.com/withcoral/coral/issues/2456)) ([0594094](https://github.com/withcoral/coral/commit/05940948d06b774d33df89afe258996f9c302209))


### Bug Fixes

* **app:** sweep device authorization leases every 15 s ([#2452](https://github.com/withcoral/coral/issues/2452)) ([1a4e4ae](https://github.com/withcoral/coral/commit/1a4e4ae5d05e841862bd7d1910e54148c1b02cce))
* **catalog:** import OpenAPI Providers whose Operations request different OAuth scopes ([#2401](https://github.com/withcoral/coral/issues/2401)) ([3acfe55](https://github.com/withcoral/coral/commit/3acfe5538a34842a6a088ffdbee3808f8c79702b))
* **catalog:** keep keyword names in Operation Call Paths ([#2506](https://github.com/withcoral/coral/issues/2506)) ([b20795b](https://github.com/withcoral/coral/commit/b20795b2a71e2b5c5ba6b9c617a568e524ffef96))
* **catalog:** never make an Operation unavailable for search material ([#2465](https://github.com/withcoral/coral/issues/2465)) ([f83c909](https://github.com/withcoral/coral/commit/f83c909b667ff99cecb929006c11d768164a9922))
* **reef:** align the catalog table with the first rail entry ([#2484](https://github.com/withcoral/coral/issues/2484)) ([fb59758](https://github.com/withcoral/coral/commit/fb597588cddac3bb246515b5f8efcf6546da83c4))
* **reef:** calm the Activity badge and keep it while the stream reconnects ([#2468](https://github.com/withcoral/coral/issues/2468)) ([1b66639](https://github.com/withcoral/coral/commit/1b666397fd671439eac1e009e48c18aba567713d))
* **reef:** frame the providers table again and lower the top bar ([#2472](https://github.com/withcoral/coral/issues/2472)) ([87e41ee](https://github.com/withcoral/coral/commit/87e41eedb5dd96084877ded39c1492d612dd04e5))
* **reef:** replace loose help text with info tips and banners ([#2461](https://github.com/withcoral/coral/issues/2461)) ([695500a](https://github.com/withcoral/coral/commit/695500a4e6207f1f2dd7cf18f774d276cfe98f49))
* **reef:** round the page's corners beside the sidebar ([#2503](https://github.com/withcoral/coral/issues/2503)) ([a1fef64](https://github.com/withcoral/coral/commit/a1fef6498be0a35ce2493c237241ffa081876041))
* **reef:** shorten OAuth scope chips that share a URL prefix ([#2455](https://github.com/withcoral/coral/issues/2455)) ([5ae67ec](https://github.com/withcoral/coral/commit/5ae67ec6294ac25f5b45b3b91d1c0c99d9878e9e))
* **run:** report an MCP JSON-RPC rejection as invalid_input or upstream_rejected ([#2402](https://github.com/withcoral/coral/issues/2402)) ([749770b](https://github.com/withcoral/coral/commit/749770b4ccb41d58d7cb9eb633f0035be94a26f7))
* **search:** load each Generation's schema graph once per search ([#2448](https://github.com/withcoral/coral/issues/2448)) ([445d945](https://github.com/withcoral/coral/commit/445d94569105087c42586756c12be1019883c63e))
* **search:** retry a failed Saved Function only after one interval ([#2460](https://github.com/withcoral/coral/issues/2460)) ([a61ab3a](https://github.com/withcoral/coral/commit/a61ab3aa0ed6073a810d46de33dd68ace382a546))


### Performance Improvements

* **catalog:** project each named TypeScript type once per Provider ([#2443](https://github.com/withcoral/coral/issues/2443)) ([153c273](https://github.com/withcoral/coral/commit/153c273c03cd90ab3daeb1f7e61cb3d2f1ddf7f0))
* **search:** prepare search entries in the indexer after commit ([#2467](https://github.com/withcoral/coral/issues/2467)) ([1ab6765](https://github.com/withcoral/coral/commit/1ab67651d9c3d3dbefbd03d1f9c7cde31cffe1b4))
* **search:** recount only Workspaces whose rows changed ([#2457](https://github.com/withcoral/coral/issues/2457)) ([777cfb3](https://github.com/withcoral/coral/commit/777cfb335a457ef1ae9306f1ae8d6c2f1dd667e4))
* **search:** skip unchanged rows when an index write runs again ([#2459](https://github.com/withcoral/coral/issues/2459)) ([43b5188](https://github.com/withcoral/coral/commit/43b518821a3344e896e0c809d8b3632c896b935e))
* **search:** write only the statistics rows that changed ([#2458](https://github.com/withcoral/coral/issues/2458)) ([8e2832c](https://github.com/withcoral/coral/commit/8e2832cc465a192035281ec20d12617dd0d6687d))

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

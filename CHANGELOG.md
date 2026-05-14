# Changelog

## [0.2.1](https://github.com/withcoral/coral/compare/v0.2.0...v0.2.1) (2026-05-14)


### Features

* **cli:** bootstrap local web UI ([#233](https://github.com/withcoral/coral/issues/233)) ([e46f8e1](https://github.com/withcoral/coral/commit/e46f8e1241f4440c9e98a1c011284e2d410dc76e))
* **client:** instrument local gRPC service spans ([#327](https://github.com/withcoral/coral/issues/327)) ([13d571a](https://github.com/withcoral/coral/commit/13d571ad50a4807491ae594b1ef5f2ca2c662481))
* **engine:** add final-result observer hook ([#304](https://github.com/withcoral/coral/issues/304)) ([f08c9c1](https://github.com/withcoral/coral/commit/f08c9c1398f5d300f812bc713b8794d9a2db672e))
* **engine:** advertise source-scoped table functions ([#237](https://github.com/withcoral/coral/issues/237)) ([f63bc52](https://github.com/withcoral/coral/commit/f63bc52f40f4882d043fe17796d236075be16032))
* **engine:** emit service-map-friendly HTTP client spans ([#326](https://github.com/withcoral/coral/issues/326)) ([a0990c0](https://github.com/withcoral/coral/commit/a0990c0bda56f3c20a17b573a9bac968b28737a7))
* **engine:** execute internal source UDTFs ([#306](https://github.com/withcoral/coral/issues/306)) ([9f348f6](https://github.com/withcoral/coral/commit/9f348f6c8978a5429d152a9ac40b6bf6585c43dd))
* **engine:** instrument optimizer rules ([#300](https://github.com/withcoral/coral/issues/300)) ([b5c6d13](https://github.com/withcoral/coral/commit/b5c6d1352026d4a9549e1265c20a9f2f74448000))
* **engine:** plan source-scoped table functions ([#243](https://github.com/withcoral/coral/issues/243)) ([3d8ace5](https://github.com/withcoral/coral/commit/3d8ace5128a6f540d3f903c36ef86845a4d2acd2))
* **feedback:** upload MCP feedback reports to Coral ([#353](https://github.com/withcoral/coral/issues/353)) ([838fb9a](https://github.com/withcoral/coral/commit/838fb9a3e5b96b4000edd0f36f4cec41c9a907f8))
* **mcp:** add describe_table discovery ([#282](https://github.com/withcoral/coral/issues/282)) ([9233c47](https://github.com/withcoral/coral/commit/9233c47bdd6c2b8291c76db0dfd552f9ce9259f4))
* **mcp:** add protocol spans and error telemetry ([#328](https://github.com/withcoral/coral/issues/328)) ([6ef2464](https://github.com/withcoral/coral/commit/6ef24647956b5a3bfb86241cca50f143eec3ec01))
* **mcp:** add regex table discovery ([#281](https://github.com/withcoral/coral/issues/281)) ([543f4ce](https://github.com/withcoral/coral/commit/543f4cece9c02c642f250f9ee0747490d245a73d))
* **mcp:** list table columns progressively ([#283](https://github.com/withcoral/coral/issues/283)) ([54037d3](https://github.com/withcoral/coral/commit/54037d33c60707339abdff58f44a4cb24bb48c97))
* **mcp:** paginate list_tables discovery ([#265](https://github.com/withcoral/coral/issues/265)) ([a651d38](https://github.com/withcoral/coral/commit/a651d38257d3c175e0f79dcf2ec74e9fea13f1e6))
* **plugin:** add Coral Codex plugin ([#315](https://github.com/withcoral/coral/issues/315)) ([730d31a](https://github.com/withcoral/coral/commit/730d31ad9f173fa2ca3003619238fba88861daa1))
* **sources/wandb:** add W&B experiment metrics source ([#316](https://github.com/withcoral/coral/issues/316)) ([1083477](https://github.com/withcoral/coral/commit/10834779da41f6d703d840dd29414cf60fd6d73c))
* **sources:** roll out source-scoped table functions ([#244](https://github.com/withcoral/coral/issues/244)) ([dcc7d0d](https://github.com/withcoral/coral/commit/dcc7d0dbd0b18b37d688380e5646ad24758babae))
* **spec:** add source-scoped table function specs ([#245](https://github.com/withcoral/coral/issues/245)) ([cf25086](https://github.com/withcoral/coral/commit/cf25086db5899851f79c6c26068bb9ee2382806b))


### Bug Fixes

* **app:** record telemetry status attributes ([#285](https://github.com/withcoral/coral/issues/285)) ([920e4b5](https://github.com/withcoral/coral/commit/920e4b51823abac425c88387aeaf836797c1f664))
* **engine:** preserve structured table-not-found references ([#299](https://github.com/withcoral/coral/issues/299)) ([28ce34f](https://github.com/withcoral/coral/commit/28ce34f31f65cdc0a4967475eaa263486ab3f4f1))
* **mcp:** advertise object-root output schema ([#415](https://github.com/withcoral/coral/issues/415)) ([2cef7de](https://github.com/withcoral/coral/commit/2cef7de7184657e8bb89ffbf4fdf22a356722bd0))
* **mcp:** align search table output schema with response ([#349](https://github.com/withcoral/coral/issues/349)) ([3048f5c](https://github.com/withcoral/coral/commit/3048f5cec2b23156a87bb666af934737b1be42c1))
* **mcp:** preserve structured resource errors ([#295](https://github.com/withcoral/coral/issues/295)) ([c1286a3](https://github.com/withcoral/coral/commit/c1286a35a2a78bf9d089b2498edafbb73f54cb09))
* **mcp:** return guide in table search results ([#340](https://github.com/withcoral/coral/issues/340)) ([5196b22](https://github.com/withcoral/coral/commit/5196b22910c5fb313fa221d4de01ebd257fd86d1))
* quote qualified table hints ([#280](https://github.com/withcoral/coral/issues/280)) ([3be0ff6](https://github.com/withcoral/coral/commit/3be0ff6066a1a8cbe706fe8d0e044721126542fd))
* **sources:** push down repository owner filter ([#284](https://github.com/withcoral/coral/issues/284)) ([703f349](https://github.com/withcoral/coral/commit/703f349c3ada3bd78dfceb98abacb8e0076fec48))
* **spec:** allow function-only HTTP manifests ([#310](https://github.com/withcoral/coral/issues/310)) ([4e7e127](https://github.com/withcoral/coral/commit/4e7e1277ecc0f84a11d629f9e3d2d0726e950d9f))
* **spec:** validate inner expr for base64_decode ([#322](https://github.com/withcoral/coral/issues/322)) ([9af5382](https://github.com/withcoral/coral/commit/9af5382c32f701246471e17520340f5aab19db69))
* table name validation ([#317](https://github.com/withcoral/coral/issues/317)) ([3f1a70e](https://github.com/withcoral/coral/commit/3f1a70e9f2d219756a606dd9f938f2d2282d3c43))


### Performance Improvements

* **app:** avoid parsing installed manifests twice ([#298](https://github.com/withcoral/coral/issues/298)) ([539e1c9](https://github.com/withcoral/coral/commit/539e1c96e6d52c341ad012e2434a8538f0534b98))

## [0.2.0](https://github.com/withcoral/coral/compare/v0.1.5...v0.2.0) (2026-05-06)


### ⚠ BREAKING CHANGES

* **engine:** replace runtime provider with config ([#218](https://github.com/withcoral/coral/issues/218))

### Features

* adds notion source, adds support for iso8601 timestamp ([#266](https://github.com/withcoral/coral/issues/266)) ([b8a18d5](https://github.com/withcoral/coral/commit/b8a18d5ed013ba18d830329ca37811377585cb2c))
* **app:** add OpenTelemetry tracing and metrics ([#37](https://github.com/withcoral/coral/issues/37)) ([f103711](https://github.com/withcoral/coral/commit/f1037114d9cb4e7cd4a560eb936be368830dcb60))
* **cli:** add `source info` command ([#188](https://github.com/withcoral/coral/issues/188)) ([5722a73](https://github.com/withcoral/coral/commit/5722a733dd2ef5edae1a72964e14f0b3a4a33dba))
* **engine:** add DataFusion query tracing instrumentation ([#273](https://github.com/withcoral/coral/issues/273)) ([9b3eb2b](https://github.com/withcoral/coral/commit/9b3eb2b397d0f7b6fbd633440356df837ec61d8c))
* **http-dsl:** support text request bodies and JSONEachRow responses ([#204](https://github.com/withcoral/coral/issues/204)) ([50234ba](https://github.com/withcoral/coral/commit/50234ba9f065c22480b350f669508852984db81f))
* **mcp:** add opt-in feedback tool ([#248](https://github.com/withcoral/coral/issues/248)) ([3279fcd](https://github.com/withcoral/coral/commit/3279fcd329bbd85bc57c9f5ad7b662f456161497))
* **sources/sentry:** add query filter to sentry.issues ([#230](https://github.com/withcoral/coral/issues/230)) ([f9f00ed](https://github.com/withcoral/coral/commit/f9f00ed601e80b24b789956e6c820061d4a2db34))
* **sources/sentry:** add short_id column ([#234](https://github.com/withcoral/coral/issues/234)) ([56cb697](https://github.com/withcoral/coral/commit/56cb697d56e8fd22e58e6f2a069a8244fc830287))
* **spec:** decode base64 content columns ([#257](https://github.com/withcoral/coral/issues/257)) ([464a4b4](https://github.com/withcoral/coral/commit/464a4b49083f69ac11bde2ca62fcdffa4740026c))


### Bug Fixes

* **catalog:** expose column nullability ([#254](https://github.com/withcoral/coral/issues/254)) ([a3e517e](https://github.com/withcoral/coral/commit/a3e517ed5ee0827acacd29ee6b45ea563943d818))
* **docs:** correct How Coral works wording ([#270](https://github.com/withcoral/coral/issues/270)) ([9616f87](https://github.com/withcoral/coral/commit/9616f8790eb05e84c78539bda3ace84cb65611ce))
* **engine:** allow literal regex patterns and escaped SIMILAR TO wildcards ([#223](https://github.com/withcoral/coral/issues/223)) ([43bdd28](https://github.com/withcoral/coral/commit/43bdd285fa1592cb607a8ef6b9d685cddb54d6b9))
* **engine:** include filters in provider hints ([#260](https://github.com/withcoral/coral/issues/260)) ([21e7ef2](https://github.com/withcoral/coral/commit/21e7ef2f7fcecbcc9a39ab917d454ad0f57c21f8))
* **engine:** normalize HTTP provider transport errors ([#219](https://github.com/withcoral/coral/issues/219)) ([cea0000](https://github.com/withcoral/coral/commit/cea0000d76d560c49201f311684482590f7689a0))
* **engine:** preserve file source input metadata ([#271](https://github.com/withcoral/coral/issues/271)) ([28bd32d](https://github.com/withcoral/coral/commit/28bd32d0510db4603de78ce3621f897536e30afd))
* **engine:** redact request error URLs ([#238](https://github.com/withcoral/coral/issues/238)) ([b8e4094](https://github.com/withcoral/coral/commit/b8e4094093257a44e6d6eca7298fc41a9dab5f3b))
* **engine:** use test_runtime() in http_tests after runtime config refactor ([#228](https://github.com/withcoral/coral/issues/228)) ([7159ce3](https://github.com/withcoral/coral/commit/7159ce3dd89adea7a69039f4def46fd0418640c6))
* **engine:** validate regex-style query patterns ([#214](https://github.com/withcoral/coral/issues/214)) ([259cd27](https://github.com/withcoral/coral/commit/259cd27dc2ecbf0ffc5c2cb625b07df92952e30e))
* **github:** include closed pulls by default ([#256](https://github.com/withcoral/coral/issues/256)) ([50ee0a4](https://github.com/withcoral/coral/commit/50ee0a40d31f74e11bee8c62aec06b4e7a7641d6))
* **github:** project common nested arrays ([#259](https://github.com/withcoral/coral/issues/259)) ([537f508](https://github.com/withcoral/coral/commit/537f5086cae1f944b824d9ac194a053480e1c78e))
* keep config file data when sources are modified ([#277](https://github.com/withcoral/coral/issues/277)) ([5347fba](https://github.com/withcoral/coral/commit/5347fbafadea46aec743a154535cd13e8bb00a83))
* **linear:** add workflow and status metadata ([#251](https://github.com/withcoral/coral/issues/251)) ([8a715e9](https://github.com/withcoral/coral/commit/8a715e949e0d47fd6a03d1c9ae46dd96397905f4))
* **linear:** expose richer issue and project metadata ([#249](https://github.com/withcoral/coral/issues/249)) ([2abd8b4](https://github.com/withcoral/coral/commit/2abd8b48df75863584dd87b67c6e478d6ef473ad))
* **linear:** read comments by issue identifier ([#261](https://github.com/withcoral/coral/issues/261)) ([09d6360](https://github.com/withcoral/coral/commit/09d63604178f6c75875252aa8d75056040121934))
* **output:** preserve null JSON fields ([#255](https://github.com/withcoral/coral/issues/255)) ([47b5dfe](https://github.com/withcoral/coral/commit/47b5dfe0ded3278287f310651d2755cdd274ea11))
* **sources/cloudwatch_metrics:** expose metric statistics time filters ([#216](https://github.com/withcoral/coral/issues/216)) ([7c87c61](https://github.com/withcoral/coral/commit/7c87c61172ece56607859748f9294966912b92b1))
* **spec:** reject duplicate table names ([#242](https://github.com/withcoral/coral/issues/242)) ([d78c4c8](https://github.com/withcoral/coral/commit/d78c4c87c358b55f797a2237a699cf0c27318c3c))


### Code Refactoring

* **engine:** replace runtime provider with config ([#218](https://github.com/withcoral/coral/issues/218)) ([6d70cc7](https://github.com/withcoral/coral/commit/6d70cc78b9fb9336be957115f77e438a9a2a748a))

## [0.1.5](https://github.com/withcoral/coral/compare/v0.1.4...v0.1.5) (2026-04-27)


### Features

* add cloudwatch source ([#200](https://github.com/withcoral/coral/issues/200)) ([e8d049d](https://github.com/withcoral/coral/commit/e8d049d7ac7788b5b71bc72e3742ba4a59565be9))
* **cli:** add --interactive flag to `coral source add` ([#164](https://github.com/withcoral/coral/issues/164)) ([de47a0c](https://github.com/withcoral/coral/commit/de47a0c05607a1f3f27962c3e773deed42be5bc3))
* **cli:** add `coral completion` for shell completions ([#205](https://github.com/withcoral/coral/issues/205)) ([32bf6e8](https://github.com/withcoral/coral/commit/32bf6e8f726db6f347f003edfa69c20280c77410))
* **cli:** improve `source test` error message ([#206](https://github.com/withcoral/coral/issues/206)) ([ae8206d](https://github.com/withcoral/coral/commit/ae8206d0d7df44854d35c5076aee4bb4d6f7016c))
* custom authenticators ([#173](https://github.com/withcoral/coral/issues/173)) ([cf5b406](https://github.com/withcoral/coral/commit/cf5b406d2189fcfc844ed6025441249c795c7749))
* **engine:** add JSON manifest type and query functions ([#160](https://github.com/withcoral/coral/issues/160)) ([5ddbadc](https://github.com/withcoral/coral/commit/5ddbadc8a865d606591997aa6d6ac5983456349d))
* **engine:** structured TABLE_NOT_FOUND and UNKNOWN_FIELD errors ([#120](https://github.com/withcoral/coral/issues/120)) ([86379d8](https://github.com/withcoral/coral/commit/86379d8074fb447cbafa3ff2dd2b7f1442c4bbdf))
* **engine:** suggest schema-qualified name for unqualified table misses ([#162](https://github.com/withcoral/coral/issues/162)) ([246a743](https://github.com/withcoral/coral/commit/246a743d056fb5c61052efc74eccaf6e1b485d7d))
* **mcp:** decode AIP-193 structured errors for tool responses ([#102](https://github.com/withcoral/coral/issues/102)) ([973415f](https://github.com/withcoral/coral/commit/973415f075e8367c4be6f04aa5d4478f5ed443db))
* **slack:** read thread replies ([#199](https://github.com/withcoral/coral/issues/199)) ([e014af3](https://github.com/withcoral/coral/commit/e014af3913ecab96d6e74f301f4a918f5a01222e))
* **sources/grafana:** add authored table guides ([#166](https://github.com/withcoral/coral/issues/166)) ([9b38279](https://github.com/withcoral/coral/commit/9b3827993e3e5fde911d375d9b2a58e23537c140))


### Bug Fixes

* **app:** make bundled source versions explicit in config state ([#169](https://github.com/withcoral/coral/issues/169)) ([d2579f0](https://github.com/withcoral/coral/commit/d2579f09722c6a6f37509385712378821d4c7eaa))
* centralize local name validation invariants ([#193](https://github.com/withcoral/coral/issues/193)) ([108c26f](https://github.com/withcoral/coral/commit/108c26f280150dd3dd19b76a5de7fc51b5eddb45))
* **engine:** pass tables arg to datafusion_to_core in json registration ([#192](https://github.com/withcoral/coral/issues/192)) ([7ea0a12](https://github.com/withcoral/coral/commit/7ea0a12148f0fb09c1bada1071ce50e3c0373a5f))
* **sources:** restore truncated column descriptions ([#170](https://github.com/withcoral/coral/issues/170)) ([579285f](https://github.com/withcoral/coral/commit/579285f1199c0ebc1646e539ba10b40c373c6d84))
* **spec:** recognize inputs block for parquet and jsonl manifests ([#159](https://github.com/withcoral/coral/issues/159)) ([9c28287](https://github.com/withcoral/coral/commit/9c28287a2dde1ac50dcd5a84350c7b7944c7d50c))

## [0.1.4](https://github.com/withcoral/coral/compare/v0.1.3...v0.1.4) (2026-04-22)


### Features

* **cli:** render structured query errors as Error/Detail/Hint ([#100](https://github.com/withcoral/coral/issues/100)) ([e2d7e45](https://github.com/withcoral/coral/commit/e2d7e45d0273487b87e25e2f7b794d2b5bf38376))
* **cli:** run source-spec test queries during source test ([#107](https://github.com/withcoral/coral/issues/107)) ([035b951](https://github.com/withcoral/coral/commit/035b95181b4fd60dcfd598af5255fb01730ce23f))
* confluence ([#150](https://github.com/withcoral/coral/issues/150)) ([a8b1a71](https://github.com/withcoral/coral/commit/a8b1a7165d894c161cc81b5ca8b6e2ab31a5f24d))
* **docs:** auto-generate bundled-sources.mdx from manifests ([#106](https://github.com/withcoral/coral/issues/106)) ([9626942](https://github.com/withcoral/coral/commit/9626942e7bb5c084c67edb5c1c828f48a13f7658))
* **engine:** emit AIP-193 structured errors for provider failures ([#92](https://github.com/withcoral/coral/issues/92)) ([e3ad047](https://github.com/withcoral/coral/commit/e3ad0470bcf93887192b5353a7e9af544517cb2b))
* **engine:** expose source config via coral.inputs ([#121](https://github.com/withcoral/coral/issues/121)) ([065abc8](https://github.com/withcoral/coral/commit/065abc88ac373736a363c30e0ed25c2615dbbfa0))
* jira ([#23](https://github.com/withcoral/coral/issues/23)) ([06ae588](https://github.com/withcoral/coral/commit/06ae5881a90a03a3b5fd895b2d050b58c0fb837d))
* jira source spec ([06ae588](https://github.com/withcoral/coral/commit/06ae5881a90a03a3b5fd895b2d050b58c0fb837d))
* **sources/linear:** expose project milestones ([#148](https://github.com/withcoral/coral/issues/148)) ([fccd4ca](https://github.com/withcoral/coral/commit/fccd4caaa275ca21c490156a781207194004a81f))
* **spec:** top level input declaration ([#97](https://github.com/withcoral/coral/issues/97)) ([16b70e5](https://github.com/withcoral/coral/commit/16b70e564b5484739807413324f6ed2b6211224b))


### Bug Fixes

* **engine:** default coral.columns to schema order ([#105](https://github.com/withcoral/coral/issues/105)) ([56fd5a4](https://github.com/withcoral/coral/commit/56fd5a4dcc591401577cc4252259df034c947747))
* **engine:** enforce static schema provider immutability ([#109](https://github.com/withcoral/coral/issues/109)) ([5476d4d](https://github.com/withcoral/coral/commit/5476d4d3b5bb044336085e16064bf0ec0bf02baa))
* **engine:** retry github 403 reset-based rate limits ([#110](https://github.com/withcoral/coral/issues/110)) ([62bb4cf](https://github.com/withcoral/coral/commit/62bb4cf3f357b6ca455bedf68e38748df1081fcd))
* **engine:** skip fabricated zero rows for malformed HTTP series points ([#112](https://github.com/withcoral/coral/issues/112)) ([3be695d](https://github.com/withcoral/coral/commit/3be695dba20822bf408e975331e2a3ca4f326925))
* **sources/jira:** align Jira ID column types ([#137](https://github.com/withcoral/coral/issues/137)) ([087d2ae](https://github.com/withcoral/coral/commit/087d2ae36911536a0551c54c29a7ad09aafce83d))
* **sources:** strip HTML tags from bundled source descriptions ([#161](https://github.com/withcoral/coral/issues/161)) ([8c22b56](https://github.com/withcoral/coral/commit/8c22b56147df1a4202522a53a4b189f71e6022fb))

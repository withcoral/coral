# Changelog

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

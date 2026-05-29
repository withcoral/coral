# Changelog

## [0.4.1](https://github.com/withcoral/coral/compare/v0.4.0...v0.4.1) (2026-05-27)


### Features

* **cli:** add runtime feature flags ([#784](https://github.com/withcoral/coral/issues/784)) ([862f269](https://github.com/withcoral/coral/commit/862f269de1cf2fbdb04603d0346d147a608c116a))
* **credentials:** support OAuth device flow ([#360](https://github.com/withcoral/coral/issues/360)) ([1413c2b](https://github.com/withcoral/coral/commit/1413c2b1fe450a01ca69d1406663d09b39cfe048))
* **sources/community/telegram:** add Telegram Bot API source ([#863](https://github.com/withcoral/coral/issues/863)) ([5a16775](https://github.com/withcoral/coral/commit/5a16775e3fbf40226e08ab1fea18a1430f3af7de))
* **sources/core/gitlab:** add OAuth credential method ([#411](https://github.com/withcoral/coral/issues/411)) ([8278f33](https://github.com/withcoral/coral/commit/8278f3311356f74bc5b6516358dcb766640b470c))
* **sources/github:** add OAuth device flow ([#361](https://github.com/withcoral/coral/issues/361)) ([fdd9018](https://github.com/withcoral/coral/commit/fdd90180b83581615ffa1cde8ef8b0768dad2ab5))


### Bug Fixes

* **release:** mark promoted releases as latest ([#876](https://github.com/withcoral/coral/issues/876)) ([ea5b53d](https://github.com/withcoral/coral/commit/ea5b53d4db673db303e33f38a5f2af84ae4b263b))

## [0.4.0](https://github.com/withcoral/coral/compare/v0.3.0...v0.4.0) (2026-05-27)


### ⚠ BREAKING CHANGES

* **file:** add unified file backend ([#453](https://github.com/withcoral/coral/issues/453))

### Features

* **app:** add keychain secret storage ([#761](https://github.com/withcoral/coral/issues/761)) ([6b03b48](https://github.com/withcoral/coral/commit/6b03b48eacd5c6d2901384954285cdcb96a9d2c2))
* **app:** keep local HTTP body previews off spans ([#376](https://github.com/withcoral/coral/issues/376)) ([20235d0](https://github.com/withcoral/coral/commit/20235d07a5468037f44bcb16d95c736da6f65b75))
* **community:** add OpenSSF Scorecard source — security health checks for any GitHub repo ([#832](https://github.com/withcoral/coral/issues/832)) ([a0d40dd](https://github.com/withcoral/coral/commit/a0d40dd3173f5f47a168bd2cb0f834c732a73d4f))
* **file:** add unified file backend ([#453](https://github.com/withcoral/coral/issues/453)) ([0ddaabc](https://github.com/withcoral/coral/commit/0ddaabc30de02913e6c0cf8c76b83115cc936297))
* **mcp:** add MCP-backed source support ([#402](https://github.com/withcoral/coral/issues/402)) ([a7f167f](https://github.com/withcoral/coral/commit/a7f167fca549cbc59f38560316f0ca293a5da580))
* **sources/community/advice-slip:** add Advice Slip source ([#857](https://github.com/withcoral/coral/issues/857)) ([cb3979f](https://github.com/withcoral/coral/commit/cb3979fdb6ac9e7c9e6d3f28bad6b32f1ec35da3))
* **sources/community/adzuna:** add Adzuna search community source ([#815](https://github.com/withcoral/coral/issues/815)) ([099b42e](https://github.com/withcoral/coral/commit/099b42e409d3bb81e43eaabce83a8141815f6bfb))
* **sources/community/auth0:** add Auth0 community source ([#719](https://github.com/withcoral/coral/issues/719)) ([20caa73](https://github.com/withcoral/coral/commit/20caa73fe01f6e9a823280a922937da115a9a9fc))
* **sources/community/axiom:** add Axiom community source ([#628](https://github.com/withcoral/coral/issues/628)) ([59e4b95](https://github.com/withcoral/coral/commit/59e4b95ef19f999aacce9f935c84468e8f9d7df0))
* **sources/community/bitbucket:** add bitbucket community source ([#744](https://github.com/withcoral/coral/issues/744)) ([9aac12b](https://github.com/withcoral/coral/commit/9aac12b8dd60f42f05d49e7f681a87995e740d72))
* **sources/community/checkly:** add Checkly community source ([#630](https://github.com/withcoral/coral/issues/630)) ([826fbe1](https://github.com/withcoral/coral/commit/826fbe1ad449a8e2ac3a13e193f6b000752f99e7))
* **sources/community/chucknorris:** add Chuck Norris API community source ([#770](https://github.com/withcoral/coral/issues/770)) ([985e6c3](https://github.com/withcoral/coral/commit/985e6c36892de95c0a1e8a444acc58d7917b255d))
* **sources/community/coingecko:** add CoinGecko API community source ([#781](https://github.com/withcoral/coral/issues/781)) ([80e1538](https://github.com/withcoral/coral/commit/80e1538027a68a027c91e922833774e4b7e51e5b))
* **sources/community/coolify:** add coolify community source ([#751](https://github.com/withcoral/coral/issues/751)) ([5caf601](https://github.com/withcoral/coral/commit/5caf601161d2ae3a9de5f2594cc405028d8565af)), closes [#729](https://github.com/withcoral/coral/issues/729)
* **sources/community/country-state-city:** add Country State City source ([#859](https://github.com/withcoral/coral/issues/859)) ([67b39af](https://github.com/withcoral/coral/commit/67b39afe2117a5f68a2fdf791f303f283df42072))
* **sources/community/dbt_cloud:** add dbt cloud source ([#627](https://github.com/withcoral/coral/issues/627)) ([83a8ae6](https://github.com/withcoral/coral/commit/83a8ae6dc5e2b478ae4b4546344c7de3c3119356))
* **sources/community/deps_dev:** add deps_dev community source ([#799](https://github.com/withcoral/coral/issues/799)) ([a179119](https://github.com/withcoral/coral/commit/a179119b2d8f9d00685e52ca82158c5f8dfdbd8c))
* **sources/community/devto:** add DEV.to community source ([#811](https://github.com/withcoral/coral/issues/811)) ([d99ed85](https://github.com/withcoral/coral/commit/d99ed85f6941115b8e3bcd366264c955bff8ed54))
* **sources/community/dog-ceo:** add Dog CEO source ([#861](https://github.com/withcoral/coral/issues/861)) ([11fc86e](https://github.com/withcoral/coral/commit/11fc86e1fc366c3072b3497dfb496efc72af76da))
* **sources/community/dub:** add Dub.co community source ([#668](https://github.com/withcoral/coral/issues/668)) ([1361d00](https://github.com/withcoral/coral/commit/1361d0001557aafacb417b1aecce349fc15d2ede))
* **sources/community/frankfurter:** add Frankfurter exchange rates community source ([#772](https://github.com/withcoral/coral/issues/772)) ([7ed3e5a](https://github.com/withcoral/coral/commit/7ed3e5a5ed6a7876198425844a5bd03f47ebf0cd))
* **sources/community/free-dictionary:** add Free Dictionary API community source ([#779](https://github.com/withcoral/coral/issues/779)) ([940e8cc](https://github.com/withcoral/coral/commit/940e8ccb0627804eccd650074b69513a511b656c))
* **sources/community/ghost:** add ghost cms integration ([#713](https://github.com/withcoral/coral/issues/713)) ([21213dc](https://github.com/withcoral/coral/commit/21213dc39a04c488f978764f91fb6114f90523d9))
* **sources/community/google_contacts:** add Google Contacts source ([#846](https://github.com/withcoral/coral/issues/846)) ([f6d3402](https://github.com/withcoral/coral/commit/f6d3402331066a3cc6a91ede02970ddabc6e8cbb))
* **sources/community/google_drive:** add Google Drive source ([#699](https://github.com/withcoral/coral/issues/699)) ([c5329a4](https://github.com/withcoral/coral/commit/c5329a4ebeaff18a866cfb59cf208a4082840676))
* **sources/community/groq_ai:** add Groq AI community source ([#754](https://github.com/withcoral/coral/issues/754)) ([11ce682](https://github.com/withcoral/coral/commit/11ce682e12b9b31346168ff595e5b309a34881dd))
* **sources/community/hn:** add dedicated category tables ([#803](https://github.com/withcoral/coral/issues/803)) ([8a18651](https://github.com/withcoral/coral/commit/8a186511857a30ed5fbce8c99c3626d54b3fdf43))
* **sources/community/influxdb:** add InfluxDB community source ([#623](https://github.com/withcoral/coral/issues/623)) ([b2a6664](https://github.com/withcoral/coral/commit/b2a666443fbb23ca1ef6386b5a5ea3fad453cd9b))
* **sources/community/ip_api:** add ip-api community source ([#844](https://github.com/withcoral/coral/issues/844)) ([eb9df37](https://github.com/withcoral/coral/commit/eb9df376272377c70c7322642b29bab43deab80b))
* **sources/community/jikan:** add Jikan community source ([#802](https://github.com/withcoral/coral/issues/802)) ([82f5924](https://github.com/withcoral/coral/commit/82f5924e0a8d295494a7d65fb3c7cb98fe574e15))
* **sources/community/jsonplaceholder:** add JSONPlaceholder source ([#840](https://github.com/withcoral/coral/issues/840)) ([9f4c04a](https://github.com/withcoral/coral/commit/9f4c04a30cf61328acb9eaf2f708f48c87eb6ac5))
* **sources/community/k8s:** add kubernetes community source ([#420](https://github.com/withcoral/coral/issues/420)) ([c19c5b8](https://github.com/withcoral/coral/commit/c19c5b82aada8e4ec34b7672a15f87ebc336e0db))
* **sources/community/keycloak:** add keycloak community source ([#739](https://github.com/withcoral/coral/issues/739)) ([df70a35](https://github.com/withcoral/coral/commit/df70a3521cf501509d419e5ceb2d2a2441d32e25))
* **sources/community/lm_studio:** add LM Studio community source ([#834](https://github.com/withcoral/coral/issues/834)) ([249f6fc](https://github.com/withcoral/coral/commit/249f6fc132b730188d0fb8cf05ebb744fdcf4585))
* **sources/community/metabase:** add Metabase source ([#785](https://github.com/withcoral/coral/issues/785)) ([5e3e5fa](https://github.com/withcoral/coral/commit/5e3e5fa89922dc914e6bf439f79a942b7f71ee4f))
* **sources/community/musicbrainz:** add MusicBrainz community source ([#789](https://github.com/withcoral/coral/issues/789)) ([2a54128](https://github.com/withcoral/coral/commit/2a54128cc9fad5e9d713bcb0ed189a1fc594a76d))
* **sources/community/neo4j:** add Neo4j community source ([#552](https://github.com/withcoral/coral/issues/552)) ([b649b60](https://github.com/withcoral/coral/commit/b649b60c0a39b92c0b77e4136e65f7d584482d45))
* **sources/community/novu:** add Novu community source ([#591](https://github.com/withcoral/coral/issues/591)) ([ac316e2](https://github.com/withcoral/coral/commit/ac316e28955f32e5f084ea8d5a7120894cc3baad)), closes [#586](https://github.com/withcoral/coral/issues/586)
* **sources/community/ollama:** add Ollama community source ([#798](https://github.com/withcoral/coral/issues/798)) ([d716dd1](https://github.com/withcoral/coral/commit/d716dd1e33401546c1b8d5dd7b135f8d2b32aac1))
* **sources/community/open-meteo:** add Open-Meteo source ([#786](https://github.com/withcoral/coral/issues/786)) ([1795455](https://github.com/withcoral/coral/commit/1795455ed7929ec81b2f2aa8a4cde1780b6f4758))
* **sources/community/openlibrary:** add Open Library API community source ([#780](https://github.com/withcoral/coral/issues/780)) ([6c08c23](https://github.com/withcoral/coral/commit/6c08c236bc7ccac79e664385e91fb1b448e319c0))
* **sources/community/openmetadata:** add OpenMetadata source ([#804](https://github.com/withcoral/coral/issues/804)) ([fa121e4](https://github.com/withcoral/coral/commit/fa121e43b1126cc6b4747fa06ad5fa725aec3f03))
* **sources/community/pinecone:** add Pinecone community source ([#513](https://github.com/withcoral/coral/issues/513)) ([632a2b8](https://github.com/withcoral/coral/commit/632a2b8bdf77e2c7fa3ed2e8df71c09d8d8b3916))
* **sources/community/plausible:** add Plausible Analytics community source ([#585](https://github.com/withcoral/coral/issues/585)) ([318acc9](https://github.com/withcoral/coral/commit/318acc9d8c71cda3ade477c49aa33a58fb02c771)), closes [#583](https://github.com/withcoral/coral/issues/583)
* **sources/community/pokeapi:** add PokeAPI community source ([#767](https://github.com/withcoral/coral/issues/767)) ([5a348d6](https://github.com/withcoral/coral/commit/5a348d6d77b88b0856dfec6bf5befbbd627db5b6))
* **sources/community/prometheus:** add prometheus community source ([#665](https://github.com/withcoral/coral/issues/665)) ([6178f0b](https://github.com/withcoral/coral/commit/6178f0bfd63bec3d43a6b2cd8b45e2e9b9dccf5f))
* **sources/community/public-holidays:** add Public Holidays (Nager.Dates) community source ([#775](https://github.com/withcoral/coral/issues/775)) ([1aa85bd](https://github.com/withcoral/coral/commit/1aa85bd8cf736eb6e48c911b0c6e487b688381e3))
* **sources/community/remotive:** add Remotive jobs connector via DSL v3 ([#685](https://github.com/withcoral/coral/issues/685)) ([e7eabaf](https://github.com/withcoral/coral/commit/e7eabaffc606da7fe8398d1d4adcf64d75d0770e))
* **sources/community/replicate:** add Replicate community source ([#629](https://github.com/withcoral/coral/issues/629)) ([e38b5c9](https://github.com/withcoral/coral/commit/e38b5c98c289e31bc5fa4107de0c26a35d6f68b1))
* **sources/community/rest-countries:** add REST Countries community source ([#765](https://github.com/withcoral/coral/issues/765)) ([413103f](https://github.com/withcoral/coral/commit/413103fd1944610936088f69cc0e1206d4c9f7bd))
* **sources/community/semaphore_ci:** add Semaphore CI community source ([#669](https://github.com/withcoral/coral/issues/669)) ([659758e](https://github.com/withcoral/coral/commit/659758e041414075ec5367563c47753661c44f99))
* **sources/community/terraform_cloud:** add Terraform Cloud source ([#756](https://github.com/withcoral/coral/issues/756)) ([8094db7](https://github.com/withcoral/coral/commit/8094db7395bf3dfba637fb2f9a5165ff9908ee6c))
* **sources/community/turso:** add Turso community source ([#590](https://github.com/withcoral/coral/issues/590)) ([f320662](https://github.com/withcoral/coral/commit/f3206625f6c2ff8107a5e5d7a437e7247229370f))
* **sources/community/typeform:** add Typeform community source ([#604](https://github.com/withcoral/coral/issues/604)) ([1a01518](https://github.com/withcoral/coral/commit/1a015184d0e12a6b319a691b1b5c1327c71c85ff))
* **sources/community/vercel:** add Vercel community source ([#434](https://github.com/withcoral/coral/issues/434)) ([3824d32](https://github.com/withcoral/coral/commit/3824d32611674527862bad01db82e7143a589f55))
* **sources/community/wikipedia:** add Wikipedia community source ([#787](https://github.com/withcoral/coral/issues/787)) ([74224b8](https://github.com/withcoral/coral/commit/74224b8ce1f2fa901b2251a151a93bfdad9f01e6))
* **sources/community/woocommerce:** add WooCommerce community source ([#638](https://github.com/withcoral/coral/issues/638)) ([9a77359](https://github.com/withcoral/coral/commit/9a77359ac4668f722bd2c685dea0eccb92b7f950))
* **sources/community/xkcd:** add xkcd community source ([#842](https://github.com/withcoral/coral/issues/842)) ([0c252d0](https://github.com/withcoral/coral/commit/0c252d02a6e7caa1eec2517d655e0d0ab5d27e9e))
* **sources/core/google_calendar:** add Google Calendar source ([#346](https://github.com/withcoral/coral/issues/346)) ([2cf281e](https://github.com/withcoral/coral/commit/2cf281e461556c871cfe0a355b67e57efec42fff))
* **sources/core/slack:** add OAuth credential method ([#380](https://github.com/withcoral/coral/issues/380)) ([b8d6f7d](https://github.com/withcoral/coral/commit/b8d6f7d307b93aed2cf2e6e450f3536183f88332))
* **sources/core:** add claude source ([#457](https://github.com/withcoral/coral/issues/457)) ([a2201ee](https://github.com/withcoral/coral/commit/a2201eef47bed84f76bba30d71c25522d5658101))
* **sources/core:** add codex source ([#456](https://github.com/withcoral/coral/issues/456)) ([a508f6d](https://github.com/withcoral/coral/commit/a508f6d07b68ab769f3a95a041b669db1018eeed))
* **sources/n8n:** add n8n community source spec ([#681](https://github.com/withcoral/coral/issues/681)) ([4058d00](https://github.com/withcoral/coral/commit/4058d0051d18652970f70574763ddd347b81d116))
* **sources:** Add Trello Community Source ([#763](https://github.com/withcoral/coral/issues/763)) ([14df2b3](https://github.com/withcoral/coral/commit/14df2b3c9ec7a78e2c1bc6cc33e478e40111d3c6))
* **ui:** add cmd+arrow shortcuts for trace navigation ([#776](https://github.com/withcoral/coral/issues/776)) ([5f96336](https://github.com/withcoral/coral/commit/5f96336133c833fa76ee18dacd6d0fde4cac1f4b))


### Bug Fixes

* **ci:** invoke Windows release smoke binary correctly ([#688](https://github.com/withcoral/coral/issues/688)) ([f67a494](https://github.com/withcoral/coral/commit/f67a494aec40787d28d6d911049350a28da59254))
* **docs:** skip community source catalog in docs check ([#800](https://github.com/withcoral/coral/issues/800)) ([48a7cda](https://github.com/withcoral/coral/commit/48a7cda4ec5e0b579df0950a635ae32af24a3f8e))
* **install:** make checksum verification resilient ([#742](https://github.com/withcoral/coral/issues/742)) ([b6018bf](https://github.com/withcoral/coral/commit/b6018bf6c55fb18864339bf951ad4cfcadf95bff))
* **spec:** validate column types in spec ([#760](https://github.com/withcoral/coral/issues/760)) ([0da9a65](https://github.com/withcoral/coral/commit/0da9a659c7d0294978a8f06246d8bbb00a1d6570))
* **ui/traces:** improve span tabs navigation ([#820](https://github.com/withcoral/coral/issues/820)) ([5a5eb2d](https://github.com/withcoral/coral/commit/5a5eb2df8c1a70f44b9469f005b3b3b1b810f4dc))
* **ui:** add expandable sidebar and query stream tooltip ([#652](https://github.com/withcoral/coral/issues/652)) ([7585465](https://github.com/withcoral/coral/commit/7585465ab055f478ad35483aa87ff5d2b34c84f8))
* **ui:** rework trace span detail layout ([#653](https://github.com/withcoral/coral/issues/653)) ([e851059](https://github.com/withcoral/coral/commit/e851059638310277fbd2f21f32a312a0d3dde2b1))

## [0.3.0](https://github.com/withcoral/coral/compare/v0.2.1...v0.3.0) (2026-05-22)


### ⚠ BREAKING CHANGES

* **catalog:** expose unified catalog discovery ([#459](https://github.com/withcoral/coral/issues/459))

### Features

* **app:** add catalog discovery service ([#448](https://github.com/withcoral/coral/issues/448)) ([25bac74](https://github.com/withcoral/coral/commit/25bac749b731b4c872f149d668a2352ae67e4be3))
* **app:** add OAuth authorization-code credential flow ([#377](https://github.com/withcoral/coral/issues/377)) ([3dd4dcc](https://github.com/withcoral/coral/commit/3dd4dcc6eb11a072a5b7e28ef9010e4c1dbe61df))
* **app:** add SQL explain API ([#305](https://github.com/withcoral/coral/issues/305)) ([f6fc6e0](https://github.com/withcoral/coral/commit/f6fc6e0d3d2e5b8b009dc6fe9a4207a75766a5ea))
* **app:** add trace storage and inspection API ([#275](https://github.com/withcoral/coral/issues/275)) ([39ac1a8](https://github.com/withcoral/coral/commit/39ac1a8154f403d7e778dfc0817d568dc668d5d0))
* **catalog:** expose unified catalog discovery ([#459](https://github.com/withcoral/coral/issues/459)) ([72677f6](https://github.com/withcoral/coral/commit/72677f641c5517440c4e094d53c37382a8c5b842))
* **cli:** collect OAuth source credentials ([#378](https://github.com/withcoral/coral/issues/378)) ([9a8b087](https://github.com/withcoral/coral/commit/9a8b0875f9a5e1a8a9949b5b08cd0cb140a78d5f))
* **cli:** friendly not-found error for `coral source remove` ([#209](https://github.com/withcoral/coral/issues/209)) ([34e64ce](https://github.com/withcoral/coral/commit/34e64ce51143c61b1c063959f21e45ac16dd28e3))
* **docs:** autogenerate changelog page from CHANGELOG.md ([#232](https://github.com/withcoral/coral/issues/232)) ([ef79160](https://github.com/withcoral/coral/commit/ef791604f27a399dfb9e30ba61e6b9a747ba3c56))
* **engine:** execute Notion search as table function ([#352](https://github.com/withcoral/coral/issues/352)) ([37e537b](https://github.com/withcoral/coral/commit/37e537b69c7d3acd9bff83d9f81c66f4226fea51))
* **engine:** expose search metadata catalog ([#312](https://github.com/withcoral/coral/issues/312)) ([890232b](https://github.com/withcoral/coral/commit/890232bd3a77f667fcf062b7171cffe1f207e736))
* **github:** add starred_at to stargazers table ([#421](https://github.com/withcoral/coral/issues/421)) ([a6372c9](https://github.com/withcoral/coral/commit/a6372c9ce98ba6de8890999517fe8f563d527fe3))
* **sources/community/airflow:** add Apache Airflow community source ([#511](https://github.com/withcoral/coral/issues/511)) ([f9f0272](https://github.com/withcoral/coral/commit/f9f0272f6c00c785634e9d4d4df5df8fb7c60820))
* **sources/community/beehiiv:** add Beehiiv community source ([#569](https://github.com/withcoral/coral/issues/569)) ([91d8769](https://github.com/withcoral/coral/commit/91d8769f937599323b76d84b5b284f260457cfd9))
* **sources/community/betterstack:** add Better Stack community source ([#505](https://github.com/withcoral/coral/issues/505)) ([17aea49](https://github.com/withcoral/coral/commit/17aea49eed5c7639cd0a38153ab04475b4181bc5))
* **sources/community/cal:** add Cal.com community source ([#383](https://github.com/withcoral/coral/issues/383)) ([7b2740d](https://github.com/withcoral/coral/commit/7b2740ddd9ab609fe56aa7de054dfb370b2a25cd))
* **sources/community/clear_ml:** add ClearML API source ([#554](https://github.com/withcoral/coral/issues/554)) ([b361196](https://github.com/withcoral/coral/commit/b361196dbcebcbd51dc86fc04d34c591319e0f43))
* **sources/community/clerk:** add Clerk community source ([#483](https://github.com/withcoral/coral/issues/483)) ([a745c28](https://github.com/withcoral/coral/commit/a745c28c34a4932ffeffabf732fa0474fcace6eb))
* **sources/community/clickhouse:** add clickhouse community source spec ([#410](https://github.com/withcoral/coral/issues/410)) ([4b33266](https://github.com/withcoral/coral/commit/4b332662cefd60247cff614102c0845d227f2fd1))
* **sources/community/cloudflare:** add cloudflare community source ([#338](https://github.com/withcoral/coral/issues/338)) ([7f235ba](https://github.com/withcoral/coral/commit/7f235ba349d87b1d6fd5e4bdd66aad606a1a0f9d))
* **sources/community/codecov:** add Codecov community source ([#503](https://github.com/withcoral/coral/issues/503)) ([a514bc5](https://github.com/withcoral/coral/commit/a514bc5c6ea676b50cd24f1b88fdb99d17a786f1))
* **sources/community/coralogix:** add Coralogix source spec ([#495](https://github.com/withcoral/coral/issues/495)) ([7ba757e](https://github.com/withcoral/coral/commit/7ba757ec60b5989153620ce804840128a96d562d))
* **sources/community/crates_io:** add crates.io community source ([#452](https://github.com/withcoral/coral/issues/452)) ([461eb3f](https://github.com/withcoral/coral/commit/461eb3fa573e989fa7b8a8266f9b4ac3d025b0bd))
* **sources/community/databricks:** add Databricks source ([#529](https://github.com/withcoral/coral/issues/529)) ([9d7cdd9](https://github.com/withcoral/coral/commit/9d7cdd99e767edf4aea56251facec964c1b4eedc))
* **sources/community/datahub:** add DataHub community source  ([#478](https://github.com/withcoral/coral/issues/478)) ([2062385](https://github.com/withcoral/coral/commit/20623852b02a3f6fd81a42e3613b4966221b6582))
* **sources/community/digitalocean:** add DigitalOcean community source ([#487](https://github.com/withcoral/coral/issues/487)) ([ff67d86](https://github.com/withcoral/coral/commit/ff67d86fc90fa6491a6d2e1f7d68acf06e583d33))
* **sources/community/elasticsearch:** add Elasticsearch community source ([#518](https://github.com/withcoral/coral/issues/518)) ([76c9b83](https://github.com/withcoral/coral/commit/76c9b831b8eb7caafc5ef2731c9a822a6938d478))
* **sources/community/figma:** add Figma source ([#537](https://github.com/withcoral/coral/issues/537)) ([6f84704](https://github.com/withcoral/coral/commit/6f84704bb65fc25f4ff4078ff1ef1b561752ca5c))
* **sources/community/fly:** add Fly.io community source ([#467](https://github.com/withcoral/coral/issues/467)) ([d934a8c](https://github.com/withcoral/coral/commit/d934a8c42627e7ec008f9c96c6bf736bd039618f))
* **sources/community/honeycomb:** add Honeycomb community source ([#463](https://github.com/withcoral/coral/issues/463)) ([48f9b9c](https://github.com/withcoral/coral/commit/48f9b9c5c07df3e0a10f2b223840601e81ec3bcf))
* **sources/community/hubspot:** add hubspot community source ([#489](https://github.com/withcoral/coral/issues/489)) ([bb8cc5b](https://github.com/withcoral/coral/commit/bb8cc5b9dff09a2a6acaaf65d95db15cd8885c30))
* **sources/community/huggingface:** add Hugging Face community source ([#465](https://github.com/withcoral/coral/issues/465)) ([b595c91](https://github.com/withcoral/coral/commit/b595c915a012e61fbcf1d24a449f1180fdd50925))
* **sources/community/jenkins:** add jenkins community source ([#472](https://github.com/withcoral/coral/issues/472)) ([e2fc8c2](https://github.com/withcoral/coral/commit/e2fc8c27bca5a4e0db18cc65388dde107daf46b0))
* **sources/community/kafka:** add Apache Kafka source ([#550](https://github.com/withcoral/coral/issues/550)) ([833536f](https://github.com/withcoral/coral/commit/833536f48f5aebf94ca0d4c979f2e8b476fdb06e))
* **sources/community/kestra:** add Kestra community source ([#468](https://github.com/withcoral/coral/issues/468)) ([5aeb25c](https://github.com/withcoral/coral/commit/5aeb25c0dfb755bf64d46e27d3ef6356c4421240))
* **sources/community/langfuse:** add langfuse community source ([#371](https://github.com/withcoral/coral/issues/371)) ([b16c265](https://github.com/withcoral/coral/commit/b16c2654b98b38ec8b19192a9bbddc950142e89d))
* **sources/community/langsmith:** add LangSmith source spec ([#494](https://github.com/withcoral/coral/issues/494)) ([8a66b1c](https://github.com/withcoral/coral/commit/8a66b1c8cb446a4c44e118e8a74ed28cf8b1ea75))
* **sources/community/loops:** add Loops community source ([#568](https://github.com/withcoral/coral/issues/568)) ([66baff5](https://github.com/withcoral/coral/commit/66baff513378b73bf68d39264cc0911de648feaf))
* **sources/community/mailgun:** add Mailgun community source ([#497](https://github.com/withcoral/coral/issues/497)) ([a0b1192](https://github.com/withcoral/coral/commit/a0b11925bae92cfa984284ca5d270d6bda48f2db))
* **sources/community/milvus:** add Milvus community source ([#541](https://github.com/withcoral/coral/issues/541)) ([ae8d071](https://github.com/withcoral/coral/commit/ae8d0713f4bf4d96cad697808a7dfa9596e1752e))
* **sources/community/mixpanel:** add Mixpanel source ([#548](https://github.com/withcoral/coral/issues/548)) ([f66b194](https://github.com/withcoral/coral/commit/f66b194c87c2394cc54cb3c92fd1a24bcf8402a4))
* **sources/community/neondb:** add NeonDB API source ([#477](https://github.com/withcoral/coral/issues/477)) ([fa18a00](https://github.com/withcoral/coral/commit/fa18a00f54666e51386ae7d6b9a2d3fa592c0aa6))
* **sources/community/netlify:** add Netlify source ([#388](https://github.com/withcoral/coral/issues/388)) ([07b7df3](https://github.com/withcoral/coral/commit/07b7df3632ebb107e694303e61c929163db66422))
* **sources/community/okta:** add Okta source spec ([#526](https://github.com/withcoral/coral/issues/526)) ([93ed8ea](https://github.com/withcoral/coral/commit/93ed8ea2acdbc5e67564604ca6c29900605164bc))
* **sources/community/opsgenie:** add Opsgenie source spec ([#531](https://github.com/withcoral/coral/issues/531)) ([3233818](https://github.com/withcoral/coral/commit/32338188aeeb1e8e23042ef9d89436d0fb93fe89))
* **sources/community/postman:** add Postman API source ([#440](https://github.com/withcoral/coral/issues/440)) ([4beb1db](https://github.com/withcoral/coral/commit/4beb1db0514a61ae2a31be8b6578cede438dcaf4))
* **sources/community/postmark:** add postmark community source ([#545](https://github.com/withcoral/coral/issues/545)) ([174f511](https://github.com/withcoral/coral/commit/174f511425e275f70b7f2a0db7f5cd0445c664c1))
* **sources/community/prefect:** added prefect source ([#521](https://github.com/withcoral/coral/issues/521)) ([582464c](https://github.com/withcoral/coral/commit/582464c6a02b0590005c0f6c1ae938dbcfba01dc))
* **sources/community/qdrant_cloud:** add Qdrant Cloud community source ([#413](https://github.com/withcoral/coral/issues/413)) ([8c4dfc0](https://github.com/withcoral/coral/commit/8c4dfc0b0930ddde5ee5f033af6bd9b93f558f68))
* **sources/community/rabbitmq:** add RabbitMQ community source ([#538](https://github.com/withcoral/coral/issues/538)) ([2daa6ee](https://github.com/withcoral/coral/commit/2daa6eead2c08d1b706497dae123cf6573c9753f))
* **sources/community/resend:** add Resend community source ([#500](https://github.com/withcoral/coral/issues/500)) ([2fc50cc](https://github.com/withcoral/coral/commit/2fc50cc8926e544547367317e17fb3266f253084))
* **sources/community/sendgrid:** add SendGrid community source ([#533](https://github.com/withcoral/coral/issues/533)) ([4ccb171](https://github.com/withcoral/coral/commit/4ccb171923a97f8ad416a034166ffee4523224c7))
* **sources/community/shopify:** add shopify community source ([#525](https://github.com/withcoral/coral/issues/525)) ([44c7f84](https://github.com/withcoral/coral/commit/44c7f8451b3fe965ac0911013c4cc7e4dc160f65))
* **sources/community/signoz:** add SigNoz community source ([#516](https://github.com/withcoral/coral/issues/516)) ([fe77454](https://github.com/withcoral/coral/commit/fe774542346ec5eca7635e9f5c13a268f0e31674))
* **sources/community/splunk:** add Splunk source spec ([#510](https://github.com/withcoral/coral/issues/510)) ([e715406](https://github.com/withcoral/coral/commit/e715406c8569d2e2a3e158debcfcd443bade9ce5))
* **sources/community/tailscale:** add Tailscale source spec ([#491](https://github.com/withcoral/coral/issues/491)) ([d6f2486](https://github.com/withcoral/coral/commit/d6f248632b68e422b379939978eaa7a8a770592e))
* **sources/community/todoist:** add Todoist community source ([#441](https://github.com/withcoral/coral/issues/441)) ([cf81147](https://github.com/withcoral/coral/commit/cf8114745894e1d9d99997562fca91785cc5037c))
* **sources/community/travis_ci:** add Travis CI source ([#546](https://github.com/withcoral/coral/issues/546)) ([2738eea](https://github.com/withcoral/coral/commit/2738eea331d61c4f18c45f89535a3a1b7e3268b8))
* **sources/community/trino:** add Trino community source ([#549](https://github.com/withcoral/coral/issues/549)) ([e2ec0f4](https://github.com/withcoral/coral/commit/e2ec0f4c2f8bee2bfb2c5a75218f897ef3572c35))
* **sources/community/umami_cloud:** add Umami Cloud community source ([#417](https://github.com/withcoral/coral/issues/417)) ([897efe9](https://github.com/withcoral/coral/commit/897efe99366c6da5fa10dde2fc582c71093c2bcb))
* **sources/community/upstash:** add Upstash community source ([#480](https://github.com/withcoral/coral/issues/480)) ([31d4985](https://github.com/withcoral/coral/commit/31d4985fdf9a9b90fb4094d922aa9d80eb103496))
* **sources/community/weaviate:** add Weaviate community source ([#574](https://github.com/withcoral/coral/issues/574)) ([cbac432](https://github.com/withcoral/coral/commit/cbac432dfda5ad058b22f8e1effc44d39da8d281))
* **sources/community/zulip:** add zulip community source ([#535](https://github.com/withcoral/coral/issues/535)) ([144ef46](https://github.com/withcoral/coral/commit/144ef46a905dac050b6a0aaa6620992ee4b6df74))
* **sources/datadog:** convert apm dependencies to function ([#404](https://github.com/withcoral/coral/issues/404)) ([b862ccb](https://github.com/withcoral/coral/commit/b862ccbbfd88d15d71d51549e02970a8d721c978))
* **sources/linear:** convert required filters to functions ([#401](https://github.com/withcoral/coral/issues/401)) ([31d8b3e](https://github.com/withcoral/coral/commit/31d8b3e2f71452572ada343a5c867483997ffd36))
* **sources/osv:** add OSV vulnerability database source ([#337](https://github.com/withcoral/coral/issues/337)) ([609bfba](https://github.com/withcoral/coral/commit/609bfbaa9bc9498877b080375e8c31edbfb948f6))
* **sources/slack:** convert message tables to functions ([#405](https://github.com/withcoral/coral/issues/405)) ([aff4fb2](https://github.com/withcoral/coral/commit/aff4fb2445f4b195b20614cdbee7ce25f1ff40a0))
* **sources:** add arg split function value sources ([#395](https://github.com/withcoral/coral/issues/395)) ([dc5e1cc](https://github.com/withcoral/coral/commit/dc5e1cc125309b847614891646d739bac6d9aeec))
* **spec:** add search metadata hints ([#311](https://github.com/withcoral/coral/issues/311)) ([919001d](https://github.com/withcoral/coral/commit/919001d321835f9c2c36157baf4fd7fbfab2bb9c))
* **spec:** harden search metadata validation ([#350](https://github.com/withcoral/coral/issues/350)) ([c77ee14](https://github.com/withcoral/coral/commit/c77ee14e0f72c901e7b026544c981dc51e2a7acb))
* **spec:** model OAuth credential methods ([#343](https://github.com/withcoral/coral/issues/343)) ([29631fb](https://github.com/withcoral/coral/commit/29631fb48f08b0e0d3243b31c06841b92d7354fe))
* **spec:** remove search filter mode ([#610](https://github.com/withcoral/coral/issues/610)) ([d2e6489](https://github.com/withcoral/coral/commit/d2e6489ee00356a598ce59f2452d13a79a7783ef))
* **spec:** support random OAuth redirect ports ([#379](https://github.com/withcoral/coral/issues/379)) ([7879c28](https://github.com/withcoral/coral/commit/7879c284600d9c9702d668a42ae61d9b0758e789))
* **ui:** add minimal Coral shell + WAX design system ([#274](https://github.com/withcoral/coral/issues/274)) ([7b5b8ce](https://github.com/withcoral/coral/commit/7b5b8ceada4defc40f4e55d0f4bb2c6755e35a44))
* **ui:** add resizable trace span detail panel ([#570](https://github.com/withcoral/coral/issues/570)) ([5852382](https://github.com/withcoral/coral/commit/585238226b5bd4e88e9942ca10f94dbb8214bd16))
* **ui:** add trace query stream ([#319](https://github.com/withcoral/coral/issues/319)) ([1a52c79](https://github.com/withcoral/coral/commit/1a52c7908e467e4159c567eb57721252b5db9d6d))
* **ui:** improve trace body viewer ([dc795aa](https://github.com/withcoral/coral/commit/dc795aad897beb6fefd1beb88416a09e58af38b1))
* **ui:** improve trace body viewer ([#639](https://github.com/withcoral/coral/issues/639)) ([dc795aa](https://github.com/withcoral/coral/commit/dc795aad897beb6fefd1beb88416a09e58af38b1))
* **ui:** port custom icons from monorepo ([#650](https://github.com/withcoral/coral/issues/650)) ([09a07f1](https://github.com/withcoral/coral/commit/09a07f1d122a588474b3316b2c79dd14fefa2c04))
* **ui:** refresh wax typography and colors ([#655](https://github.com/withcoral/coral/issues/655)) ([7638fc3](https://github.com/withcoral/coral/commit/7638fc3dab09898907bd6d76380090e43b9f48b2))


### Bug Fixes

* **ci:** avoid exposing skills write token during export ([#592](https://github.com/withcoral/coral/issues/592)) ([e6a18a7](https://github.com/withcoral/coral/commit/e6a18a7e407760374731f32dbfc40805d2fb60b8))
* **ci:** avoid persisted checkout credentials in release-please ([#593](https://github.com/withcoral/coral/issues/593)) ([0207f57](https://github.com/withcoral/coral/commit/0207f57b8dd5f88ae8106e71df49a1861002faf6))
* **engine:** resolve Windows file URLs for JSONL sources ([#662](https://github.com/withcoral/coral/issues/662)) ([c741976](https://github.com/withcoral/coral/commit/c741976d5d29986a3c5ff97a45cde0dac13c28f6))
* revert "feat(sources/community/google_calendar): add Google Calendar community source" ([#608](https://github.com/withcoral/coral/issues/608)) ([2be07e8](https://github.com/withcoral/coral/commit/2be07e83c587428de38efa65eb09e63dd9ac1bc8))
* **sources/community/weaviate:** store API key as secret ([#594](https://github.com/withcoral/coral/issues/594)) ([5d0c339](https://github.com/withcoral/coral/commit/5d0c33932e8d3906e633996ad1b5e3707e982990))
* **ui:** improve trace waterfall scanability ([#648](https://github.com/withcoral/coral/issues/648)) ([b75a0eb](https://github.com/withcoral/coral/commit/b75a0eb7b942bc68d0a48f589866098387c7493d))
* **ui:** tweak query stream search bar ([#649](https://github.com/withcoral/coral/issues/649)) ([1bb7c4d](https://github.com/withcoral/coral/commit/1bb7c4d4bec1e808ca868a577a8d81d91e9c8f7b))

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

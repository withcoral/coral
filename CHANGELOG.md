# Changelog

## [1.4.0](https://github.com/withcoral/coral/compare/v1.3.0...v1.4.0) (2026-10-29)


### Features

* **desktop:** restart to update from the sidebar notice ([#2725](https://github.com/withcoral/coral/issues/2725)) ([cad7792](https://github.com/withcoral/coral/commit/cad7792e896619ba0a62fbb123a2f42c1ef88c65))
* **mcp:** serve a fixed /mcp endpoint that takes its workspace from sign-in ([#2664](https://github.com/withcoral/coral/issues/2664)) ([cf830db](https://github.com/withcoral/coral/commit/cf830dbd3dddc6116ac432675dda21ca9e37f521))
* **mcp:** trace MCP and auth requests and return the trace ID on tool failures ([#2716](https://github.com/withcoral/coral/issues/2716)) ([7dac856](https://github.com/withcoral/coral/commit/7dac8562e4c4a92b741d74b44169b3e11e507eee))
* **provider:** let Coral decide which routes fit a source and when Connect can start ([#2652](https://github.com/withcoral/coral/issues/2652)) ([ce3975b](https://github.com/withcoral/coral/commit/ce3975b9005fa819f9041b50238d810c61e6ef54))
* **provider:** report each provider's callRoot and callable operations in listProviders ([#2643](https://github.com/withcoral/coral/issues/2643)) ([9eda089](https://github.com/withcoral/coral/commit/9eda089211ee1b6dedcaf77ca13dc74886c9db6e))
* **recipes:** add the Keenable recipe ([#2651](https://github.com/withcoral/coral/issues/2651)) ([1afb93f](https://github.com/withcoral/coral/commit/1afb93f8a910924c15151a144fa96a2e89584b33))
* rename operations to provider tools ([#2702](https://github.com/withcoral/coral/issues/2702)) ([041653c](https://github.com/withcoral/coral/commit/041653c85b7a3617b2597db329a886bd2df09f78))
* **run:** apply a Run's waiting Run Inbox requests when recovery ends it ([#2708](https://github.com/withcoral/coral/issues/2708)) ([8b69a0a](https://github.com/withcoral/coral/commit/8b69a0aa647b06979a170f90b3e9609b3d8cc2a8))
* **run:** change a provider's operation policy with coral.workspace.updatePolicy ([#2667](https://github.com/withcoral/coral/issues/2667)) ([d7f6ee8](https://github.com/withcoral/coral/commit/d7f6ee827d161d895ed21b35b1147e6992c95e7d))
* **run:** give each Program Run a Coral-owned trace ([#2723](https://github.com/withcoral/coral/issues/2723)) ([913a3e8](https://github.com/withcoral/coral/commit/913a3e8aa8916858e0a01f46eea0708195df8993))
* **run:** let approval decisions and MCP prompt answers reach the Run Owner from any Coral Instance ([#2695](https://github.com/withcoral/coral/issues/2695)) ([0134e7a](https://github.com/withcoral/coral/commit/0134e7a4c4805f9661a758f7b49eb3e8760ae267))
* **run:** let cancellations reach the Run Owner from any Coral Instance ([#2683](https://github.com/withcoral/coral/issues/2683)) ([ed45afc](https://github.com/withcoral/coral/commit/ed45afc0ddbddc164a57a31f9d56a2b58c67d88f))
* **run:** read a provider's configuration with coral.workspace.getProvider ([#2598](https://github.com/withcoral/coral/issues/2598)) ([37f37a5](https://github.com/withcoral/coral/commit/37f37a50b254b640d2dab6689ebf94d0515cfaaf))
* **run:** read a provider's operation policy with coral.workspace.getPolicy ([#2661](https://github.com/withcoral/coral/issues/2661)) ([ac517ab](https://github.com/withcoral/coral/commit/ac517abdb6b1aff8fa144695756e5717111f8a65))
* **run:** record which Coral Instance owns each Run ([#2680](https://github.com/withcoral/coral/issues/2680)) ([0d3f43a](https://github.com/withcoral/coral/commit/0d3f43ad38507d173eaa5c42775b8fbd7bced4ca))
* **run:** rename coral.workspace.createProvider to addProvider ([#2648](https://github.com/withcoral/coral/issues/2648)) ([191d095](https://github.com/withcoral/coral/commit/191d09524c10512e2a36bc31a5f2dbc13bf322f2))
* **ui:** add a Brand label and keep password managers out of autocomplete-off fields ([#2718](https://github.com/withcoral/coral/issues/2718)) ([c05bf83](https://github.com/withcoral/coral/commit/c05bf83290dee5bb89f960141d061376d6ae45ff))
* **ui:** add namespace tabs and collapsible groups to the functions table ([#2713](https://github.com/withcoral/coral/issues/2713)) ([ec2824d](https://github.com/withcoral/coral/commit/ec2824d9042f9feb82827c8ec678f9aae4e4a6b1))
* **ui:** dock activity detail beside the table ([#2704](https://github.com/withcoral/coral/issues/2704)) ([79f68bf](https://github.com/withcoral/coral/commit/79f68bfde43102173bb170c0e32241c0241830e7))
* **ui:** gather help links into one sidebar menu ([#2714](https://github.com/withcoral/coral/issues/2714)) ([daba1a0](https://github.com/withcoral/coral/commit/daba1a0cc619c3e42555a765359ce7dea89f85f1))
* **ui:** make the namespace follow the display name until you type one ([#2694](https://github.com/withcoral/coral/issues/2694)) ([ca4b3e4](https://github.com/withcoral/coral/commit/ca4b3e40fb1201a65f1d13122898173455ae7687))
* **ui:** offer the catalog when the provider filter matches nothing ([#2687](https://github.com/withcoral/coral/issues/2687)) ([fd24c93](https://github.com/withcoral/coral/commit/fd24c931185821089263310258b97012361f3e46))
* **ui:** show organization owners a newer Coral release in the sidebar ([#2707](https://github.com/withcoral/coral/issues/2707)) ([f62f69d](https://github.com/withcoral/coral/commit/f62f69d29bb16df7ecf4b16ede95945281aeb8f2))
* **ui:** show progress while a cancel or decision is being recorded ([#2706](https://github.com/withcoral/coral/issues/2706)) ([3604b29](https://github.com/withcoral/coral/commit/3604b29cf51d2b802f41d736b5e76db8a0a878cc))
* **ui:** show the Overview summary as calls, needs you and context ([#2628](https://github.com/withcoral/coral/issues/2628)) ([a788ba3](https://github.com/withcoral/coral/commit/a788ba396b921e6430dfd8aa2b33454396e95bd9))
* **ui:** show who called each tool in workspace activity ([#2722](https://github.com/withcoral/coral/issues/2722)) ([aca342d](https://github.com/withcoral/coral/commit/aca342d8e45c5c3c274b26b6246c77d9813f8348))
* **ui:** title each browser tab with the page's breadcrumb trail ([#2649](https://github.com/withcoral/coral/issues/2649)) ([6bb6918](https://github.com/withcoral/coral/commit/6bb69181ef9ba2e2559126ca761b0b3e26ecee6e))


### Bug Fixes

* **db:** renumber the Run Inbox migration to 0043 ([#2691](https://github.com/withcoral/coral/issues/2691)) ([1e13b83](https://github.com/withcoral/coral/commit/1e13b839ce6d53b3b6e32eada238897a26aa277e))
* **desktop:** update Electron and UI test tooling past security advisories ([#2678](https://github.com/withcoral/coral/issues/2678)) ([ea9a5a5](https://github.com/withcoral/coral/commit/ea9a5a5c4d8110e1b09265a1dd89ebdca4742b98))
* **provider:** offer Connect for a typed credential held during discovery, and say where replacement credentials go ([#2653](https://github.com/withcoral/coral/issues/2653)) ([cde6ddc](https://github.com/withcoral/coral/commit/cde6ddc3ee2b0da12655d61902069109db2e5605))
* **provider:** tell an agent to call addProvider again after provider_publication_failed ([#2654](https://github.com/withcoral/coral/issues/2654)) ([f13750d](https://github.com/withcoral/coral/commit/f13750d9242310cfe78185fdedebd154a50cf573))
* **ui:** advance Activity Live after the followed run ends ([#2618](https://github.com/withcoral/coral/issues/2618)) ([919ed54](https://github.com/withcoral/coral/commit/919ed54ff733152b88a110f127e91df5c3b0a799))
* **ui:** connect your account in a dialog and remove the operation policy page ([#2647](https://github.com/withcoral/coral/issues/2647)) ([3bf6fd6](https://github.com/withcoral/coral/commit/3bf6fd638772aac77d155c2d842486dd9e2869e5))
* **ui:** fade scroll areas into whatever background is behind them ([#2720](https://github.com/withcoral/coral/issues/2720)) ([09aa0be](https://github.com/withcoral/coral/commit/09aa0beebdaff66cf6267509b1fae0e66b6a3a8d))
* **ui:** show the members-only info tip beside the page title ([#2658](https://github.com/withcoral/coral/issues/2658)) ([d705a2c](https://github.com/withcoral/coral/commit/d705a2ceddb96310703f40ef74cc1088c2a82eda))
* **ui:** show the white coral mark in dark browser tabs ([#2696](https://github.com/withcoral/coral/issues/2696)) ([0e2d89a](https://github.com/withcoral/coral/commit/0e2d89ae005d720fdc0c7143b13b264585be36d2))
* **ui:** use the website's coral mark for the browser tab and app icons ([#2690](https://github.com/withcoral/coral/issues/2690)) ([27ccfd0](https://github.com/withcoral/coral/commit/27ccfd0206ca360b9e65b75950ee913344afe8e8))

## [1.3.0](https://github.com/withcoral/coral/compare/v1.2.0...v1.3.0) (2026-10-23)


### Features

* animate Activity detail navigation ([#2581](https://github.com/withcoral/coral/issues/2581)) ([c49a708](https://github.com/withcoral/coral/commit/c49a70809c3105237cfeee29839d8a9f8bea5272))
* **auth:** finish OAuth sign-ins on any Coral process ([#2587](https://github.com/withcoral/coral/issues/2587)) ([62d022c](https://github.com/withcoral/coral/commit/62d022c9e0e28503cfdc5a7d1ae952e72d0d7f16))
* follow live activity in Activity ([#2568](https://github.com/withcoral/coral/issues/2568)) ([5e42272](https://github.com/withcoral/coral/commit/5e4227239937d572ff88e63f19d7ceae604f1a1f))
* **provider:** let members and agents call coral.workspace.listProviders ([#2616](https://github.com/withcoral/coral/issues/2616)) ([4d18a62](https://github.com/withcoral/coral/commit/4d18a6220625fdc403743ad096c7338dab9b5daf))
* **run:** move Run, approval and connection deadlines into Run Owner timers ([#2592](https://github.com/withcoral/coral/issues/2592)) ([cebb89f](https://github.com/withcoral/coral/commit/cebb89fda84daeaf7846b9d4d2635d6c13329297))
* **run:** recover Runs with an expired-lease sweep and drain them on shutdown ([#2614](https://github.com/withcoral/coral/issues/2614)) ([3fb00ae](https://github.com/withcoral/coral/commit/3fb00ae08b0691394bf4c84ded1067cbb871af42))
* **ui:** fill the agent page with its accounts and recent activity ([#2617](https://github.com/withcoral/coral/issues/2617)) ([44d0815](https://github.com/withcoral/coral/commit/44d0815bffe2be839a94b4d09a24b0fff672e709))
* **ui:** move Add members and Create agent to their toolbars ([#2611](https://github.com/withcoral/coral/issues/2611)) ([22f2ea5](https://github.com/withcoral/coral/commit/22f2ea58dd3d6a933ef6dc94da0b1d12332366c6))
* **ui:** show an agent's accounts and recent activity on its page ([#2586](https://github.com/withcoral/coral/issues/2586)) ([e0c4f90](https://github.com/withcoral/coral/commit/e0c4f905a5cf26c97e55fb74730d29220599f808))


### Bug Fixes

* **db:** renumber the OAuth authorization codes migration to 0040 ([#2622](https://github.com/withcoral/coral/issues/2622)) ([787736d](https://github.com/withcoral/coral/commit/787736dcf264a7c1bd7f69c3f5650d662eb397a8))
* **release:** show release notes on the Desktop feed and container packages ([#2604](https://github.com/withcoral/coral/issues/2604)) ([d558c2f](https://github.com/withcoral/coral/commit/d558c2f56becca1d63a43e2b66b3a59fe062519b))
* **ui:** count search and describe context in the Overview summary ([#2607](https://github.com/withcoral/coral/issues/2607)) ([c7f6bb5](https://github.com/withcoral/coral/commit/c7f6bb519f8d032d428509e95d424ebf80063568))
* **ui:** preserve the final-value row during activity navigation ([#2621](https://github.com/withcoral/coral/issues/2621)) ([edbd6f8](https://github.com/withcoral/coral/commit/edbd6f8cade490d967f8e8dc4adfd37b6d5efe7c))
* **ui:** show each member's email in the members table ([#2606](https://github.com/withcoral/coral/issues/2606)) ([e42b378](https://github.com/withcoral/coral/commit/e42b37836f91a20899b128cd9f84792887e9be45))
* **ui:** show submit progress with a spinner on every form ([#2583](https://github.com/withcoral/coral/issues/2583)) ([ce15691](https://github.com/withcoral/coral/commit/ce1569120e7cea96fddfab977eec11bbf3aad8f1))


### Performance Improvements

* **run:** wake parked Invocations through the Run Owner instead of polling ([#2609](https://github.com/withcoral/coral/issues/2609)) ([f350b15](https://github.com/withcoral/coral/commit/f350b158b7434589b5ecc70cd93d119e444d43bf))
* **search:** cut peak memory when indexing a large provider ([#2626](https://github.com/withcoral/coral/issues/2626)) ([61bf3bb](https://github.com/withcoral/coral/commit/61bf3bb2354108e801f5019cbd85119dd5894e77))
* **search:** delete SQLite text index rows by rowid, not by scan ([#2605](https://github.com/withcoral/coral/issues/2605)) ([b616163](https://github.com/withcoral/coral/commit/b616163ad57700dc5488eefb4d79697065a2d8fc))

## [1.2.0](https://github.com/withcoral/coral/compare/v1.1.0...v1.2.0) (2026-10-22)


### Features

* **activity:** record authored source locations for calls and outputs ([#2535](https://github.com/withcoral/coral/issues/2535)) ([2d735ca](https://github.com/withcoral/coral/commit/2d735ca98eb3c0edc24f7c6ef3e09cdfc8e91e20))
* **activity:** show persistent Program source beside selected records ([#2569](https://github.com/withcoral/coral/issues/2569)) ([f403819](https://github.com/withcoral/coral/commit/f403819a3de562511f6a4c7bae283f259db0594f))
* add the Claude recipe and read gzipped OpenAPI documents ([#2513](https://github.com/withcoral/coral/issues/2513)) ([07c27a2](https://github.com/withcoral/coral/commit/07c27a29b961e5927174ff876da0c510dd160473))
* **catalog:** share named TypeScript types across a Provider ([#2482](https://github.com/withcoral/coral/issues/2482)) ([256e37d](https://github.com/withcoral/coral/commit/256e37d892d6aef0a4a8518984e5f1748ba1fbad))
* **desktop:** run the runtime on a Postgres database named in desktop.json ([#2485](https://github.com/withcoral/coral/issues/2485)) ([96465a0](https://github.com/withcoral/coral/commit/96465a0864a204d61819edcbbf58fa0373f9837f))
* **functions:** type coral.providers, coral.workspace and coral.functions at Saved Function save ([#2526](https://github.com/withcoral/coral/issues/2526)) ([a465850](https://github.com/withcoral/coral/commit/a4658503d0e9eee8cbdb10476195509f410b3a81))
* **provider:** create the Coral Built-in Provider at coral.workspace in every new Workspace ([#2349](https://github.com/withcoral/coral/issues/2349)) ([06e65ca](https://github.com/withcoral/coral/commit/06e65ca4d7b071a32c4767c0e6cc19c0c5290ece))
* **provider:** store an agent's createProvider request on its Setup ([#2593](https://github.com/withcoral/coral/issues/2593)) ([92847d2](https://github.com/withcoral/coral/commit/92847d2a5304224b2d23a425d77cb838d19ce4fd))
* **provider:** update every Workspace's Built-in Provider catalog at boot ([#2355](https://github.com/withcoral/coral/issues/2355)) ([24615d9](https://github.com/withcoral/coral/commit/24615d980f849b1c8fcd107e3dee79de7a312f1c))
* **reef:** add NextDNS provider recipe ([#2509](https://github.com/withcoral/coral/issues/2509)) ([e44880e](https://github.com/withcoral/coral/commit/e44880e7782dc92703967a147451d5970f9e5c24))
* **reef:** calmer providers table and a flush app shell ([#2462](https://github.com/withcoral/coral/issues/2462)) ([ffe279c](https://github.com/withcoral/coral/commit/ffe279c1be9070ed12b85102cb748c05a7d3e87a))
* **reef:** improve the UI on small screens ([#2531](https://github.com/withcoral/coral/issues/2531)) ([75e020a](https://github.com/withcoral/coral/commit/75e020a2748b8b364845d1d59d4dde6fc183049b))
* **reef:** make the providers rail vertical tabs with an All providers entry ([#2552](https://github.com/withcoral/coral/issues/2552)) ([08d007a](https://github.com/withcoral/coral/commit/08d007a57fbc45ab94f54835f47cb143af67e582))
* **reef:** providers page UI tweaks ([#2562](https://github.com/withcoral/coral/issues/2562)) ([7ca218e](https://github.com/withcoral/coral/commit/7ca218ed615e0f48d07e060f672ce3b1d7a333e8))
* **reef:** put the list pages' main action back in the top bar as a primary button ([#2546](https://github.com/withcoral/coral/issues/2546)) ([761ea6d](https://github.com/withcoral/coral/commit/761ea6d9b7906eca520bca37018dbac2abc1630a))
* **reef:** read ^, $ and * in every filter field ([#2520](https://github.com/withcoral/coral/issues/2520)) ([d322356](https://github.com/withcoral/coral/commit/d322356a90f89de4608a07f05cb24ac253602727))
* **reef:** refuse a provider display name another provider uses ([#2518](https://github.com/withcoral/coral/issues/2518)) ([fdcd473](https://github.com/withcoral/coral/commit/fdcd4731b416f78dd91b9c158af0bca8d93e2c0e))
* **reef:** review provider configuration changes in a dialog ([#2579](https://github.com/withcoral/coral/issues/2579)) ([24417e9](https://github.com/withcoral/coral/commit/24417e91f7f2ddd1d16496a0dc4b32f2738bc595))
* **reef:** show search indexing failures on a Provider and its Operations ([#2504](https://github.com/withcoral/coral/issues/2504)) ([6cffed0](https://github.com/withcoral/coral/commit/6cffed0da46a057c70e9eea13cba733c7f5aa6ef))
* **reef:** show your recent activity summary on the workspace overview ([#2574](https://github.com/withcoral/coral/issues/2574)) ([0d1dba5](https://github.com/withcoral/coral/commit/0d1dba51ba3afc17476d8d2d7a7737b6b42d9e16))
* **reef:** title list pages in their toolbar, with the main action beside it ([#2525](https://github.com/withcoral/coral/issues/2525)) ([fd44b72](https://github.com/withcoral/coral/commit/fd44b725871fa2044cac69d7eaadaa5d6f9db9cd))
* **reef:** UI tweaks to provider's page ([#2566](https://github.com/withcoral/coral/issues/2566)) ([62204ca](https://github.com/withcoral/coral/commit/62204cadd7b9c94c8490b2c3848f9a92b154586d))
* **reef:** unify Activity detail panels and action reviews ([#2544](https://github.com/withcoral/coral/issues/2544)) ([409d062](https://github.com/withcoral/coral/commit/409d062e2e448b5e4a361c626f2a41758dd523b6))
* return only declared successes from Operation calls ([#2517](https://github.com/withcoral/coral/issues/2517)) ([0270bf1](https://github.com/withcoral/coral/commit/0270bf11c7c3ac9fec63072591229ebab6585894))
* **run:** enforce the Fan-out Limit inside the Run Owner ([#2571](https://github.com/withcoral/coral/issues/2571)) ([cb53419](https://github.com/withcoral/coral/commit/cb53419ecc840162a4daff6753fdc8870533d796))
* **run:** introduce the Run Owner for Program requests ([#2524](https://github.com/withcoral/coral/issues/2524)) ([245d19d](https://github.com/withcoral/coral/commit/245d19dd74f0801e9d9fcc876a8d8d4f3bd977cc))
* **run:** let coral.workspace.createProvider configure and publish the Provider itself ([#2352](https://github.com/withcoral/coral/issues/2352)) ([088faaf](https://github.com/withcoral/coral/commit/088faaf18adb0b18bf41d33ea676a4a9f5f5697a))
* **run:** let createProvider configure sign-in and link to Reef's sign-in step ([#2353](https://github.com/withcoral/coral/issues/2353)) ([8d5495c](https://github.com/withcoral/coral/commit/8d5495cb56c15693cec8713445f150b235c0966c))
* **run:** list a workspace's providers with coral.workspace.listProviders ([#2374](https://github.com/withcoral/coral/issues/2374)) ([179c43a](https://github.com/withcoral/coral/commit/179c43ac822138ae9a4116510a55a919b0977e3f))
* **run:** make the Run Owner survive uncertain commits and lost leases ([#2541](https://github.com/withcoral/coral/issues/2541)) ([3a1fad1](https://github.com/withcoral/coral/commit/3a1fad1ad6e654173a84d1a7a924daa32574f8ef))
* **run:** name Tasks and Runs with four-word identifiers ([#2558](https://github.com/withcoral/coral/issues/2558)) ([c82259a](https://github.com/withcoral/coral/commit/c82259a32b6df5d9027853701f2a03b0be786d72))
* **run:** prepare a Provider from a Program with coral.workspace.createProvider ([#2351](https://github.com/withcoral/coral/issues/2351)) ([e7e56fd](https://github.com/withcoral/coral/commit/e7e56fd59ef3d75a74c9c3d86ea46c2ce4164375))
* **run:** reshape the Dispatch Fence into one batched check per chunk commit ([#2550](https://github.com/withcoral/coral/issues/2550)) ([120c9d3](https://github.com/withcoral/coral/commit/120c9d30f8ed1c2020132d9c9711aefc0421b372))
* **run:** route approval decisions and cancellation through the Run Owner ([#2577](https://github.com/withcoral/coral/issues/2577)) ([77bcbf5](https://github.com/withcoral/coral/commit/77bcbf5811fada931c9f312f497aed84e85bc677))
* **run:** summarize your recent activity in a Workspace ([#2573](https://github.com/withcoral/coral/issues/2573)) ([bf4c75e](https://github.com/withcoral/coral/commit/bf4c75e63941a13e730910a41ce6553bca409116))
* **search:** show a Saved Function's result fields in its signature line ([#2522](https://github.com/withcoral/coral/issues/2522)) ([41365f6](https://github.com/withcoral/coral/commit/41365f6fe65b75da28f90c365c0ec04c84aa7c1e))
* **search:** trace each reconcile pass and its steps ([#2456](https://github.com/withcoral/coral/issues/2456)) ([0594094](https://github.com/withcoral/coral/commit/05940948d06b774d33df89afe258996f9c302209))
* **ui:** add option to create a correctly-configured Slack app during Slack provider setup ([#2585](https://github.com/withcoral/coral/issues/2585)) ([8c3a883](https://github.com/withcoral/coral/commit/8c3a8836dce307a953b50f73fda4394d3585dd35))
* **ui:** move Add provider to the providers toolbar ([#2591](https://github.com/withcoral/coral/issues/2591)) ([e1ee4cb](https://github.com/withcoral/coral/commit/e1ee4cbe70ade525468a9404f04aacacafdcfe33))


### Bug Fixes

* **app:** sweep device authorization leases every 15 s ([#2452](https://github.com/withcoral/coral/issues/2452)) ([1a4e4ae](https://github.com/withcoral/coral/commit/1a4e4ae5d05e841862bd7d1910e54148c1b02cce))
* **auth:** drop the unverified client name note from the approval page ([#2578](https://github.com/withcoral/coral/issues/2578)) ([ddf5aa4](https://github.com/withcoral/coral/commit/ddf5aa437ace28b16d0145ec0f8bf5bf205ab3e0))
* **catalog:** derive each Built-in Operation's contract from the types Coral parses ([#2582](https://github.com/withcoral/coral/issues/2582)) ([534e636](https://github.com/withcoral/coral/commit/534e6367cbece73341c78bef2c7ebca7c9572108))
* **catalog:** import OpenAPI Providers whose Operations request different OAuth scopes ([#2401](https://github.com/withcoral/coral/issues/2401)) ([3acfe55](https://github.com/withcoral/coral/commit/3acfe5538a34842a6a088ffdbee3808f8c79702b))
* **catalog:** keep keyword names in Operation Call Paths ([#2506](https://github.com/withcoral/coral/issues/2506)) ([b20795b](https://github.com/withcoral/coral/commit/b20795b2a71e2b5c5ba6b9c617a568e524ffef96))
* **catalog:** never make an Operation unavailable for search material ([#2465](https://github.com/withcoral/coral/issues/2465)) ([f83c909](https://github.com/withcoral/coral/commit/f83c909b667ff99cecb929006c11d768164a9922))
* **catalog:** stop rejecting createProvider request headers by name ([#2589](https://github.com/withcoral/coral/issues/2589)) ([39e54b9](https://github.com/withcoral/coral/commit/39e54b99a99761ca56b076d340836537a3040688))
* **provider:** ask for an OAuth client ID when Coral cannot register one ([#2580](https://github.com/withcoral/coral/issues/2580)) ([baefe9e](https://github.com/withcoral/coral/commit/baefe9ed7f04867fc96c27ee55814167e44274b6))
* **provider:** bind each Setup to its starter and hide Coral from Members ([#2480](https://github.com/withcoral/coral/issues/2480)) ([6011a64](https://github.com/withcoral/coral/commit/6011a6457c4c836c9c90348a7453914aec24ff1e))
* **reef:** align the catalog table with the first rail entry ([#2484](https://github.com/withcoral/coral/issues/2484)) ([fb59758](https://github.com/withcoral/coral/commit/fb597588cddac3bb246515b5f8efcf6546da83c4))
* **reef:** calm the Activity badge and keep it while the stream reconnects ([#2468](https://github.com/withcoral/coral/issues/2468)) ([1b66639](https://github.com/withcoral/coral/commit/1b666397fd671439eac1e009e48c18aba567713d))
* **reef:** frame the providers table again and lower the top bar ([#2472](https://github.com/withcoral/coral/issues/2472)) ([87e41ee](https://github.com/withcoral/coral/commit/87e41eedb5dd96084877ded39c1492d612dd04e5))
* **reef:** pin third-party recipe OpenAPI documents to a commit ([#2510](https://github.com/withcoral/coral/issues/2510)) ([b4e9280](https://github.com/withcoral/coral/commit/b4e9280830590d56bdab4a8104acdc1edde0eab2))
* **reef:** replace loose help text with info tips and banners ([#2461](https://github.com/withcoral/coral/issues/2461)) ([695500a](https://github.com/withcoral/coral/commit/695500a4e6207f1f2dd7cf18f774d276cfe98f49))
* **reef:** round the page's corners beside the sidebar ([#2503](https://github.com/withcoral/coral/issues/2503)) ([a1fef64](https://github.com/withcoral/coral/commit/a1fef6498be0a35ce2493c237241ffa081876041))
* **reef:** shorten OAuth scope chips that share a URL prefix ([#2455](https://github.com/withcoral/coral/issues/2455)) ([5ae67ec](https://github.com/withcoral/coral/commit/5ae67ec6294ac25f5b45b3b91d1c0c99d9878e9e))
* **reef:** suggest a provider's namespace from its document name, not its host ([#2565](https://github.com/withcoral/coral/issues/2565)) ([8d77893](https://github.com/withcoral/coral/commit/8d778932bf1a6518f5fa51f5f9e337086f0c14e2))
* **run:** name every argument to correct in an invalid_input failure ([#2354](https://github.com/withcoral/coral/issues/2354)) ([07bd727](https://github.com/withcoral/coral/commit/07bd727e5bb9ad8a63d93731ac22e8634a3ab09d))
* **run:** reject a credential beside createProvider's bearer method and report unreachable sources as upstream_incomplete ([#2542](https://github.com/withcoral/coral/issues/2542)) ([590eab7](https://github.com/withcoral/coral/commit/590eab758efac10f36ab57444dc54e8052d158b5))
* **run:** report an MCP JSON-RPC rejection as invalid_input or upstream_rejected ([#2402](https://github.com/withcoral/coral/issues/2402)) ([749770b](https://github.com/withcoral/coral/commit/749770b4ccb41d58d7cb9eb633f0035be94a26f7))
* **search:** load each Generation's schema graph once per search ([#2448](https://github.com/withcoral/coral/issues/2448)) ([445d945](https://github.com/withcoral/coral/commit/445d94569105087c42586756c12be1019883c63e))
* **search:** retry a failed Saved Function only after one interval ([#2460](https://github.com/withcoral/coral/issues/2460)) ([a61ab3a](https://github.com/withcoral/coral/commit/a61ab3aa0ed6073a810d46de33dd68ace382a546))
* **search:** return no hits for a query without a letter or digit ([#2561](https://github.com/withcoral/coral/issues/2561)) ([bb0a4f0](https://github.com/withcoral/coral/commit/bb0a4f0e96599089a349f47e02f70760858d25ad))
* **search:** start the write limit at the lock and build rows before the write ([#2575](https://github.com/withcoral/coral/issues/2575)) ([06ad55d](https://github.com/withcoral/coral/commit/06ad55df2bbe98814088ab8bad65c9e6b9593b8d))
* **search:** stop a self-referential Promise result from overflowing the stack ([#2543](https://github.com/withcoral/coral/issues/2543)) ([1d4f257](https://github.com/withcoral/coral/commit/1d4f2573ac67caa3d0a17314984a09edd9dcd6a0))
* **search:** stop retrying an index write that fails the same way every time ([#2548](https://github.com/withcoral/coral/issues/2548)) ([e3b6d15](https://github.com/withcoral/coral/commit/e3b6d15f708ead860083a541811f1491dc4a01bc))


### Performance Improvements

* **catalog:** dedup credential destinations once per build, not per Operation ([#2555](https://github.com/withcoral/coral/issues/2555)) ([0afbcf5](https://github.com/withcoral/coral/commit/0afbcf5742b9ea071482ef7e582beb9a38bd688d))
* **catalog:** project each named TypeScript type once per Provider ([#2443](https://github.com/withcoral/coral/issues/2443)) ([153c273](https://github.com/withcoral/coral/commit/153c273c03cd90ab3daeb1f7e61cb3d2f1ddf7f0))
* **search:** drop schema text from the phrase test ([#2559](https://github.com/withcoral/coral/issues/2559)) ([cfdbafa](https://github.com/withcoral/coral/commit/cfdbafacfd0ebb8d157ae63a303dc63ba15c8aa3))
* **search:** index each word of schema text once ([#2547](https://github.com/withcoral/coral/issues/2547)) ([d1123fb](https://github.com/withcoral/coral/commit/d1123fb799bb43319ce026af83dccfbe78d5460c))
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

## Result

Wax can ship as an npm package of TypeScript source. A scratch Vite app and the console both build and test with one tarball, made from `main` at `0bfe33ac3`. Nothing was published and Reef did not change.

**Recommendation: package now.** The package needs no build step, one plugin line in each Vite config, and no change to Reef's 381 imports of Wax.

## What ran

The tarball holds 540 files (427 kB packed): `app/wax`, the helpers it imports, the fonts, the stories and the unit tests.

| Check in the scratch app | Result |
| --- | --- |
| `tsc` | pass |
| `vite build`, and an SSR build that Node then runs | pass |
| Dev server, loaded in Chromium, and Vite's SSR module loader | pass: styles apply, fonts load |
| Vitest, a test that renders Wax | pass |
| The package's own unit tests, run from `node_modules` | 28 files, 239 tests pass |
| `storybook build`, stories read from the package | 67 files, 400 stories |
| `storybook dev` on a cold cache, 6 Wax stories opened in Chromium | pass: styles apply, fonts load |
| Story tests in Chromium | 67 files, 400 tests pass |

The console then took the same tarball in place of its copy. Its `npm run typecheck`, `npm test` and `npm run build-storybook` pass. They also pass on the tarball that question 6 recommends, which has no unit tests (511 files).

Versions in the scratch app: Node 24.18, Vite 8.3.4, Vitest 5.0.5, Storybook 10.6.5, `@vanilla-extract/vite-plugin` 5.2.6, TypeScript 7.0.2, Playwright 1.64.1.

## The errors, and how each one went away

1. **Dev server: `Styles were unable to be assigned to a file` at `theme/layers.css.ts`.** Vite pre-bundles a dependency into one file, so the `.css.ts` files never reach the Vanilla Extract plugin. Fix: `optimizeDeps.exclude: ['@withcoral/wax']`.
2. **Dev server: `'/node_modules/classnames/index.js' does not provide an export named 'default'`.** Vite pre-bundles nothing that an excluded package imports, so a CommonJS dependency reaches the browser as it is. Fix: `optimizeDeps.include: ['@withcoral/wax > classnames']`.
3. **Dev server: `use-sync-external-store/shim/index.js does not provide an export named 'useSyncExternalStore'`.** The same error, one level down, from Base UI. Fix: include each of the 32 specifiers that Wax imports at run time. The package build writes that list into a plain `vite.js`, which exports a plugin. An app adds `wax()` to its `plugins` and writes no list.
4. **An `exports` map, `"./*": "./*"`: `TS2307 Cannot find module '@withcoral/wax/components'`.** TypeScript adds no extension and no `index.ts` to an `exports` target. Fix: no `exports` and no `main`. Reef imports 68 different paths of Wax, and each resolves as an app's own file does.
5. **The `@/wax` alias kept inside the package**: see question 3. It needed three more settings.

These needed no setting: the production build, the SSR build, Vitest and Storybook. Vite and Vitest do not externalize a `.ts` file in `node_modules`, so `ssr.noExternal` and `server.deps.inline` stayed unset. Storybook reads stories from `node_modules` when the glob names it.

## The six questions

### 1. Can Wax ship as source?

Yes, in a dev server, a production build, an SSR build, Vitest and Storybook. Pre-bundling was the only obstacle, and `wax()` removes it. Three things follow from source:

- The app's `tsc` checks Wax's files with the app's own compiler options, and `skipLibCheck` does not skip them. A type error planted in the installed package failed the app's typecheck. An app needs compiler options that Wax passes under.
- `@rolldown/plugin-babel` leaves `node_modules` out by its default `exclude`, so the React Compiler does not compile an installed Wax. Reef runs the compiler only in its Vitest config, so its production build does not compile Wax either.
- Node cannot load the package's `.ts` files itself. Only a bundler can use it.

### 2. What does Wax import from outside itself?

| Import | Decision | Reef's own imports to change |
| --- | --- | --- |
| `@/styles/utils`, `@/styles/breakpoints` | Move into Wax | 5 |
| `@/utils/filter-query` | Move into Wax, with its test | 14 |
| `@/utils/compose-refs`, `@/utils/use-truncation-detection` | Move into Wax, the second with its test | 0 |
| `@/utils/count-label`, `@/utils/download` | Move into Wax | 8 |
| `@/test-support/route` | Test only. Stays in Reef | 0 |
| `.storybook/browser` (4 stories import it by a relative path) | Move into Wax | 8 stories |
| `.storybook/addons/theme-comparison` | Move into Wax | `.storybook` only |
| `public/fonts` | Move into Wax | `root.tsx` preload |

- `@/test-support/route` reaches Reef's required-actions stream through `shellSignal`. One Wax test, `breadcrumbs.test.tsx`, uses its `markupRoute`. Give that test its own memory router.
- `theme/reset.css` names the fonts by `/fonts/...`, which only an app's `public` folder serves. In the package it names them by `../fonts/...`, and the app's bundler emits them. An app that preloads a font imports its address with `?url`. Both worked in a production build.
- `styles/utils.ts` imports a type from `@vanilla-extract/css` without `import type`. Fix that in the move.

### 3. What about the `@/wax` alias?

Use relative imports inside the package. Outside it, Reef keeps `@/wax/...` and the console writes `@withcoral/wax/...`.

| | Relative inside the package | Alias kept inside the package |
| --- | --- | --- |
| Edits in Wax | 547 imports in 216 files, by script | 33 imports (question 2 only) |
| Edits in Reef | None for Wax. 27 imports and 8 stories for the helpers that move | The same |
| Edits in the console | Its imports of Wax, by `sed` | A `paths` entry and an alias instead |
| Settings in each app | `wax()` | `wax()`, a `paths` entry in `tsconfig.json`, `resolve.alias` in each Vite config, and `optimizeDeps.exclude: ['@/wax']` |
| Trial | Pass | Pass, after the three errors below |

The alias met three errors:

- With no setting, `tsc` gave 214 errors.
- With a `paths` entry, `tsc` passed and Vite failed: `Cannot find package '@/wax/css' imported from node_modules/@withcoral/wax/theme/global.css.ts`. `resolve.tsconfigPaths` does not apply to a file in `node_modules`, so each Vite config needs `resolve.alias`.
- With the alias, the dev server pre-bundled each `@/wax/...` path as a dependency of its own, and error 1 came back. `optimizeDeps.exclude: ['@/wax']` removed it.

With the alias, the package works only in an app that gives it the name `@/wax`. The relative imports are a single scripted change, and the package's 239 unit tests and 399 story tests pass after it.

The counts at `0bfe33ac3`: Wax names `@/wax` 511 times in 208 files, and Reef names it 381 times in 153 files. That counts every specifier, with `.storybook`; the issue counts `from` lines in `app`.

Reef needs no change to those 381 imports if Wax stays in `apps/reef/app/wax` and that folder becomes the package. Its `@/*` alias still reaches it. A move to `packages/wax` is possible later and does not change what is published.

### 4. What are its dependencies?

Ranges come from Reef's `package.json`.

- **`peerDependencies`**, where the app and Wax must share one copy: `react`, `react-dom`, `react-router`, `jotai`, `@vanilla-extract/css`, `@vanilla-extract/vite-plugin`.
- **`dependencies`**: `@base-ui/react`, `@internationalized/date`, `@lezer/highlight`, `@lezer/javascript`, `@lezer/json`, `@tanstack/react-virtual`, `@vanilla-extract/recipes`, `classnames`, `lucide-react`, `react-aria`, `react-aria-components`, `react-markdown`, `react-toastify`, `tinykeys`.
- **Optional peers**, which only the stories and their helpers import: `storybook`, `@storybook/react-vite`, `@storybook/icons`, `react-hook-form`, `vitest`.

Three notes:

- Reef's architecture test already keeps Base UI, react-aria and lucide inside Wax, so they are plain dependencies.
- Reef does not install the package, so a test must keep the two manifests in step: each bare import in Wax is declared, and each range equals Reef's.
- Reef pins `react-router` to `8.5.1` exactly. As a peer, that makes an app move React Router in the same pull request as Wax. Give the peer a range, `^8.5.1`.

The story helper `storybook/browser.ts` imports `@vitest/browser/context`. Reef's `vitest.stories.config.ts` maps that to `vitest/browser`. An app that runs the stories needs the same alias until addon-vitest 11.

### 5. How does it publish?

Publish to npm from a workflow here. pkg.pr.new cannot give the console a pinned version that a bot moves.

| | npm, trusted publishing | pkg.pr.new |
| --- | --- | --- |
| Account | An npm organization for `@withcoral` | None |
| What the console installs | `@withcoral/wax@0.0.0-main.…`, an exact version | A URL with a commit |
| `main` dist-tag | Yes: `npm publish --tag main` | No dist-tags and no versions |
| A bot moves it | Dependabot or Renovate | Neither updates a URL dependency |
| Retention | Permanent | Removed after six months, or one month with no download. No stated guarantee |
| Provenance | Automatic for a public repository | None |

What the npm workflow needs, from npm's documentation today:

- **A GitHub-hosted runner for the publish job.** Trusted publishing does not accept another runner, and every workflow here runs on Blacksmith.
- **The package must exist first.** Create it once by hand, then configure the trusted publisher with the workflow's file name.
- **"Allow npm publish" on the configuration.** A configuration made after 3 September 2026 allows only `npm stage publish`, which waits for a person.
- `permissions: id-token: write`, npm 11.5.1 or later, and `repository.url` equal to this repository.
- `publishConfig: { "access": "public", "tag": "main" }`. npm refuses a prerelease version with no tag.
- A version that sorts in commit order: `0.0.0-main.<UTC timestamp>.g<sha>`. The `g` keeps an all-digit sha with a leading zero valid.

Sources: [trusted publishers](https://docs.npmjs.com/trusted-publishers), [`npm trust`](https://docs.npmjs.com/cli/v11/commands/npm-trust), [the September 2026 change](https://github.blog/changelog/2026-09-03-multiple-trusted-publishing-configurations-for-npm/), [pkg.pr.new](https://github.com/stackblitz-labs/pkg.pr.new) and [its retention](https://blog.stackblitz.com/posts/cloudflare-backing-pkg-pr-new-data-infrastructure/).

The bot is not settled. Renovate documents `followTag`, which follows a dist-tag. Dependabot's source moves a prerelease to a newer one with the same `0.0.0`, but GitHub's documentation does not say so. If neither fits, a scheduled workflow can run `npm install --save-exact @withcoral/wax@main` and open the pull request.

### 6. Where do the stories and their tests run?

- **This repository runs them all**, as Reef's `npm test` does today. A merge to `main` has passed them, so the publish needs no second run.
- **Ship the stories and the Storybook helpers. Do not ship the unit tests.** Vitest leaves `node_modules` out, so an app never runs them. That tarball is 511 files and 402 kB. With no stories it is 441 files and 325 kB.
- **An app keeps Wax in its Storybook** by one glob, `../node_modules/@withcoral/wax/**/*.stories.tsx`. Its story tests then run Wax's stories against its own lockfile, which is the one thing this repository cannot check. The scratch app ran all 399 in about 10 s.

## Not tried

- Reef with the relative imports in place. The package's files passed with them, but Reef's own gates did not run.
- A publish, the trusted publisher, and the bot.
- Hot reload of a Wax file, which matters only in this repository.
- The console's 14 changed files. Its three commands passed on `main`'s Wax without them. Nothing compared the pages by eye.

## Tickets that follow

1. **Make Wax a package where it is** (this repository). Move the helpers of question 2 and the fonts into `apps/reef/app/wax`. Make its imports relative. Add `package.json` and `vite.js`. Add a check that Wax imports nothing outside itself, that it declares each import, and that `vite.js` lists each run-time import. Change Reef's 27 helper imports, 8 stories and font preload. Update the `filter-query` path in `AGENTS.md`.
2. **Publish `@withcoral/wax` from `main`** (this repository). Blocked by 1. Create the package and the trusted publisher by hand. Add the workflow: a path filter on Wax, a GitHub-hosted runner, the version, `npm publish`.
3. **Bring the console's 14 changed files back here.** Already its own ticket. It blocks 4, or the console loses those changes.
4. **Install the package in the console** (`withcoral/coral-cloud`). Blocked by 2 and 3. Delete the copy, change the imports, add `wax()`, and add the bot.

## Where the spike is

Branch [`wax-spike`](https://github.com/withcoral/coral/tree/wax-spike/spikes/wax-package), folder `spikes/wax-package`: `build.ts` makes the tarball from Reef, and `scratch-app` is the Vite app. `npm run trial` in `scratch-app` repeats every check in the table above.

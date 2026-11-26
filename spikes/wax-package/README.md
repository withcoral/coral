# Spike: Wax as an npm package

Throwaway code for [withcoral/coral#3541](https://github.com/withcoral/coral/issues/3541).
Never merge this folder to `main`. It changes nothing in Reef and publishes nothing.

[FINDINGS.md](FINDINGS.md) holds the answer: the six questions, the errors the
trial met, the recommendation and the tickets that follow.

## What is here

- `build.ts` assembles `@withcoral/wax` from `apps/reef` and packs it into `out/`.
  It copies `app/wax` and the files outside it that Wax imports, rewrites the
  imports, writes `package.json` and the `vite.js` plugin, and runs `npm pack`.
- `scratch-app/` is a plain Vite app with the Vanilla Extract plugin. It installs
  the tarball.

## Run it

Node 24 runs the scripts as they are.

```sh
cd spikes/wax-package/scratch-app
./reinstall.sh        # build the tarball and install it
npm run trial         # typecheck, builds, dev servers, tests, Storybook, story tests
```

The lockfile is not committed, because npm pins the tarball by a digest that each
build changes. FINDINGS.md gives the versions that the reported results ran on.

`npm run trial` needs the Chromium that Playwright 1.64.1 downloads
(`npx playwright install chromium`).

`./reinstall.sh --imports alias` keeps the `@/wax` alias inside the package. That
variant needs a `paths` entry, `resolve.alias` and `optimizeDeps.exclude: ['@/wax']`
in the app. FINDINGS.md has the errors. `--no-tests` and `--no-stories` leave those
files out of the tarball.

This folder is not next to `apps/reef/app/wax` because Reef's `tsconfig.json` and
linter take every file under `apps/reef`.

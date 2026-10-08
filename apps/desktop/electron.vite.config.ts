import { defineConfig, externalizeDepsPlugin } from 'electron-vite'
import { execFileSync } from 'node:child_process'

// Fills the About panel's build-number slot; empty outside a git checkout.
function buildCommit(): string {
  try {
    return execFileSync('git', ['rev-parse', '--short', 'HEAD'], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim()
  } catch {
    return ''
  }
}

export function createConfig(env: NodeJS.ProcessEnv = process.env) {
  return defineConfig({
    main: {
      plugins: [externalizeDepsPlugin()],
      // Bake the release flag into the bundle. Only release packages set
      // CORAL_DESKTOP_RELEASE=1; unsigned QA and local builds bake `false` so
      // the auto-updater stays inert (it cannot install updates into an
      // unsigned app, and only release builds publish an update feed).
      define: {
        __CORAL_DESKTOP_COMMIT__: JSON.stringify(buildCommit()),
        __CORAL_DESKTOP_RELEASE__: JSON.stringify(env.CORAL_DESKTOP_RELEASE === '1'),
      },
      build: {
        rollupOptions: {
          input: { index: 'src/main/index.ts' },
        },
      },
    },
    preload: {
      plugins: [externalizeDepsPlugin()],
      build: {
        rollupOptions: {
          input: { index: 'src/preload/index.ts' },
          output: {
            format: 'cjs',
            entryFileNames: '[name].cjs',
          },
        },
      },
    },
  })
}

export default createConfig()

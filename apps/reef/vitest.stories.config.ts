/// <reference types="vitest/config" />

// Stories run in their own Vitest process. The Storybook plugin loads `.env` into
// process.env, which would leak local settings into the Node test projects.

import { availableParallelism } from 'node:os'
import path from 'node:path'

import { storybookTest } from '@storybook/addon-vitest/vitest-plugin'
import { playwright } from '@vitest/browser-playwright'
import { defineConfig } from 'vite'

import { shared } from './vitest.config.ts'

const dirname = import.meta.dirname

export default defineConfig({
  ...shared,
  plugins: [...shared.plugins, storybookTest({ configDir: path.join(dirname, '.storybook') })],
  resolve: {
    alias: {
      ...shared.resolve.alias,
      // addon-vitest 10 sets each story's viewport through this Vitest 4 path,
      // which Vitest 5 serves only as `vitest/browser`. Drop with addon-vitest 11.
      '@vitest/browser/context': 'vitest/browser',
    },
  },
  test: {
    // addon-vitest turns these off unless set. `.storybook/browser.ts` needs both for
    // CDP. The API server listens on localhost only.
    api: { allowExec: true, allowWrite: true },
    // Renders every story in Chromium and runs its play function.
    browser: {
      enabled: true,
      headless: true,
      instances: [{ browser: 'chromium' }],
      provider: playwright(),
    },
    css: true,
    // One Node process serves every page and relays each real key press. Past four
    // pages it falls behind, and stories that press keys time out.
    maxWorkers: Math.min(4, availableParallelism()),
    name: 'storybook',
    // Page live mode shows each record for MIN_LIVE_RECORD_STAY_MS, five seconds, before
    // it selects another, so a story that follows several records takes longer.
    testTimeout: 60_000,
  },
})

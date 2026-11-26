/// <reference types="vitest/config" />
// SPIKE (withcoral/coral#3541). Reef's vitest.stories.config.ts, less its aliases.
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
      // addon-vitest 10 sets each story's viewport through this Vitest 4 path,
      // which Vitest 5 serves only as `vitest/browser`. Drop with addon-vitest 11.
      '@vitest/browser/context': 'vitest/browser',
    },
  },
  test: {
    api: { allowExec: true, allowWrite: true },
    browser: {
      enabled: true,
      headless: true,
      instances: [{ browser: 'chromium' }],
      provider: playwright(),
    },
    css: true,
    maxWorkers: Math.min(4, availableParallelism()),
    name: 'storybook',
  },
})

/// <reference types="vitest/config" />

// Stories run in their own Vitest process. The Storybook plugin loads `.env` into
// process.env, which would leak local settings into the Node test projects.

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
    // Renders every story in Chromium and runs its play function.
    browser: {
      enabled: true,
      headless: true,
      instances: [{ browser: 'chromium' }],
      provider: playwright(),
    },
    css: true,
    name: 'storybook',
  },
})

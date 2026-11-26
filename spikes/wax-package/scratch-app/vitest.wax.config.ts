/// <reference types="vitest/config" />
// SPIKE (withcoral/coral#3541). Runs the package's own unit tests from node_modules, to
// prove that the rewritten imports left them passing. An app would not do this: Vitest
// leaves node_modules out, and the tests run in the repository that publishes Wax.
import { defineConfig } from 'vite'

import { shared } from './vitest.config.ts'

export default defineConfig({
  ...shared,
  test: {
    css: true,
    environment: 'node',
    exclude: [],
    include: ['node_modules/@withcoral/wax/**/*.test.{ts,tsx}'],
    name: 'wax',
    setupFiles: ['./vitest.setup.ts'],
  },
})

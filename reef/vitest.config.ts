/// <reference types="vitest/config" />

import path from 'node:path'
import { fileURLToPath } from 'node:url'

import babel from '@rolldown/plugin-babel'
import { storybookTest } from '@storybook/addon-vitest/vitest-plugin'
import { vanillaExtractPlugin } from '@vanilla-extract/vite-plugin'
import react, { reactCompilerPreset } from '@vitejs/plugin-react'
import { playwright } from '@vitest/browser-playwright'
import { defineConfig } from 'vite'

const dirname =
  typeof __dirname !== 'undefined' ? __dirname : path.dirname(fileURLToPath(import.meta.url))

// More info at: https://storybook.js.org/docs/next/writing-tests/integrations/vitest-addon
export default defineConfig({
  optimizeDeps: {
    include: ['@vanilla-extract/recipes/createRuntimeFn'],
  },
  plugins: [react(), babel({ presets: [reactCompilerPreset()] }), vanillaExtractPlugin()],
  resolve: {
    alias: {
      '@': path.resolve(dirname, './app'),
      '~': path.resolve(dirname, './app'),
    },
  },
  test: {
    browser: {
      enabled: true,
      instances: [
        {
          browser: 'chromium',
        },
      ],
      provider: playwright(),
      viewport: {
        height: 768,
        width: 1024,
      },
    },
    css: true,
    projects: [
      {
        extends: true,
        test: {
          exclude: [
            'app/**/*.server.test.{ts,tsx}',
            'app/**/*.stories.tsx',
            'app/__tests__/architecture.test.ts',
          ],
          include: ['app/**/*.test.{ts,tsx}'],
          name: 'unit',
        },
      },
      {
        extends: true,
        test: {
          browser: {
            enabled: false,
          },
          environment: 'node',
          include: ['app/**/*.server.test.{ts,tsx}'],
          name: 'server',
        },
      },
      {
        test: {
          browser: {
            enabled: false,
          },
          environment: 'node',
          include: ['app/__tests__/architecture.test.ts'],
          name: 'architecture',
        },
      },
      {
        extends: true,
        plugins: [
          storybookTest({
            configDir: path.join(dirname, '.storybook'),
          }),
        ],
        test: {
          browser: {
            enabled: true,
            headless: true,
            provider: playwright(),
          },
          name: 'storybook',
          setupFiles: ['.storybook/vitest.setup.ts'],
        },
      },
    ],
  },
})

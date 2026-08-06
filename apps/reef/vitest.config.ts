/// <reference types="vitest/config" />

import path from 'node:path'
import { fileURLToPath } from 'node:url'

import babel from '@rolldown/plugin-babel'
import { vanillaExtractPlugin } from '@vanilla-extract/vite-plugin'
import react, { reactCompilerPreset } from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

const dirname =
  typeof __dirname !== 'undefined' ? __dirname : path.dirname(fileURLToPath(import.meta.url))

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
    css: true,
    projects: [
      {
        extends: true,
        plugins: [browserServerBoundaryPlugin()],
        test: {
          exclude: [
            'app/**/*.server.test.{ts,tsx}',
            'app/**/*.stories.tsx',
            'app/__tests__/*architecture.test.ts',
          ],
          environment: 'node',
          include: ['app/**/*.test.{ts,tsx}'],
          name: 'unit',
        },
      },
      {
        extends: true,
        test: {
          environment: 'node',
          include: ['app/**/*.server.test.{ts,tsx}'],
          name: 'server',
        },
      },
      {
        extends: true,
        test: {
          environment: 'node',
          include: ['app/__tests__/*architecture.test.ts'],
          name: 'architecture',
        },
      },
    ],
  },
})

function browserServerBoundaryPlugin() {
  const stub = path.resolve(dirname, 'app/test-utils/coral-request.browser.ts')
  return {
    enforce: 'pre' as const,
    name: 'reef-browser-server-boundary',
    resolveId(id: string) {
      if (/(^|\/)coral-request\.server$/.test(id)) return stub
      return null
    },
  }
}

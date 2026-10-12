/// <reference types="vitest/config" />

import path from 'node:path'

import babel from '@rolldown/plugin-babel'
import { vanillaExtractPlugin } from '@vanilla-extract/vite-plugin'
import react, { reactCompilerPreset } from '@vitejs/plugin-react'
import { defineConfig, type UserConfig } from 'vite'

const dirname = import.meta.dirname

/** Build settings shared with `vitest.stories.config.ts`. */
export const shared = {
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
} satisfies UserConfig

export default defineConfig({
  ...shared,
  test: {
    css: true,
    projects: [
      {
        extends: true,
        test: {
          exclude: [
            'app/**/*.server.test.{ts,tsx}',
            'app/**/*.stories.tsx',
            'app/__tests__/*architecture.test.ts',
          ],
          environment: 'node',
          include: ['app/**/*.test.{ts,tsx}'],
          name: 'unit',
          setupFiles: ['./vitest.setup.ts'],
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

/// <reference types="vitest/config" />
// SPIKE (withcoral/coral#3541). The shape of Reef's vitest.config.ts, less its aliases.
import { vanillaExtractPlugin } from '@vanilla-extract/vite-plugin'
import react from '@vitejs/plugin-react'
import { wax } from '@withcoral/wax/vite.js'
import { defineConfig, type UserConfig } from 'vite'

/** Build settings shared with `vitest.stories.config.ts`. */
export const shared = {
  plugins: [react(), vanillaExtractPlugin(), wax()],
} satisfies UserConfig

export default defineConfig({
  ...shared,
  test: {
    css: true,
    environment: 'node',
    include: ['src/**/*.test.{ts,tsx}'],
    name: 'unit',
  },
})

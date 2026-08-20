import { reactRouter } from '@react-router/dev/vite'
import { vanillaExtractPlugin } from '@vanilla-extract/vite-plugin'
import { defineConfig, loadEnv } from 'vite'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const coralEndpoint = env.CORAL_ENDPOINT?.trim() || 'http://127.0.0.1:1457'

  // The proxy supports React Router server loaders when the dev server request
  // origin is not a recognized local origin. Browser code never receives a Coral
  // endpoint and must use loaders, actions, or resource routes instead.
  const proxy = {
    '/coral.v1': {
      changeOrigin: true,
      target: coralEndpoint,
    },
  }

  return {
    define: {
      'import.meta.env.CORAL_DESKTOP_APP': JSON.stringify(process.env.CORAL_DESKTOP_APP === '1'),
    },
    plugins: [vanillaExtractPlugin(), reactRouter()],
    resolve: {
      tsconfigPaths: true,
    },
    server: {
      proxy,
    },
  }
})

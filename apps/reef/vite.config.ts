import { reactRouter } from '@react-router/dev/vite'
import { vanillaExtractPlugin } from '@vanilla-extract/vite-plugin'
import { defineConfig } from 'vite'

import { devRequestTracing } from './dev-tracing.ts'

export default defineConfig({
  server: {
    host: '127.0.0.1',
    port: 5173,
    strictPort: true,
    // Keep Vite's default deny list and exclude checkout-local dev credentials
    // and databases even if a plugin widens the filesystem allow list.
    fs: {
      deny: [
        '.env',
        '.env.*',
        '*.{crt,pem,key,p12,pfx,cer,der}',
        '.npmrc',
        '.yarnrc.yml',
        '**/.git/**',
        '**/.dev/**',
      ],
    },
  },
  plugins: [vanillaExtractPlugin(), devRequestTracing(), reactRouter()],
  resolve: {
    tsconfigPaths: true,
  },
})

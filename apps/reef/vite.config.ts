import { reactRouter } from '@react-router/dev/vite'
import { vanillaExtractPlugin } from '@vanilla-extract/vite-plugin'
import { defineConfig } from 'vite'

// Under the desktop dev harness (apps/desktop/scripts/dev.mjs) the sidecar binds
// this fixed port. Proxying the same-origin `/__coral__` prefix to it keeps the
// gRPC-web client same-origin in dev — no CORS — mirroring the packaged app://
// proxy. Must match GRPC_PATH_PREFIX in apps/desktop/src/main/app-renderer.ts.
// Absent when Reef runs standalone, so no proxy is added there.
const sidecarPort = process.env.CORAL_DEV_SIDECAR_PORT
const coralProxy = sidecarPort
  ? {
      '/__coral__': {
        target: `http://127.0.0.1:${sidecarPort}`,
        changeOrigin: true,
        rewrite: (path: string) => path.replace(/^\/__coral__/, ''),
      },
    }
  : undefined

export default defineConfig({
  plugins: [vanillaExtractPlugin(), reactRouter()],
  resolve: {
    tsconfigPaths: true,
  },
  server: {
    proxy: coralProxy,
  },
})

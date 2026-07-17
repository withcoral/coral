import { reactRouter } from '@react-router/dev/vite'
import { vanillaExtractPlugin } from '@vanilla-extract/vite-plugin'
import { defineConfig, loadEnv, type ProxyOptions } from 'vite'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const coralEndpoint = env.CORAL_ENDPOINT?.trim() || 'http://127.0.0.1:1457'

  // Under the desktop dev harness (apps/desktop/scripts/dev.mjs) the sidecar binds
  // this fixed port. Proxying the same-origin `/__coral__` prefix to it keeps the
  // gRPC-web client same-origin in dev — no CORS — mirroring the packaged app://
  // proxy. Must match GRPC_PATH_PREFIX in apps/desktop/src/main/app-renderer.ts.
  const sidecarPort = env.CORAL_DEV_SIDECAR_PORT?.trim()
  const proxy: Record<string, string | ProxyOptions> = {
    '/coral.v1': {
      changeOrigin: true,
      target: coralEndpoint,
    },
  }

  if (sidecarPort) {
    proxy['/__coral__'] = {
      target: `http://127.0.0.1:${sidecarPort}`,
      changeOrigin: true,
      rewrite: (path: string) => path.replace(/^\/__coral__/, ''),
    }
  }

  return {
    plugins: [vanillaExtractPlugin(), reactRouter()],
    resolve: {
      tsconfigPaths: true,
    },
    server: {
      proxy,
    },
  }
})

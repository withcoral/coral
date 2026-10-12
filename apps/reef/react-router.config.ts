import type { Config } from '@react-router/dev/config'

// Vite loads .env before this config. TLS proxies leave the dev server seeing
// an internal HTTP origin, so explicitly allow Reef's configured public host.
// The production server replaces this build-time value at process bootstrap.
const publicUrl =
  process.env.CORAL_WEB_AUTH_MODE?.trim().toLowerCase() === 'required'
    ? process.env.CORAL_WEB_PUBLIC_URL?.trim()
    : undefined

export default {
  allowedActionOrigins: publicUrl ? [new URL(publicUrl).host] : [],
  // Desktop stages its own build here, so it never replaces this checkout's.
  // scripts/bundle-server.mjs bundles from the same directory.
  buildDirectory: process.env.REEF_BUILD_DIRECTORY || 'build',
} satisfies Config

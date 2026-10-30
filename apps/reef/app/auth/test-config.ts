import { vi } from 'vitest'

import type { CoralWebRuntimeConfig } from '@/lib/runtime-config.server'

import type { RequiredAuthConfig } from './types'

/** The hosted configuration that `hostedEnv()` resolves to. */
export const requiredAuthConfig: RequiredAuthConfig = {
  cookieName: '__Host-coral_web_session',
  issuer: 'https://coral.example.test',
  mode: 'required',
  publicUrl: 'https://coral-web.example.test',
  sessionMaxAgeSeconds: 3600,
  sessionSecret: '0123456789abcdef0123456789abcdef',
}

/**
 * The one topology that serves Coral web over HTTP: loopback throughout, so no
 * cookie can carry `Secure` or the `__Host-` prefix.
 */
export const loopbackAuthConfig: RequiredAuthConfig = {
  ...requiredAuthConfig,
  cookieName: 'coral_web_session',
  issuer: 'http://127.0.0.1:3000',
  publicUrl: 'http://localhost:5173',
}

/** A complete hosted deployment environment: required auth and an HTTPS Coral endpoint. */
export function hostedEnv(overrides: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv {
  return {
    NODE_ENV: 'production',
    CORAL_ENDPOINT: 'https://coral.example.test',
    CORAL_WEB_AUTH_ISSUER: requiredAuthConfig.issuer,
    CORAL_WEB_AUTH_MODE: 'required',
    CORAL_WEB_PUBLIC_URL: requiredAuthConfig.publicUrl,
    CORAL_WEB_SESSION_SECRET: requiredAuthConfig.sessionSecret,
    ...overrides,
  }
}

/** The runtime configuration that `hostedEnv()` resolves to. */
export const hostedRuntimeConfig: CoralWebRuntimeConfig = {
  auth: requiredAuthConfig,
  endpoint: { authenticatedCleartextOrigin: null, baseUrl: 'https://coral.example.test' },
  upstreamOAuthCallbackUrl: '',
  clientsWithAvailablePlugins: [],
}

/** Stubs `hostedEnv(overrides)` into `process.env`. Pair it with `vi.unstubAllEnvs()`. */
export function stubHostedEnv(overrides: NodeJS.ProcessEnv = {}): void {
  for (const [name, value] of Object.entries(hostedEnv(overrides))) vi.stubEnv(name, value)
}

import { isIP } from 'node:net'

import type { AuthConfig, RequiredAuthConfig } from './types'

const DEFAULT_COOKIE_NAME = 'reef_session'
const DEFAULT_SESSION_MAX_AGE_SECONDS = 60 * 60

interface AuthConfigInput {
  env: NodeJS.ProcessEnv
  isDesktopBuild: boolean
}

export function reefAuthConfig(): AuthConfig {
  return resolveAuthConfig({
    env: process.env,
    isDesktopBuild: import.meta.env.VITE_CORAL_DESKTOP_APP === '1',
  })
}

export function resolveAuthConfig({ env, isDesktopBuild }: AuthConfigInput): AuthConfig {
  if (isDesktopBuild) return { mode: 'disabled' }

  const configuredMode = env.REEF_AUTH_MODE?.trim().toLowerCase()
  if (!configuredMode) {
    if (env.NODE_ENV === 'production') {
      throw new Error('REEF_AUTH_MODE must be set to disabled or required in production')
    }

    return { mode: 'disabled' }
  }
  if (configuredMode === 'disabled') return { mode: 'disabled' }
  if (configuredMode !== 'required') {
    throw new Error('REEF_AUTH_MODE must be set to disabled or required')
  }

  return requiredAuthConfig(env)
}

function requiredAuthConfig(env: NodeJS.ProcessEnv): RequiredAuthConfig {
  const sessionSecret = env.REEF_SESSION_SECRET?.trim()
  if (!sessionSecret || sessionSecret.length < 32) {
    throw new Error('REEF_SESSION_SECRET must be at least 32 characters when auth is required')
  }

  const issuer = requiredString(env.REEF_AUTH_ISSUER, 'REEF_AUTH_ISSUER')
  const issuerUrl = httpUrl(issuer, 'REEF_AUTH_ISSUER')
  if (issuerUrl.username || issuerUrl.password) {
    throw new Error('REEF_AUTH_ISSUER must not include credentials')
  }
  if (issuerUrl.search || issuerUrl.hash) {
    throw new Error('REEF_AUTH_ISSUER must not include a query string or fragment')
  }
  if (issuerUrl.protocol === 'http:' && !isExplicitLoopback(issuerUrl)) {
    throw new Error('REEF_AUTH_ISSUER must use HTTPS or explicit-loopback HTTP')
  }

  const publicUrl = publicOrigin(requiredString(env.REEF_PUBLIC_URL, 'REEF_PUBLIC_URL'))
  if (new URL(publicUrl).protocol === 'http:' && issuerUrl.protocol !== 'http:') {
    throw new Error(
      'REEF_PUBLIC_URL may use HTTP only when REEF_AUTH_ISSUER also uses explicit-loopback HTTP',
    )
  }

  return {
    cookieName: optionalString(env.REEF_SESSION_COOKIE_NAME) ?? DEFAULT_COOKIE_NAME,
    issuer,
    mode: 'required',
    publicUrl,
    sessionMaxAgeSeconds: positiveIntegerEnv(
      env.REEF_SESSION_MAX_AGE_SECONDS,
      'REEF_SESSION_MAX_AGE_SECONDS',
      DEFAULT_SESSION_MAX_AGE_SECONDS,
    ),
    sessionSecret,
  }
}

export function authResource(config: RequiredAuthConfig): string {
  return config.publicUrl
}

export function authClientId(config: RequiredAuthConfig): string {
  return new URL('/.well-known/oauth-client', config.publicUrl).toString()
}

export function authRedirectUri(config: RequiredAuthConfig): string {
  return new URL('/auth/callback', config.publicUrl).toString()
}

export function authCookieSecure(config: RequiredAuthConfig): boolean {
  return new URL(config.publicUrl).protocol === 'https:'
}

function requiredString(value: string | undefined, name: string): string {
  const configured = optionalString(value)
  if (!configured) throw new Error(`${name} must be set when auth is required`)
  return configured
}

function httpUrl(value: string, name: string): URL {
  let url: URL
  try {
    url = new URL(value)
  } catch {
    throw new Error(`${name} must be an absolute HTTP(S) URL`)
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`${name} must be an absolute HTTP(S) URL`)
  }

  return url
}

function publicOrigin(value: string): string {
  let url: URL
  try {
    url = new URL(value)
  } catch {
    throw invalidPublicUrl()
  }
  if (
    (url.protocol !== 'https:' && url.protocol !== 'http:') ||
    url.username ||
    url.password ||
    url.pathname !== '/' ||
    url.search ||
    url.hash ||
    isLocalhostSubdomain(url.hostname) ||
    (url.protocol === 'http:' && !isExplicitLoopback(url))
  ) {
    throw invalidPublicUrl()
  }

  return url.origin
}

function invalidPublicUrl(): Error {
  return new Error(
    'REEF_PUBLIC_URL must be an HTTPS or explicit-loopback HTTP origin without credentials, path, query, or fragment',
  )
}

function isExplicitLoopback(url: URL): boolean {
  const hostname = unbracketedHostname(url.hostname).toLowerCase().replace(/\.$/, '')
  if (hostname === 'localhost') return true

  const family = isIP(hostname)
  if (family === 4) return hostname.split('.')[0] === '127'
  if (family !== 6) return false
  if (hostname === '::1') return true

  // WHATWG URL serialization renders IPv4-mapped loopback addresses in this
  // canonical hexadecimal form, e.g. ::ffff:127.0.0.1 -> ::ffff:7f00:1.
  return /^::ffff:7f[0-9a-f]{2}:[0-9a-f]{1,4}$/i.test(hostname)
}

function unbracketedHostname(hostname: string): string {
  return hostname.startsWith('[') && hostname.endsWith(']') ? hostname.slice(1, -1) : hostname
}

function isLocalhostSubdomain(hostname: string): boolean {
  return unbracketedHostname(hostname).toLowerCase().replace(/\.$/, '').endsWith('.localhost')
}

function positiveIntegerEnv(value: string | undefined, name: string, fallback: number): number {
  const configured = optionalString(value)
  if (!configured) return fallback
  const parsed = Number.parseInt(configured, 10)
  if (!/^[0-9]+$/.test(configured) || !Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`)
  }

  return parsed
}

function optionalString(value: string | undefined): string | null {
  return value?.trim() || null
}

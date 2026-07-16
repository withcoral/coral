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
  if (issuerUrl.search || issuerUrl.hash) {
    throw new Error('REEF_AUTH_ISSUER must not include a query string or fragment')
  }

  const redirectUri = optionalString(env.REEF_AUTH_REDIRECT_URI)
  const redirectUrl = redirectUri ? httpUrl(redirectUri, 'REEF_AUTH_REDIRECT_URI') : null
  if (redirectUrl?.hash) throw new Error('REEF_AUTH_REDIRECT_URI must not include a fragment')

  const cookieSecure = booleanEnv(env.REEF_COOKIE_SECURE) ?? redirectUrl?.protocol === 'https:'
  if (env.NODE_ENV === 'production') {
    if (issuerUrl.protocol !== 'https:')
      throw new Error('REEF_AUTH_ISSUER must use HTTPS in production')
    if (!redirectUrl) throw new Error('REEF_AUTH_REDIRECT_URI must be set in production')
    if (redirectUrl.protocol !== 'https:') {
      throw new Error('REEF_AUTH_REDIRECT_URI must use HTTPS in production')
    }
    if (!cookieSecure) throw new Error('REEF_COOKIE_SECURE cannot be false in production')
  }

  return {
    clientId: optionalString(env.REEF_AUTH_CLIENT_ID),
    cookieName: optionalString(env.REEF_SESSION_COOKIE_NAME) ?? DEFAULT_COOKIE_NAME,
    cookieSecure,
    issuer,
    mode: 'required',
    redirectUri,
    scope: optionalString(env.REEF_AUTH_SCOPE),
    sessionMaxAgeSeconds: positiveIntegerEnv(
      env.REEF_SESSION_MAX_AGE_SECONDS,
      'REEF_SESSION_MAX_AGE_SECONDS',
      DEFAULT_SESSION_MAX_AGE_SECONDS,
    ),
    sessionSecret,
  }
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

function booleanEnv(value: string | undefined): boolean | null {
  const configured = optionalString(value)?.toLowerCase()
  if (!configured) return null
  if (['1', 'true', 'yes'].includes(configured)) return true
  if (['0', 'false', 'no'].includes(configured)) return false
  throw new Error('REEF_COOKIE_SECURE must be true or false')
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

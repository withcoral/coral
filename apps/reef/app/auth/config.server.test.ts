import { readFileSync } from 'node:fs'

import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  authClientId,
  authCookieSecure,
  authRedirectUri,
  authResource,
  coralWebAuthConfig,
  resolveAuthConfig,
} from './config.server'
import { hostedEnv, requiredAuthConfig, stubHostedEnv } from './test-config'
import type { RequiredAuthConfig } from './types'

function requiredConfig(overrides: NodeJS.ProcessEnv = {}): RequiredAuthConfig {
  const config = resolveAuthConfig({ env: hostedEnv(overrides) })
  expect(config.mode).toBe('required')
  if (config.mode !== 'required') throw new Error('expected required auth config')
  return config
}

function loopbackConfig(overrides: NodeJS.ProcessEnv = {}): RequiredAuthConfig {
  return requiredConfig({
    CORAL_WEB_AUTH_ISSUER: 'http://127.0.0.1:9080',
    CORAL_WEB_PUBLIC_URL: 'http://127.0.0.1:5173',
    ...overrides,
  })
}

describe('Coral web auth config', () => {
  it('defaults local development and tests to disabled auth', () => {
    expect(resolveAuthConfig({ env: { NODE_ENV: 'development' } })).toEqual({
      mode: 'disabled',
    })
    expect(resolveAuthConfig({ env: { NODE_ENV: 'test' } })).toEqual({
      mode: 'disabled',
    })
  })

  it('fails closed when a production server has no explicit mode', () => {
    expect(() => resolveAuthConfig({ env: { NODE_ENV: 'production' } })).toThrow(
      'CORAL_WEB_AUTH_MODE must be set to disabled or required in production',
    )
  })

  it('allows production to explicitly disable auth', () => {
    expect(
      resolveAuthConfig({
        env: { NODE_ENV: 'production', CORAL_WEB_AUTH_MODE: 'disabled' },
      }),
    ).toEqual({ mode: 'disabled' })
  })

  it('derives every OAuth identifier and cookie security from the canonical public URL', () => {
    // The shared hosted fixtures describe one deployment.
    expect(requiredConfig()).toEqual(requiredAuthConfig)
    const config = requiredConfig({
      CORAL_WEB_AUTH_CLIENT_ID: 'ignored-client-id',
      CORAL_WEB_AUTH_ISSUER: 'https://coral.example.test:443/tenant/',
      CORAL_WEB_AUTH_REDIRECT_URI: 'https://ignored.example.test/callback',
      CORAL_WEB_AUTH_RESOURCE: 'https://ignored.example.test',
      CORAL_WEB_AUTH_SCOPE: 'ignored:scope',
      CORAL_WEB_COOKIE_SECURE: 'false',
      CORAL_WEB_MCP_URL: 'https://ignored.example.test/mcp',
      CORAL_WEB_PUBLIC_URL: 'https://CORAL-WEB.Example.test:443/',
      CORAL_WEB_SESSION_COOKIE_NAME: 'custom_session',
      CORAL_WEB_SESSION_MAX_AGE_SECONDS: '1800',
    })

    expect(config).toEqual({
      cookieName: '__Host-custom_session',
      issuer: 'https://coral.example.test:443/tenant/',
      mode: 'required',
      publicUrl: 'https://coral-web.example.test',
      sessionMaxAgeSeconds: 1800,
      sessionSecret: requiredAuthConfig.sessionSecret,
    })
    expect(authResource(config)).toBe('https://coral-web.example.test')
    expect(authClientId(config)).toBe('https://coral-web.example.test/.well-known/oauth-client')
    expect(authRedirectUri(config)).toBe('https://coral-web.example.test/auth/callback')
    expect(authCookieSecure(config)).toBe(true)
  })

  it('allows plain HTTP only when Coral web and Coral are both explicit loopback URLs', () => {
    const config = requiredConfig({
      CORAL_WEB_AUTH_ISSUER: 'http://127.42.0.1:3000',
      CORAL_WEB_PUBLIC_URL: 'http://[::1]:5173',
    })

    expect(config.publicUrl).toBe('http://[::1]:5173')
    expect(config.cookieName).toBe('coral_web_session')
    expect(authResource(config)).toBe('http://[::1]:5173')
    expect(authClientId(config)).toBe('http://[::1]:5173/.well-known/oauth-client')
    expect(authRedirectUri(config)).toBe('http://[::1]:5173/auth/callback')
    expect(authCookieSecure(config)).toBe(false)
  })

  it.each([
    ['http://localhost:5173', 'http://localhost:3000'],
    ['http://localhost.:5173', 'http://localhost.:3000'],
    ['http://127.255.1.2:5173', 'http://127.0.0.1:3000'],
    ['http://[::1]:5173', 'http://[::1]:3000'],
    ['http://[::ffff:127.0.0.1]:5173', 'http://[::ffff:127.0.0.2]:3000'],
  ])('accepts explicit loopback pair %s and %s', (publicUrl, issuer) => {
    expect(() =>
      requiredConfig({ CORAL_WEB_AUTH_ISSUER: issuer, CORAL_WEB_PUBLIC_URL: publicUrl }),
    ).not.toThrow()
  })

  it('rejects an HTTP Coral web URL when the Coral issuer is not also loopback HTTP', () => {
    expect(() => requiredConfig({ CORAL_WEB_PUBLIC_URL: 'http://127.0.0.1:5173' })).toThrow(
      'CORAL_WEB_PUBLIC_URL may use HTTP only when CORAL_WEB_AUTH_ISSUER also uses explicit-loopback HTTP',
    )
  })

  it.each([
    'http://192.168.1.10:5173',
    'http://10.0.0.10:5173',
    'http://169.254.1.10:5173',
    'http://coral-web.internal:5173',
    'http://preview.localhost:5173',
  ])('rejects a non-loopback HTTP public URL: %s', (publicUrl) => {
    expect(() => requiredConfig({ CORAL_WEB_PUBLIC_URL: publicUrl })).toThrow(
      'CORAL_WEB_PUBLIC_URL must be an HTTPS or explicit-loopback HTTP origin',
    )
  })

  it.each([
    'https://user:password@coral-web.example.test',
    'https://coral-web.example.test/a/path',
    'https://coral-web.example.test?tenant=coral',
    'https://coral-web.example.test#fragment',
    'ftp://coral-web.example.test',
  ])('rejects a public URL that is not a clean HTTP(S) origin: %s', (publicUrl) => {
    expect(() => requiredConfig({ CORAL_WEB_PUBLIC_URL: publicUrl })).toThrow(
      'CORAL_WEB_PUBLIC_URL must be an HTTPS or explicit-loopback HTTP origin',
    )
  })

  it.each([
    'http://192.168.1.10:3000',
    'http://10.0.0.10:3000',
    'http://169.254.1.10:3000',
    'http://coral.internal:3000',
    'http://preview.localhost:3000',
  ])('rejects a non-loopback HTTP Coral issuer: %s', (issuer) => {
    expect(() => requiredConfig({ CORAL_WEB_AUTH_ISSUER: issuer })).toThrow(
      'CORAL_WEB_AUTH_ISSUER must use HTTPS or explicit-loopback HTTP',
    )
  })

  it.each([
    ['sometimes', 'CORAL_WEB_AUTH_MODE must be set to disabled or required'],
    ['', 'CORAL_WEB_AUTH_MODE must be set to disabled or required'],
  ])('rejects invalid auth mode %s', (mode, message) => {
    expect(() =>
      resolveAuthConfig({
        env: hostedEnv({ CORAL_WEB_AUTH_MODE: mode }),
      }),
    ).toThrow(message)
  })

  it.each([
    [
      { CORAL_WEB_SESSION_SECRET: 'too-short' },
      'CORAL_WEB_SESSION_SECRET must be at least 32 characters',
    ],
    [{ CORAL_WEB_AUTH_ISSUER: '' }, 'CORAL_WEB_AUTH_ISSUER must be set'],
    [{ CORAL_WEB_PUBLIC_URL: '' }, 'CORAL_WEB_PUBLIC_URL must be set'],
    [
      { CORAL_WEB_AUTH_ISSUER: 'file:///tmp/issuer' },
      'CORAL_WEB_AUTH_ISSUER must be an absolute HTTP(S) URL',
    ],
    [
      { CORAL_WEB_AUTH_ISSUER: 'https://user:password@coral.example.test' },
      'CORAL_WEB_AUTH_ISSUER must not include credentials',
    ],
    [
      { CORAL_WEB_AUTH_ISSUER: 'https://coral.example.test/tenant?region=us' },
      'CORAL_WEB_AUTH_ISSUER must not include a query string or fragment',
    ],
    [
      { CORAL_WEB_SESSION_MAX_AGE_SECONDS: '0' },
      'CORAL_WEB_SESSION_MAX_AGE_SECONDS must be a positive integer',
    ],
    // Each of these serializes into `Set-Cookie` without complaint and is then
    // dropped by the browser, so the only symptom is a session that never
    // persists. A separator, whitespace, and a control character cover the three
    // ways a name leaves the token grammar.
    [
      { CORAL_WEB_SESSION_COOKIE_NAME: 'coral-web session' },
      'CORAL_WEB_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { CORAL_WEB_SESSION_COOKIE_NAME: 'coral-web=session' },
      'CORAL_WEB_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { CORAL_WEB_SESSION_COOKIE_NAME: 'coral-web;session' },
      'CORAL_WEB_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { CORAL_WEB_SESSION_COOKIE_NAME: 'coral-web\tsession' },
      'CORAL_WEB_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { CORAL_WEB_SESSION_COOKIE_NAME: 'coral-web,session' },
      'CORAL_WEB_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { CORAL_WEB_SESSION_COOKIE_NAME: 'coral-web"session"' },
      'CORAL_WEB_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
  ])('rejects invalid required config %#', (overrides, message) => {
    expect(() => requiredConfig(overrides)).toThrow(message)
  })

  it('accepts every character the cookie-name grammar allows', () => {
    const name = "coral_web_session-0.9!#$%&'*+^`|~"
    expect(requiredConfig({ CORAL_WEB_SESSION_COOKIE_NAME: name }).cookieName).toBe(
      `__Host-${name}`,
    )
    expect(loopbackConfig({ CORAL_WEB_SESSION_COOKIE_NAME: name }).cookieName).toBe(name)
  })

  // `__Host-` is what binds a hosted session to Reef's exact host, and a browser
  // honours it only on a cookie set with `Secure`. Coral web derives `Secure`
  // from the public URL rather than from a switch, so the prefix is applied
  // over HTTPS, kept where an operator already wrote it, and refused over
  // loopback HTTP, where the browser would discard the cookie.
  it.each([
    ['custom_session', '__Host-custom_session'],
    ['__Host-custom_session', '__Host-custom_session'],
    // Browsers match the prefix without regard to case, so this is not prefixed again.
    ['__host-custom_session', '__host-custom_session'],
  ])('binds the configured name %s to the host over HTTPS as %s', (configured, bound) => {
    expect(requiredConfig({ CORAL_WEB_SESSION_COOKIE_NAME: configured }).cookieName).toBe(bound)
  })

  it.each(['__Host-coral_web_session', '__host-coral_web_session'])(
    'rejects %s over loopback HTTP',
    (name) => {
      expect(() => loopbackConfig({ CORAL_WEB_SESSION_COOKIE_NAME: name })).toThrow(
        'CORAL_WEB_SESSION_COOKIE_NAME may use a __Host- prefix only when CORAL_WEB_PUBLIC_URL is HTTPS',
      )
    },
  )

  // Each of these has an accessor that computes it from `CORAL_WEB_PUBLIC_URL`
  // — `authClientId`, `authRedirectUri`, `authResource`, `authCookieSecure` —
  // so none is read from the environment, and documenting one in `.env.example`
  // would advertise a setting that silently does nothing. The contract is what
  // the file says, so the file is what this asserts against.
  it('keeps derived auth settings out of the documented environment contract', () => {
    const example = readFileSync(new URL('../../.env.example', import.meta.url), 'utf8')
    for (const derivedName of [
      'CORAL_WEB_AUTH_CLIENT_ID',
      'CORAL_WEB_AUTH_REDIRECT_URI',
      'CORAL_WEB_AUTH_RESOURCE',
      'CORAL_WEB_COOKIE_SECURE',
    ]) {
      expect(example).not.toContain(derivedName)
    }
  })
})

// The cases above hand `resolveAuthConfig` an explicit environment. This one
// checks that the server's own entry point reads the process environment.
describe('coralWebAuthConfig', () => {
  afterEach(() => {
    vi.unstubAllEnvs()
  })

  it('reads its mode from the process environment', () => {
    stubHostedEnv()

    expect(coralWebAuthConfig().mode).toBe('required')
  })
})

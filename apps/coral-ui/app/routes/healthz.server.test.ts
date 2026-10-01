import { afterEach, describe, expect, it, vi } from 'vitest'

import { loader } from './healthz'

const SESSION_SECRET = '0123456789abcdef0123456789abcdef'

function stubRequiredEnv(overrides: NodeJS.ProcessEnv = {}): void {
  const env = {
    CORAL_ENDPOINT: 'https://unreachable.invalid',
    NODE_ENV: 'production',
    CORAL_UI_AUTH_ISSUER: 'https://unreachable.invalid',
    CORAL_UI_AUTH_MODE: 'required',
    CORAL_UI_PUBLIC_URL: 'https://coral-ui.example.test',
    CORAL_UI_SESSION_SECRET: SESSION_SECRET,
    ...overrides,
  }
  for (const [name, value] of Object.entries(env)) vi.stubEnv(name, value)
}

describe('health check route', () => {
  afterEach(() => {
    vi.unstubAllEnvs()
    vi.unstubAllGlobals()
  })

  it('returns 200 for valid config without contacting Coral or an auth provider', async () => {
    stubRequiredEnv()
    const fetch = vi.fn(() => Promise.reject(new Error('outbound access is unavailable')))
    vi.stubGlobal('fetch', fetch)

    const response = runLoader()

    expect(response.status).toBe(200)
    expect(response.headers.get('Content-Type')).toContain('application/json')
    await expect(response.json()).resolves.toEqual({ status: 'ok' })
    expect(fetch).not.toHaveBeenCalled()
  })

  it('throws synchronously for invalid required-auth config', () => {
    stubRequiredEnv({ CORAL_UI_PUBLIC_URL: '' })

    expect(() => runLoader()).toThrow('CORAL_UI_PUBLIC_URL must be set when auth is required')
  })

  it('throws synchronously for an insecure authenticated Coral endpoint', () => {
    stubRequiredEnv({ CORAL_ENDPOINT: 'http://coral.internal:14555' })

    expect(() => runLoader()).toThrow(
      'CORAL_ENDPOINT must use HTTPS or explicit-loopback HTTP when Coral authentication is enabled',
    )
  })
})

function runLoader(): Response {
  return loader({ request: new Request('http://coral-ui.test/healthz') } as never)
}

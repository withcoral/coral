import { afterEach, describe, expect, it, vi } from 'vitest'

import { loader } from './healthz'

describe('health check route', () => {
  afterEach(() => {
    vi.unstubAllEnvs()
    vi.unstubAllGlobals()
  })

  it('returns 200 without contacting Coral or an auth provider', async () => {
    const fetch = vi.fn(() => Promise.reject(new Error('outbound access is unavailable')))
    vi.stubGlobal('fetch', fetch)

    const response = loader()

    expect(response.status).toBe(200)
    expect(response.headers.get('Content-Type')).toContain('application/json')
    await expect(response.json()).resolves.toEqual({ status: 'ok' })
    expect(fetch).not.toHaveBeenCalled()
  })

  // Startup refuses an invalid configuration. A running process must not start
  // failing liveness because its environment changed afterwards.
  it('does not read the environment', () => {
    vi.stubEnv('NODE_ENV', 'production')
    vi.stubEnv('CORAL_WEB_AUTH_MODE', 'not-a-mode')

    expect(loader().status).toBe(200)
  })
})

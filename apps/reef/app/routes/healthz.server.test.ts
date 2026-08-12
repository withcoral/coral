import { afterEach, describe, expect, it, vi } from 'vitest'

import { loader } from './healthz'

describe('health check route', () => {
  afterEach(() => {
    vi.unstubAllEnvs()
    vi.unstubAllGlobals()
  })

  it('returns 200 without contacting Coral or an auth provider', async () => {
    vi.stubEnv('CORAL_ENDPOINT', 'http://unreachable.invalid')
    vi.stubEnv('REEF_AUTH_ISSUER', 'https://unreachable.invalid')
    const fetch = vi.fn(() => Promise.reject(new Error('outbound access is unavailable')))
    vi.stubGlobal('fetch', fetch)

    const response = loader({
      request: new Request('http://reef.test/healthz'),
    } as never)

    expect(response.status).toBe(200)
    expect(response.headers.get('Content-Type')).toContain('application/json')
    await expect(response.json()).resolves.toEqual({ status: 'ok' })
    expect(fetch).not.toHaveBeenCalled()
  })
})

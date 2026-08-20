import { describe, expect, it, vi } from 'vitest'

import type { RequiredAuthConfig } from '@/auth/types'

const authMocks = vi.hoisted(() => ({ coralUIAuthConfig: vi.fn() }))

vi.mock('@/auth/config.server', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/auth/config.server')>()),
  coralUIAuthConfig: authMocks.coralUIAuthConfig,
}))

import { loader } from './oauth-client-metadata'

const requiredConfig: RequiredAuthConfig = {
  cookieName: 'coral_ui_session',
  issuer: 'https://coral.example.test',
  mode: 'required',
  publicUrl: 'https://coral-ui.example.test',
  sessionMaxAgeSeconds: 3600,
  sessionSecret: '0123456789abcdef0123456789abcdef',
}

describe('OAuth client metadata route', () => {
  it('serves a cacheable CIMD document derived from the canonical Coral UI public URL', async () => {
    authMocks.coralUIAuthConfig.mockReturnValue(requiredConfig)

    const response = await runLoader('https://internal-proxy/.well-known/oauth-client')
    const body = await response.json()

    expect(response.status).toBe(200)
    expect(response.headers.get('Content-Type')).toBe('application/json')
    expect(response.headers.get('Cache-Control')).toBe('public, max-age=300')
    expect(response.headers.has('Set-Cookie')).toBe(false)
    expect(Buffer.byteLength(JSON.stringify(body))).toBeLessThanOrEqual(5 * 1024)
    expect(body).toEqual({
      client_id: 'https://coral-ui.example.test/.well-known/oauth-client',
      client_name: 'Coral UI',
      grant_types: ['authorization_code'],
      redirect_uris: ['https://coral-ui.example.test/auth/callback'],
      response_types: ['code'],
      token_endpoint_auth_method: 'none',
    })
  })

  it('serves the same document over the accepted all-loopback HTTP topology', async () => {
    authMocks.coralUIAuthConfig.mockReturnValue({
      ...requiredConfig,
      issuer: 'http://127.0.0.1:3000',
      publicUrl: 'http://localhost:5173',
    })

    const body = await (await runLoader('http://localhost:5173/.well-known/oauth-client')).json()

    expect(body.client_id).toBe('http://localhost:5173/.well-known/oauth-client')
    expect(body.redirect_uris).toEqual(['http://localhost:5173/auth/callback'])
  })

  it('does not publish OAuth client metadata when auth is disabled', async () => {
    authMocks.coralUIAuthConfig.mockReturnValue({ mode: 'disabled' })

    const error = await runLoader('http://localhost:5173/.well-known/oauth-client').catch(
      (caught: unknown) => caught,
    )

    expect(error).toBeInstanceOf(Response)
    expect((error as Response).status).toBe(404)
  })
})

function runLoader(url: string): Promise<Response> {
  return loader({ request: new Request(url) } as never)
}

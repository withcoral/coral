import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { RequiredAuthConfig } from '@/auth/types'

const authMocks = vi.hoisted(() => ({
  readReefSession: vi.fn(),
  reefAuthConfig: vi.fn(),
  startCoralOAuthLogin: vi.fn(),
}))

vi.mock('@/auth/config.server', () => ({ reefAuthConfig: authMocks.reefAuthConfig }))
vi.mock('@/auth/coral-oauth.server', () => ({
  startCoralOAuthLogin: authMocks.startCoralOAuthLogin,
}))
vi.mock('@/auth/session.server', () => ({ readReefSession: authMocks.readReefSession }))

import { headers, loader } from './login'

const requiredConfig: RequiredAuthConfig = {
  cookieName: 'reef_session',
  issuer: 'https://coral.example.test',
  mode: 'required',
  publicUrl: 'https://reef.example.test',
  sessionMaxAgeSeconds: 3600,
  sessionSecret: '0123456789abcdef0123456789abcdef',
}

describe('login route', () => {
  beforeEach(() => {
    authMocks.readReefSession.mockReset()
    authMocks.reefAuthConfig.mockReset()
    authMocks.startCoralOAuthLogin.mockReset()
  })

  it('marks rendered login responses private at the route boundary', () => {
    const responseHeaders = new Headers(headers())

    expect(responseHeaders.get('Cache-Control')).toBe('private, no-store')
    expect(responseHeaders.get('Vary')).toBe('Cookie')
  })

  it('returns disabled local and desktop requests home without reading auth state', async () => {
    authMocks.reefAuthConfig.mockReturnValue({ mode: 'disabled' })

    const response = await runLoader('http://localhost:5173/login')

    expect(response.status).toBe(302)
    expect(response.headers.get('location')).toBe('/')
    expect(authMocks.readReefSession).not.toHaveBeenCalled()
    expect(authMocks.startCoralOAuthLogin).not.toHaveBeenCalled()
  })

  it('returns an existing hosted session home instead of restarting OAuth', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue({
      accessToken: 'server-only-token',
      expiresAt: 4_102_444_800,
      tokenType: 'Bearer',
    })

    const response = await runLoader('https://reef.example.test/login')

    expect(response.status).toBe(302)
    expect(response.headers.get('location')).toBe('/')
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    expect(authMocks.startCoralOAuthLogin).not.toHaveBeenCalled()
  })

  it('renders the signed-out interstitial without immediately restarting SSO', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue(null)

    await expect(runLoaderValue('https://reef.example.test/login?signedOut=1')).resolves.toBeNull()
    expect(authMocks.startCoralOAuthLogin).not.toHaveBeenCalled()
  })

  it('starts hosted OAuth and preserves its redirect and transaction cookie', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue(null)
    const upstream = new Response(null, {
      headers: {
        location: 'https://coral.example.test/authorize',
        'Set-Cookie': 'reef_oauth=transaction; Path=/; HttpOnly',
      },
      status: 302,
    })
    authMocks.startCoralOAuthLogin.mockResolvedValue(upstream)
    const request = new Request('https://reef.example.test/login?returnTo=%2Fworkspaces')

    const response = (await loader({ request } as never)) as Response

    expect(authMocks.startCoralOAuthLogin).toHaveBeenCalledWith(request, requiredConfig)
    expect(response).toBe(upstream)
    expect(response.headers.get('location')).toBe('https://coral.example.test/authorize')
    expect(response.headers.get('Set-Cookie')).toContain('reef_oauth=transaction')
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    expect(response.headers.get('Vary')).toContain('Cookie')
  })
})

async function runLoader(url: string): Promise<Response> {
  const result = await runLoaderValue(url)
  expect(result).toBeInstanceOf(Response)
  return result as Response
}

async function runLoaderValue(url: string) {
  return loader({ request: new Request(url) } as never)
}

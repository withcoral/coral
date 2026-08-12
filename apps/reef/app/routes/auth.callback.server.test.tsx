import { renderToStaticMarkup } from 'react-dom/server'
import { MemoryRouter } from 'react-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { RequiredAuthConfig } from '@/auth/types'

const authMocks = vi.hoisted(() => ({
  completeCoralOAuthLogin: vi.fn(),
  reefAuthConfig: vi.fn(),
}))

vi.mock('@/auth/config.server', () => ({ reefAuthConfig: authMocks.reefAuthConfig }))
vi.mock('@/auth/coral-oauth.server', () => ({
  completeCoralOAuthLogin: authMocks.completeCoralOAuthLogin,
}))

import { ErrorBoundary, loader } from './auth.callback'

const requiredConfig: RequiredAuthConfig = {
  cookieName: 'reef_session',
  issuer: 'https://coral.example.test',
  mode: 'required',
  publicUrl: 'https://reef.example.test',
  sessionMaxAgeSeconds: 3600,
  sessionSecret: '0123456789abcdef0123456789abcdef',
}

describe('auth callback route', () => {
  beforeEach(() => {
    authMocks.completeCoralOAuthLogin.mockReset()
    authMocks.reefAuthConfig.mockReset()
  })

  it('is unavailable when local or desktop auth is disabled', async () => {
    authMocks.reefAuthConfig.mockReturnValue({ mode: 'disabled' })

    const thrown = await runLoader().catch((error: unknown) => error)

    expect(thrown).toBeInstanceOf(Response)
    expect((thrown as Response).status).toBe(404)
    expect(authMocks.completeCoralOAuthLogin).not.toHaveBeenCalled()
  })

  it('preserves the hosted callback redirect and session cookie response', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    const callbackResponse = new Response(null, {
      headers: [
        ['location', '/workspaces/analytics/sources'],
        ['Set-Cookie', 'reef_oauth=; Max-Age=0; Path=/; HttpOnly'],
        ['Set-Cookie', 'reef_session=encrypted; Path=/; HttpOnly; Secure'],
      ],
      status: 302,
    })
    authMocks.completeCoralOAuthLogin.mockResolvedValue(callbackResponse)
    const request = new Request('https://reef.example.test/auth/callback?code=code&state=state')

    const response = await loader({ request } as never)

    expect(authMocks.completeCoralOAuthLogin).toHaveBeenCalledWith(request, requiredConfig)
    expect(response).toBe(callbackResponse)
    expect(response.headers.get('location')).toBe('/workspaces/analytics/sources')
    expect(response.headers.getSetCookie()).toHaveLength(2)
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
  })

  it('marks expected callback failures private before the error boundary handles them', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    const callbackError = new Response('provider detail', { status: 400 })
    authMocks.completeCoralOAuthLogin.mockRejectedValue(callbackError)

    const thrown = await runLoader().catch((error: unknown) => error)

    expect(thrown).toBe(callbackError)
    expect(callbackError.headers.get('Cache-Control')).toBe('private, no-store')
    expect(callbackError.headers.get('Vary')).toContain('Cookie')
  })

  it('renders an accessible generic error without provider or token details', () => {
    const markup = renderToStaticMarkup(
      <MemoryRouter initialEntries={['/auth/callback']}>
        <ErrorBoundary />
      </MemoryRouter>,
    )

    expect(markup).toContain('aria-labelledby="auth-error-title"')
    expect(markup).toContain('Sign-in failed')
    expect(markup).toContain('href="/login"')
    expect(markup).toContain('Try again')
    expect(markup).not.toContain('provider detail')
    expect(markup).not.toContain('access_token')
  })
})

async function runLoader() {
  return loader({
    request: new Request('https://reef.example.test/auth/callback?code=code&state=state'),
  } as never)
}

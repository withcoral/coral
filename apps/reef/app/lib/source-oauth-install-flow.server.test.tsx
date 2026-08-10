import { renderToString } from 'react-dom/server'
import { describe, expect, it, vi } from 'vitest'

import {
  AUTH_STREAM_REQUEST_HEADER,
  AUTH_STREAM_RETURN_TO_HEADER,
  EXPIRED_SESSION_LOGIN_HEADER,
} from '@/auth/response'

import { runOAuthInstallFlow, useOAuthInstallFlow } from './source-oauth-install-flow'

function OAuthFlowHarness() {
  useOAuthInstallFlow({
    fetchOAuthInstall: vi.fn(),
    onComplete: vi.fn(),
    openAuthorization: vi.fn(),
  })
  return null
}

describe('useOAuthInstallFlow server rendering', () => {
  it('does not read browser globals while rendering', () => {
    expect(() => renderToString(<OAuthFlowHarness />)).not.toThrow()
  })
})

describe('runOAuthInstallFlow', () => {
  it('navigates to login without consuming an expired-session stream', async () => {
    const response = new Response(
      new ReadableStream({
        pull(controller) {
          controller.error(new Error('expired-session response body must not be read'))
        },
      }),
      {
        headers: {
          [EXPIRED_SESSION_LOGIN_HEADER]:
            '/login?returnTo=%2Fworkspaces%2Fanalytics%2Fsources%2Fnew',
        },
        status: 401,
      },
    )
    const fetchOAuthInstall = vi.fn<typeof fetch>().mockResolvedValue(response)
    const navigateToLogin = vi.fn()
    const onComplete = vi.fn()
    const openAuthorization = vi.fn()
    const errors: Array<string | null> = []
    const progress: Array<{ kind: string }> = []
    const formData = new FormData()
    const abortController = new AbortController()

    await runOAuthInstallFlow({
      endpoint: '/sources/github/oauth-install',
      fetchOAuthInstall,
      formData,
      navigateToLogin,
      onComplete,
      openAuthorization,
      setError: (error) => errors.push(error),
      setProgress: (nextProgress) => progress.push(nextProgress),
      signal: abortController.signal,
      visibleLocation: '/workspaces/analytics/sources/new?step=oauth',
    })

    expect(fetchOAuthInstall).toHaveBeenCalledOnce()
    expect(fetchOAuthInstall).toHaveBeenCalledWith(
      '/sources/github/oauth-install',
      expect.objectContaining({
        body: formData,
        headers: {
          [AUTH_STREAM_REQUEST_HEADER]: '1',
          [AUTH_STREAM_RETURN_TO_HEADER]: '/workspaces/analytics/sources/new?step=oauth',
        },
        method: 'POST',
        signal: abortController.signal,
      }),
    )
    expect(navigateToLogin).toHaveBeenCalledOnce()
    expect(navigateToLogin).toHaveBeenCalledWith(
      '/login?returnTo=%2Fworkspaces%2Fanalytics%2Fsources%2Fnew',
    )
    expect(progress).toEqual([{ kind: 'busy' }, { kind: 'idle' }])
    expect(errors).toEqual([null])
    expect(response.bodyUsed).toBe(false)
    expect(openAuthorization).not.toHaveBeenCalled()
    expect(onComplete).not.toHaveBeenCalled()
  })
})

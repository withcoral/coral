import { describe, expect, it, vi } from 'vitest'
import { renderHook } from 'vitest-browser-react'

import {
  AUTH_STREAM_REQUEST_HEADER,
  AUTH_STREAM_RETURN_TO_HEADER,
  EXPIRED_SESSION_LOGIN_HEADER,
} from '@/auth/response'

import { useOAuthInstallFlow } from './source-oauth-install-flow'

describe('useOAuthInstallFlow', () => {
  it('navigates the document when the stream endpoint reports an expired Reef session', async () => {
    const loginLocation = '/login?returnTo=%2Fworkspaces%2Fanalytics%2Fsources%2Foauth-import'
    const fetchOAuthInstall = vi.fn(
      async () =>
        new Response(null, {
          headers: { [EXPIRED_SESSION_LOGIN_HEADER]: loginLocation },
          status: 401,
        }),
    )
    const navigateToLogin = vi.fn()
    const hook = await renderHook(() =>
      useOAuthInstallFlow({
        fetchOAuthInstall,
        navigateToLogin,
        onComplete: vi.fn(),
        openAuthorization: vi.fn(),
        returnTo: '/workspaces/analytics/sources/github?tab=setup',
      }),
    )

    await hook.act(() =>
      hook.result.current.start('/workspaces/analytics/sources/oauth-import', new FormData()),
    )

    expect(fetchOAuthInstall).toHaveBeenCalledWith(
      '/workspaces/analytics/sources/oauth-import',
      expect.objectContaining({
        headers: {
          [AUTH_STREAM_REQUEST_HEADER]: '1',
          [AUTH_STREAM_RETURN_TO_HEADER]: '/workspaces/analytics/sources/github?tab=setup',
        },
        method: 'POST',
      }),
    )
    expect(navigateToLogin).toHaveBeenCalledWith(loginLocation)
    expect(hook.result.current.progress).toEqual({ kind: 'idle' })
    expect(hook.result.current.error).toBeNull()
  })
})

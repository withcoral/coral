import { beforeEach, describe, expect, it, vi } from 'vitest'

const { getGuiOnboardingCompleted, redirectToFirstWorkspaceTraces } = vi.hoisted(() => ({
  getGuiOnboardingCompleted: vi.fn(),
  redirectToFirstWorkspaceTraces: vi.fn(),
}))

vi.mock('@/lib/gui-onboarding.server', () => ({ getGuiOnboardingCompleted }))
vi.mock('@/lib/workspace-redirect.server', () => ({ redirectToFirstWorkspaceTraces }))

import { authRouteTestArgs } from '@/auth/server-context.test-helper'

import { loader } from './index'

describe('app index loader', () => {
  beforeEach(() => {
    getGuiOnboardingCompleted.mockReset()
    redirectToFirstWorkspaceTraces.mockReset()
  })

  it('starts incomplete users in onboarding', async () => {
    getGuiOnboardingCompleted.mockResolvedValue(false)
    const request = new Request('http://coral-ui.test/')

    const response = await loader(authRouteTestArgs(request, {}, null))

    expect(response).toBeInstanceOf(Response)
    expect(response.status).toBe(302)
    expect(response.headers.get('Location')).toBe('/onboarding')
    expect(response.headers.get('X-Remix-Replace')).toBe('true')
    expect(getGuiOnboardingCompleted).toHaveBeenCalledWith(request, null)
    expect(redirectToFirstWorkspaceTraces).not.toHaveBeenCalled()
  })

  it('starts completed users in the normal app', async () => {
    getGuiOnboardingCompleted.mockResolvedValue(true)
    const request = new Request('http://coral-ui.test/')
    const response = new Response(null, {
      headers: { Location: '/workspaces/default/traces' },
      status: 302,
    })
    redirectToFirstWorkspaceTraces.mockResolvedValue(response)

    await expect(loader(authRouteTestArgs(request, {}, 'coral-access-token'))).resolves.toBe(
      response,
    )
    expect(getGuiOnboardingCompleted).toHaveBeenCalledWith(request, 'coral-access-token')
    expect(redirectToFirstWorkspaceTraces).toHaveBeenCalledWith(request, 'coral-access-token')
  })
})

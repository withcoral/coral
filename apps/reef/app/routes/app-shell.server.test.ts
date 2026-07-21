import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const { listWorkspacesForRequest } = vi.hoisted(() => ({
  listWorkspacesForRequest: vi.fn(),
}))

vi.mock('@/lib/workspaces.server', () => ({ listWorkspacesForRequest }))

import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { routePath } from '@/routing/routemap'

import { loader } from './app-shell'

describe('app shell loader', () => {
  beforeEach(() => {
    listWorkspacesForRequest.mockReset()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('loads the hosted workspace list and browser-safe auth state for the sidebar', async () => {
    const request = new Request('http://reef.test/workspaces/default/sources')
    const workspaces = [{ name: 'default' }, { name: 'analytics' }]
    listWorkspacesForRequest.mockResolvedValue(workspaces)

    await expect(loader(authRouteTestArgs(request, {}, 'coral-access-token'))).resolves.toEqual({
      auth: { csrfToken: 'test-csrf-token', mode: 'required' },
      workspaces,
    })
    expect(listWorkspacesForRequest).toHaveBeenCalledOnce()
    expect(listWorkspacesForRequest).toHaveBeenCalledWith(request, 'coral-access-token')
  })

  it('leaves workspace lookup to the index redirect loader', async () => {
    await expect(
      loader(authRouteTestArgs(new Request(`http://reef.test${routePath('home')}`), {})),
    ).resolves.toEqual({
      auth: { csrfToken: 'test-csrf-token', mode: 'required' },
      workspaces: [],
    })
    expect(listWorkspacesForRequest).not.toHaveBeenCalled()
  })

  it('falls back to an empty sidebar when workspace loading fails', async () => {
    const error = new Error('sidecar unavailable')
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    listWorkspacesForRequest.mockRejectedValue(error)

    await expect(
      loader(authRouteTestArgs(new Request('http://reef.test/workspaces/default/sources'), {})),
    ).resolves.toEqual({
      auth: { csrfToken: 'test-csrf-token', mode: 'required' },
      workspaces: [],
    })
    expect(consoleError).toHaveBeenCalledWith('Failed to load sidebar workspaces:', error)
  })

  it('keeps local sidebar auth disabled', async () => {
    const request = new Request('http://reef.test/workspaces/default/sources')
    listWorkspacesForRequest.mockResolvedValue([{ name: 'default' }])

    await expect(loader(authRouteTestArgs(request, {}, null))).resolves.toEqual({
      auth: { mode: 'disabled' },
      workspaces: [{ name: 'default' }],
    })
    expect(listWorkspacesForRequest).toHaveBeenCalledWith(request, null)
  })
})

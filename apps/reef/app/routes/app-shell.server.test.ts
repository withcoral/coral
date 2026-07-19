import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const { listWorkspacesForRequest } = vi.hoisted(() => ({
  listWorkspacesForRequest: vi.fn(),
}))

vi.mock('@/lib/workspaces.server', () => ({ listWorkspacesForRequest }))

import { routePath } from '@/routing/routemap'

import { loader } from './app-shell'

describe('app shell loader', () => {
  beforeEach(() => {
    listWorkspacesForRequest.mockReset()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('loads the local workspace list once for the sidebar', async () => {
    const request = new Request('http://reef.test/workspaces/default/sources')
    const workspaces = [{ name: 'default' }, { name: 'analytics' }]
    listWorkspacesForRequest.mockResolvedValue(workspaces)

    await expect(loader({ request } as Parameters<typeof loader>[0])).resolves.toEqual({
      workspaces,
    })
    expect(listWorkspacesForRequest).toHaveBeenCalledOnce()
    expect(listWorkspacesForRequest).toHaveBeenCalledWith(request)
  })

  it('leaves workspace lookup to the index redirect loader', async () => {
    await expect(
      loader({
        request: new Request(`http://reef.test${routePath('home')}`),
      } as Parameters<typeof loader>[0]),
    ).resolves.toEqual({ workspaces: [] })
    expect(listWorkspacesForRequest).not.toHaveBeenCalled()
  })

  it('falls back to an empty sidebar when workspace loading fails', async () => {
    const error = new Error('sidecar unavailable')
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    listWorkspacesForRequest.mockRejectedValue(error)

    await expect(
      loader({
        request: new Request('http://reef.test/workspaces/default/sources'),
      } as Parameters<typeof loader>[0]),
    ).resolves.toEqual({ workspaces: [] })
    expect(consoleError).toHaveBeenCalledWith('Failed to load sidebar workspaces:', error)
  })
})

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const { listWorkspacesForRequest } = vi.hoisted(() => ({
  listWorkspacesForRequest: vi.fn(),
}))

vi.mock('@/lib/workspaces.server', () => ({ listWorkspacesForRequest }))

import { WorkspaceRole } from '@/generated/coral/v1/workspaces_pb'
import { routePath } from '@/routing/routemap'

import { loader } from './app-shell'

describe('app shell loader', () => {
  beforeEach(() => {
    listWorkspacesForRequest.mockReset()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('loads the memberships once and keeps every role', async () => {
    const request = new Request('http://reef.test/workspaces/default/sources')
    listWorkspacesForRequest.mockResolvedValue([
      { role: WorkspaceRole.OWNER, workspace: { name: 'default' } },
      { role: WorkspaceRole.MEMBER, workspace: { name: 'analytics' } },
    ])

    // Written out instead of reusing the mocked value: a loader that projected memberships back
    // down to bare workspaces, or blanked the role, fails this comparison.
    await expect(loader({ request } as Parameters<typeof loader>[0])).resolves.toEqual({
      memberships: [
        { role: WorkspaceRole.OWNER, workspace: { name: 'default' } },
        { role: WorkspaceRole.MEMBER, workspace: { name: 'analytics' } },
      ],
    })
    expect(listWorkspacesForRequest).toHaveBeenCalledOnce()
    expect(listWorkspacesForRequest).toHaveBeenCalledWith(request)
  })

  it('passes through a membership the server sent without a workspace unchanged', async () => {
    listWorkspacesForRequest.mockResolvedValue([{ role: WorkspaceRole.MEMBER }])

    await expect(
      loader({
        request: new Request('http://reef.test/workspaces/default/sources'),
      } as Parameters<typeof loader>[0]),
    ).resolves.toEqual({ memberships: [{ role: WorkspaceRole.MEMBER }] })
  })

  it('leaves workspace lookup to the index redirect loader', async () => {
    await expect(
      loader({
        request: new Request(`http://reef.test${routePath('home')}`),
      } as Parameters<typeof loader>[0]),
    ).resolves.toEqual({ memberships: [] })
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
    ).resolves.toEqual({ memberships: [] })
    expect(consoleError).toHaveBeenCalledWith('Failed to load sidebar workspaces:', error)
  })
})

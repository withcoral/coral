import { beforeEach, describe, expect, it, vi } from 'vitest'

const { listWorkspaces, workspaceClientForRequest } = vi.hoisted(() => ({
  listWorkspaces: vi.fn(),
  workspaceClientForRequest: vi.fn(),
}))

vi.mock('@/lib/coral-request.server', () => ({ workspaceClientForRequest }))

import { firstWorkspaceForRequest, listWorkspacesForRequest } from './workspaces.server'

describe('local workspaces', () => {
  beforeEach(() => {
    listWorkspaces.mockReset()
    workspaceClientForRequest.mockReset()
    workspaceClientForRequest.mockReturnValue({ listWorkspaces })
  })

  it('lists workspaces through the local WorkspaceService and selects the first', async () => {
    const request = new Request('http://localhost/')
    listWorkspaces.mockResolvedValue({
      memberships: [
        { workspace: { name: 'default' }, role: 1 },
        { workspace: { name: 'analytics' }, role: 2 },
      ],
    })

    await expect(listWorkspacesForRequest(request)).resolves.toEqual([
      { workspace: { name: 'default' }, role: 1 },
      { workspace: { name: 'analytics' }, role: 2 },
    ])
    await expect(firstWorkspaceForRequest(request)).resolves.toEqual({ name: 'default' })
    expect(workspaceClientForRequest).toHaveBeenCalledWith(request)
  })

  it('returns a clear route error when no local workspace exists', async () => {
    listWorkspaces.mockResolvedValue({ memberships: [] })

    await expect(firstWorkspaceForRequest(new Request('http://localhost/'))).rejects.toMatchObject({
      status: 404,
      statusText: 'Workspace Not Found',
    })
  })
})

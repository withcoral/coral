import { beforeEach, describe, expect, it, vi } from 'vitest'

const { listWorkspaces, workspaceClientForRequest } = vi.hoisted(() => ({
  listWorkspaces: vi.fn(),
  workspaceClientForRequest: vi.fn(),
}))

vi.mock('@/lib/coral-request.server', () => ({ workspaceClientForRequest }))

import { WorkspaceRole } from '@/generated/coral/v1/workspaces_pb'

import { firstWorkspaceForRequest, listWorkspacesForRequest } from './workspaces.server'

describe('local workspaces', () => {
  beforeEach(() => {
    listWorkspaces.mockReset()
    workspaceClientForRequest.mockReset()
    workspaceClientForRequest.mockReturnValue({ listWorkspaces })
  })

  it('lists memberships with their roles and selects the first workspace', async () => {
    const request = new Request('http://localhost/')
    listWorkspaces.mockResolvedValue({
      memberships: [
        { role: WorkspaceRole.OWNER, workspace: { name: 'default' } },
        { role: WorkspaceRole.MEMBER, workspace: { name: 'analytics' } },
      ],
    })

    // Spelled out rather than compared against the mocked array so that dropping `role`
    // anywhere between the transport and the caller fails here.
    await expect(listWorkspacesForRequest(request)).resolves.toEqual([
      { role: WorkspaceRole.OWNER, workspace: { name: 'default' } },
      { role: WorkspaceRole.MEMBER, workspace: { name: 'analytics' } },
    ])
    await expect(firstWorkspaceForRequest(request)).resolves.toEqual({ name: 'default' })
    expect(workspaceClientForRequest).toHaveBeenCalledWith(request)
  })

  it('skips a membership the server sent without a workspace resource', async () => {
    listWorkspaces.mockResolvedValue({
      memberships: [
        { role: WorkspaceRole.MEMBER },
        { role: WorkspaceRole.OWNER, workspace: { name: 'analytics' } },
      ],
    })

    await expect(firstWorkspaceForRequest(new Request('http://localhost/'))).resolves.toEqual({
      name: 'analytics',
    })
  })

  it('returns a clear route error when no local workspace exists', async () => {
    listWorkspaces.mockResolvedValue({ memberships: [] })

    await expect(firstWorkspaceForRequest(new Request('http://localhost/'))).rejects.toMatchObject({
      status: 404,
      statusText: 'Workspace Not Found',
    })
  })

  it('refuses to invent a workspace when no membership carries one', async () => {
    listWorkspaces.mockResolvedValue({ memberships: [{ role: WorkspaceRole.MEMBER }] })

    await expect(firstWorkspaceForRequest(new Request('http://localhost/'))).rejects.toMatchObject({
      status: 404,
      statusText: 'Workspace Not Found',
    })
  })
})

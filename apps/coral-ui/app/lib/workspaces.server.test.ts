import { describe, expect, it, vi } from 'vitest'

const coralRequestMocks = vi.hoisted(() => ({
  workspaceClientForRequest: vi.fn(),
}))

vi.mock('@/lib/coral-request.server', () => ({
  workspaceClientForRequest: coralRequestMocks.workspaceClientForRequest,
}))

import {
  DEFAULT_WORKSPACE_ID,
  impliedWorkspaceForRequest,
  listWorkspacesForRequest,
} from './workspaces.server'

const request = new Request('http://localhost:5173/')

function answering(memberships: unknown[]) {
  coralRequestMocks.workspaceClientForRequest.mockReturnValue({
    listWorkspaces: () => Promise.resolve({ memberships }),
  })
}

describe('listWorkspacesForRequest', () => {
  it('reads the workspace out of every membership', async () => {
    answering([
      { workspace: { name: 'analytics' }, role: 1 },
      { workspace: { name: 'reporting' }, role: 2 },
    ])

    await expect(listWorkspacesForRequest(request, null)).resolves.toEqual([
      { name: 'analytics' },
      { name: 'reporting' },
    ])
  })

  it('reports a membership that carries no workspace', async () => {
    answering([{ workspace: { name: 'analytics' }, role: 1 }, { role: 2 }])

    await expect(listWorkspacesForRequest(request, null)).rejects.toThrow(
      'Coral returned a workspace membership without a workspace',
    )
  })

  it('answers a caller who belongs to nothing with an empty list', async () => {
    answering([])

    await expect(listWorkspacesForRequest(request, null)).resolves.toEqual([])
  })
})

describe('impliedWorkspaceForRequest', () => {
  it('prefers the default workspace over the memberships listed before it', async () => {
    answering([
      { workspace: { name: 'analytics' }, role: 1 },
      { workspace: { name: DEFAULT_WORKSPACE_ID }, role: 2 },
    ])

    await expect(impliedWorkspaceForRequest(request, null)).resolves.toEqual({
      name: DEFAULT_WORKSPACE_ID,
    })
  })

  it('falls back to the first workspace when no membership names the default', async () => {
    answering([
      { workspace: { name: 'analytics' }, role: 1 },
      { workspace: { name: 'reporting' }, role: 2 },
    ])

    await expect(impliedWorkspaceForRequest(request, null)).resolves.toEqual({
      name: 'analytics',
    })
  })

  it('answers 404 only when the caller genuinely belongs to nothing', async () => {
    answering([])

    const refusal = await impliedWorkspaceForRequest(request, null).catch(
      (thrown: unknown) => thrown,
    )
    expect(refusal).toBeInstanceOf(Response)
    expect((refusal as Response).status).toBe(404)
  })

  // The two failures used to be one: a malformed membership was dropped, the
  // list came back empty, and the caller was told no workspace was configured.
  // Telling those apart is the whole point of reporting the malformed one.
  it('does not answer a malformed response with the not-configured 404', async () => {
    answering([{ role: 2 }])

    const thrown = await impliedWorkspaceForRequest(request, null).catch((error: unknown) => error)
    expect(thrown).not.toBeInstanceOf(Response)
    expect(thrown).toBeInstanceOf(Error)
    expect((thrown as Error).message).toBe(
      'Coral returned a workspace membership without a workspace',
    )
  })
})

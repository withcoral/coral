import { create } from '@bufbuild/protobuf'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const { createWorkspace } = vi.hoisted(() => ({ createWorkspace: vi.fn() }))

vi.mock('@/lib/coral-request.server', () => ({
  workspaceClientForRequest: () => ({ createWorkspace }),
}))

import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'

import { action } from './workspaces-action'

function createRequest(name: string, intent = 'create') {
  return new Request('http://reef.test/workspaces', {
    body: new URLSearchParams({ intent, name }),
    method: 'POST',
  })
}

describe('create workspace action', () => {
  beforeEach(() => {
    createWorkspace.mockReset()
  })

  it('rejects unsupported intents before mutation', async () => {
    const result = await action({
      request: createRequest('analytics', 'rename'),
    } as Parameters<typeof action>[0])

    expect(result).toMatchObject({
      data: { error: 'Unsupported workspace action.', name: '' },
      init: { status: 400 },
    })
    expect(createWorkspace).not.toHaveBeenCalled()
  })

  it.each([
    ['', 'Workspace name is required'],
    ['Team', 'Workspace name may only contain lowercase letters, numbers, and hyphens'],
    ['team_name', 'Workspace name may only contain lowercase letters, numbers, and hyphens'],
    ['-team', 'Workspace name must not start or end with a hyphen'],
    ['team-', 'Workspace name must not start or end with a hyphen'],
    ['a'.repeat(64), 'Workspace name must be 63 characters or fewer'],
  ])('rejects invalid workspace name %j before mutation', async (name, error) => {
    const result = await action({ request: createRequest(name) } as Parameters<typeof action>[0])

    expect(result).toMatchObject({
      data: { error, name },
      init: { status: 400 },
      type: 'DataWithResponseInit',
    })
    expect(createWorkspace).not.toHaveBeenCalled()
  })

  it('creates the local workspace and redirects to its sources', async () => {
    createWorkspace.mockResolvedValue({
      workspace: create(WorkspaceSchema, { name: 'analytics' }),
    })

    const result = await action({
      request: createRequest('analytics'),
    } as Parameters<typeof action>[0])

    expect(createWorkspace).toHaveBeenCalledOnce()
    expect(createWorkspace).toHaveBeenCalledWith(
      expect.objectContaining({
        workspace: expect.objectContaining({ name: 'analytics' }),
      }),
    )
    expect(result).toBeInstanceOf(Response)
    expect((result as Response).status).toBe(302)
    expect((result as Response).headers.get('Location')).toBe('/workspaces/analytics/sources')
  })

  it('returns service errors to the dialog without redirecting', async () => {
    createWorkspace.mockRejectedValue(new Error('workspace already exists'))

    await expect(
      action({ request: createRequest('analytics') } as Parameters<typeof action>[0]),
    ).resolves.toMatchObject({
      data: { error: 'workspace already exists', name: 'analytics' },
      init: { status: 502 },
    })
  })
})

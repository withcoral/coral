import { create } from '@bufbuild/protobuf'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const { createWorkspace, workspaceClientForRequest } = vi.hoisted(() => {
  const createWorkspaceMock = vi.fn()
  return {
    createWorkspace: createWorkspaceMock,
    workspaceClientForRequest: vi.fn(() => ({ createWorkspace: createWorkspaceMock })),
  }
})

vi.mock('@/lib/coral-request.server', () => ({
  workspaceClientForRequest,
}))

import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'

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
    workspaceClientForRequest.mockClear()
  })

  it('rejects unsupported intents before mutation', async () => {
    const result = await action(authRouteTestArgs(createRequest('analytics', 'rename'), {}))

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
    const result = await action(authRouteTestArgs(createRequest(name), {}))

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

    const request = createRequest('analytics')
    const result = await action(authRouteTestArgs(request, {}))

    expect(createWorkspace).toHaveBeenCalledOnce()
    expect(createWorkspace).toHaveBeenCalledWith(
      expect.objectContaining({
        workspace: expect.objectContaining({ name: 'analytics' }),
      }),
    )
    expect(workspaceClientForRequest).toHaveBeenCalledWith(request, 'test-coral-token')
    expect(result).toBeInstanceOf(Response)
    expect((result as Response).status).toBe(302)
    expect((result as Response).headers.get('Location')).toBe('/workspaces/analytics/sources')
  })

  it('keeps local workspace creation unauthenticated', async () => {
    createWorkspace.mockResolvedValue({
      workspace: create(WorkspaceSchema, { name: 'analytics' }),
    })

    const request = createRequest('analytics')
    await action(authRouteTestArgs(request, {}, null))

    expect(workspaceClientForRequest).toHaveBeenCalledWith(request, null)
  })

  it('returns service errors to the dialog without redirecting', async () => {
    createWorkspace.mockRejectedValue(new Error('workspace already exists'))

    await expect(action(authRouteTestArgs(createRequest('analytics'), {}))).resolves.toMatchObject({
      data: { error: 'workspace already exists', name: 'analytics' },
      init: { status: 502 },
    })
  })
})

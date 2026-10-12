import { expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { loader } from './index'

const listed = vi.hoisted(() => ({ workspaces: [] as { name: string; state: number }[] }))
const transport = fakeManagementTransport({
  workspaces: { listWorkspaces: () => ({ workspaces: listed.workspaces, nextPageToken: '' }) },
})
vi.mock('@/lib/coral-request.server', () => ({ managementTransportForRequest: () => transport }))

const archived = 'workspaces/00000000-0000-4000-8000-000000000001'
const active = 'workspaces/00000000-0000-4000-8000-000000000002'

it.each([
  [
    'opens the first active Workspace, skipping archived ones',
    [
      { name: archived, state: WorkspaceState.ARCHIVED },
      { name: active, state: WorkspaceState.ACTIVE },
    ],
    `/${active}/activity`,
  ],
  ['stays home without a Workspace', [], null],
  [
    'stays home when every Workspace is archived',
    [{ name: archived, state: WorkspaceState.ARCHIVED }],
    null,
  ],
])('%s', async (_, workspaces, location) => {
  listed.workspaces = workspaces
  const result = await loader(authRouteTestArgs(new Request('http://reef/'), {}, null))
  if (location === null) expect(result).toBeNull()
  else {
    expect(result).toBeInstanceOf(Response)
    expect((result as Response).headers.get('location')).toBe(location)
  }
})

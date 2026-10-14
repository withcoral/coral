import { expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { loader } from './app-shell'

const transport = fakeManagementTransport({
  workspaces: {
    listWorkspaces: ({ pageToken }) =>
      pageToken === ''
        ? {
            workspaces: [workspace(1), workspace(2, WorkspaceState.ARCHIVED)],
            nextPageToken: 'p2',
          }
        : { workspaces: [workspace(3)], nextPageToken: '' },
  },
  organizationMemberships: { getCurrentOrganizationMembership: () => ({}) },
})
vi.mock('@/lib/coral-request.server', () => ({ managementTransportForRequest: () => transport }))

function workspace(id: number, state = WorkspaceState.ACTIVE) {
  return {
    name: `workspaces/00000000-0000-4000-8000-00000000000${id}`,
    displayName: `Workspace ${id}`,
    state,
    effectiveRole: WorkspaceRole.MEMBER,
    mcpEndpoint: `https://coral.example.com/mcp/${id}`,
  }
}

const paths = (list: { path: string }[]) => list.map((w) => w.path)

it('lists every Workspace across pages, archived ones apart, because the sidebar is the only listing', async () => {
  const result = await loader(authRouteTestArgs(new Request('http://reef/activity'), {}, null))
  expect(paths(result.data.workspaces)).toEqual([
    '/workspaces/00000000-0000-4000-8000-000000000001',
    '/workspaces/00000000-0000-4000-8000-000000000003',
  ])
  expect(paths(result.data.archivedWorkspaces)).toEqual([
    '/workspaces/00000000-0000-4000-8000-000000000002',
  ])
})

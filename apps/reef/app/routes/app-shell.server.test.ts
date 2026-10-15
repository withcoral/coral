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
    etag: `etag-${id}`,
  }
}

// What the sidebar shows for the fixture Workspace `id`.
function sidebarEntry(id: number, state: 'active' | 'archived') {
  return {
    displayName: `Workspace ${id}`,
    path: `/workspaces/00000000-0000-4000-8000-00000000000${id}`,
    effectiveRole: 'member',
    mcpEndpoint: `https://coral.example.com/mcp/${id}`,
    etag: `etag-${id}`,
    state,
  }
}

it('lists every Workspace across pages, archived ones apart, because the sidebar is the only listing', async () => {
  const result = await loader(authRouteTestArgs(new Request('http://reef/activity'), {}, null))
  expect(result.data.workspaces).toEqual([sidebarEntry(1, 'active'), sidebarEntry(3, 'active')])
  expect(result.data.archivedWorkspaces).toEqual([sidebarEntry(2, 'archived')])
})

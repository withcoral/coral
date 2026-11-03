import { Code, ConnectError } from '@connectrpc/connect'
import { expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { OrganizationRole } from '@/generated/coral/management/v1/organization_membership_pb'
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
  organizationMemberships: { getCurrentOrganizationMembership: () => ({ role }) },
  releases: {
    getNewerRelease: () => {
      if (release === 'unavailable') throw new ConnectError('down', Code.Unavailable)
      return { release }
    },
  },
})
let role = OrganizationRole.UNSPECIFIED
let release: { version: string; notesUrl: string } | undefined | 'unavailable'
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

it.each([
  ['an owner, with a newer release', OrganizationRole.OWNER, '1.4.0', true],
  ['a member, with a newer release', OrganizationRole.MEMBER, '1.4.0', false],
  ['an owner, with nothing newer', OrganizationRole.OWNER, undefined, false],
  ['an owner, when Coral cannot answer', OrganizationRole.OWNER, 'unavailable', false],
] as const)(
  'offers the release notes to %s only when an owner can act on them',
  async (_case, caseRole, version, shown) => {
    role = caseRole
    const notesUrl = 'https://github.com/withcoral/coral/releases/tag/v1.4.0'
    release = version === '1.4.0' ? { version, notesUrl } : version
    const result = await loader(authRouteTestArgs(new Request('http://reef/activity'), {}, null))
    expect(result.data.newerRelease).toEqual(shown ? { version: '1.4.0', notesUrl } : undefined)
  },
)

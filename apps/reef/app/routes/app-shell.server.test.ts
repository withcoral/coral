import type { ShouldRevalidateFunctionArgs } from 'react-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { Edition, EnterpriseFeatureState } from '@/generated/coral/management/v1/edition_pb'
import { OrganizationRole } from '@/generated/coral/management/v1/organization_membership_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { loader, shouldRevalidate } from './app-shell'

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
  secretStores: {
    listSecretStores: () => {
      if (secretStores === 'unavailable') throw new ConnectError('down', Code.Unavailable)
      return { secretStores: [], disabled: secretStores === 'off' }
    },
  },
  editions: {
    getEdition: () => {
      if (edition === 'unavailable') throw new ConnectError('down', Code.Unavailable)
      return { ...edition, features: [...edition.features] }
    },
  },
  releases: {
    getNewerRelease: () => {
      if (release === 'unavailable') throw new ConnectError('down', Code.Unavailable)
      return { release }
    },
  },
})
let role = OrganizationRole.UNSPECIFIED
let release: { version: string; notesUrl: string } | undefined | 'unavailable'
let edition:
  | {
      edition: Edition
      features: readonly { id: string; displayName: string; state: EnterpriseFeatureState }[]
    }
  | 'unavailable' = { edition: Edition.COMMUNITY, features: [] }
let secretStores: 'none defined' | 'off' | 'unavailable' = 'none defined'
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

// Catches: a workspace on a later page missing from the switcher, or an archived
// one listed among the workspaces you can work in.
it('lists the active Workspaces across pages, and no archived one', async () => {
  const result = await loader(authRouteTestArgs(new Request('http://reef/activity'), {}, null))
  expect(result.data.workspaces).toEqual([sidebarEntry(1, 'active'), sidebarEntry(3, 'active')])
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

// Catches: an owner losing the section while their deployment defines no store
// yet, which hides how to define one, or Desktop, which turned them off, keeping it.
it.each([
  ['an owner, with none defined', OrganizationRole.OWNER, 'none defined', true],
  ['an owner, when Coral cannot answer', OrganizationRole.OWNER, 'unavailable', true],
  ['an owner, where the deployment turned them off', OrganizationRole.OWNER, 'off', false],
  ['a member', OrganizationRole.MEMBER, 'none defined', false],
] as const)(
  'offers secret stores to %s only while the deployment has them',
  async (_case, caseRole, stores, shown) => {
    role = caseRole
    secretStores = stores
    const result = await loader(authRouteTestArgs(new Request('http://reef/activity'), {}, null))
    expect(result.data.secretStoresEnabled).toBe(shown)
  },
)

const feature = { id: 'example-feature', displayName: 'Example feature' }

// Catches: a page that cannot tell whether an Enterprise Feature works, or one
// that shows a feature as missing when Coral could not answer.
it.each([
  [
    'a source build',
    { edition: Edition.COMMUNITY, features: [] },
    { edition: 'community', features: [] },
  ],
  [
    'a released build without a license',
    {
      edition: Edition.COMMUNITY,
      features: [{ ...feature, state: EnterpriseFeatureState.NEEDS_ENTERPRISE }],
    },
    { edition: 'community', features: [{ ...feature, state: 'needs_enterprise' }] },
  ],
  [
    'a released build with a license',
    {
      edition: Edition.ENTERPRISE,
      features: [{ ...feature, state: EnterpriseFeatureState.ENABLED }],
    },
    { edition: 'enterprise', features: [{ ...feature, state: 'enabled' }] },
  ],
  ['Coral when it cannot answer', 'unavailable', null],
] as const)('gives every page the edition of %s', async (_case, answer, expected) => {
  edition = answer
  const result = await loader(authRouteTestArgs(new Request('http://reef/activity'), {}, null))
  expect(result.data.edition).toEqual(expected)
})

// A row opens its review through the query. The sidebar must not reload for it, but a refresh
// and a move to another page must still read the shell.
it.each([
  ['opening a review', '/workspaces/one/approvals', '/workspaces/one/approvals?request=a', false],
  ['closing a review', '/workspaces/one/approvals?request=a', '/workspaces/one/approvals', false],
  ['refreshing the open review', '/workspaces/one/approvals?request=a', '', true],
  ['leaving for another page', '/workspaces/one/approvals', '/workspaces/one/activity', true],
  // A tool's drawer opens over its group, a provider or the private tools.
  ['opening a tool', '/workspaces/one/providers/p', '/workspaces/one/providers/p/tools/t', false],
  [
    'opening a private tool',
    '/workspaces/one/tools/private',
    '/workspaces/one/tools/private/t',
    false,
  ],
  [
    'opening provider settings',
    '/workspaces/one/providers/p',
    '/workspaces/one/providers/p/settings',
    true,
  ],
  ['changing group', '/workspaces/one/tools/private', '/workspaces/one/providers/p', true],
  [
    'changing the query of another page',
    '/workspaces/one/activity',
    '/workspaces/one/activity?x=1',
    true,
  ],
])('%s', (_, from, to, expected) => {
  const currentUrl = new URL(from, 'http://reef.test')
  const nextUrl = new URL(to || from, 'http://reef.test')
  expect(
    shouldRevalidate({
      currentUrl,
      nextUrl,
      defaultShouldRevalidate: true,
    } as ShouldRevalidateFunctionArgs),
  ).toBe(expected)
})

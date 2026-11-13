import { Code, ConnectError } from '@connectrpc/connect'
import { expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { ProviderState } from '@/generated/coral/management/v1/provider_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { SecretStoreState } from '@/generated/coral/management/v1/secret_store_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { loader } from './workspace'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))
const workspace = '00000000-0000-4000-8000-000000000001'
const parent = `workspaces/${workspace}`

/** A summary that never answers, unless a test gives one. */
function fakeCoral(
  summaries: number[] = [],
  summarize: () => Promise<object> = () => new Promise(() => {}),
  role = WorkspaceRole.OWNER,
) {
  coral.transport = fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => ({
        name,
        displayName: 'Overview',
        effectiveRole: role,
        state: WorkspaceState.ACTIVE,
      }),
    },
    agents: { listAgents: () => ({ agents: [] }) },
    providers: {
      listProviders: () => ({
        providers: [
          {
            name: `${parent}/providers/builtin`,
            displayName: 'Coral',
            callRoot: 'coral',
            builtin: true,
            state: ProviderState.ACTIVE,
          },
          {
            name: `${parent}/providers/added`,
            displayName: 'Weather',
            namespace: 'weather',
            callRoot: 'coral.providers.weather',
            state: ProviderState.ACTIVE,
          },
        ],
      }),
    },
    memberships: {
      listWorkspaceOwners: () => ({ owners: [] }),
      getWorkspaceMembership: ({ name }) => ({ name }),
    },
    secretStores: {
      listWorkspaceSecretStores: () => {
        if (role !== WorkspaceRole.OWNER)
          throw new ConnectError('permission denied', Code.PermissionDenied)
        return {
          secretStores: [
            { name: 'secretStores/platform', state: SecretStoreState.AVAILABLE },
            { name: 'secretStores/licensed', state: SecretStoreState.NEEDS_ENTERPRISE },
            { name: 'secretStores/removed', state: SecretStoreState.REMOVED },
          ],
        }
      },
    },
    privateTools: { listPrivateTools: () => ({ privateTools: [] }) },
    activity: {
      browseActivityRoots: () => ({ roots: [] }),
      browseRequiredActions: () => ({ actions: [] }),
      summarizeActivity: ({ days }) => {
        summaries.push(days)
        return summarize()
      },
    },
  })
}

const overview = (search = '') =>
  loader(authRouteTestArgs(new Request(`http://reef/${parent}${search}`), { workspace }, null))

// Catches: a new workspace without Add provider, or built-in calls the summary cannot name or open.
it('leaves the Built-in Provider out of the providers card, but not out of the summary', async () => {
  fakeCoral()
  const result = await overview()
  if (result instanceof Response) throw new Error('The workspace page renders')
  expect(result.data.providers.map((provider) => provider.displayName)).toEqual(['Weather'])
  expect(result.data.activity.providers.map((provider) => provider.callRoot)).toEqual([
    'coral',
    'coral.providers.weather',
  ])
})

// Catches: an owner shown a resource name, or a store Coral cannot read shown as usable.
it('shows an owner the granted secret stores and why Coral cannot read them', async () => {
  fakeCoral()
  const result = await overview()
  if (result instanceof Response) throw new Error('The workspace page renders')
  expect(result.data.secretStores).toEqual([
    { name: 'platform', blockers: [] },
    { name: 'licensed', blockers: ['Needs Enterprise'] },
    { name: 'removed', blockers: ['Removed from config'] },
  ])
})

// Catches: the page failing for a member, whom Coral refuses this list.
it('does not ask for the secret stores for a member', async () => {
  fakeCoral([], undefined, WorkspaceRole.MEMBER)
  const result = await overview()
  if (result instanceof Response) throw new Error('The workspace page renders')
  expect(result.data.secretStores).toEqual([])
})

// Catches: an overview that waits for a slow summary, or a reload showing another period
// than the URL names.
it.each([
  ['the default period', '', 7],
  ['a period the URL names', '?days=30', 30],
  ['the last 24 hours', '?days=1', 1],
])('renders %s while the summary has not answered', async (_, search, days) => {
  const summaries: number[] = []
  fakeCoral(summaries)
  const result = await overview(search)
  if (result instanceof Response) throw new Error('The workspace page renders')
  expect(result.data.activity.days).toBe(days)
  expect(summaries).toEqual([days])
})

// Catches: an outage streamed as zeros. activity-summary.server.test.ts owns which codes
// read as unavailable.
it('streams a summary Coral cannot give now as unavailable', async () => {
  fakeCoral([], () => Promise.reject(new ConnectError('down', Code.Unavailable)))
  const result = await overview()
  if (result instanceof Response) throw new Error('The workspace page renders')
  await expect(result.data.activity.summary).resolves.toBeNull()
})

// Catches: a URL that names one period while the page shows another.
it.each([
  ['a period the page does not offer', '?days=3', `/${parent}`],
  ['a period that is not a number', '?days=week', `/${parent}`],
  ['the other query it keeps', '?days=3&created=a', `/${parent}?created=a`],
])('redirects %s to the default period', async (_, search, location) => {
  const summaries: number[] = []
  fakeCoral(summaries)
  const result = await overview(search)
  if (!(result instanceof Response)) throw new Error('The loader redirects')
  expect(result.headers.get('location')).toBe(location)
  expect(summaries).toEqual([])
})

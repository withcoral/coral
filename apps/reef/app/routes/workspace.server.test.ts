import { expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { ProviderState } from '@/generated/coral/management/v1/provider_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { loader } from './workspace'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))
const workspace = '00000000-0000-4000-8000-000000000001'
const parent = `workspaces/${workspace}`

function fakeCoral(summaries: number[] = []) {
  coral.transport = fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => ({
        name,
        displayName: 'Overview',
        effectiveRole: WorkspaceRole.OWNER,
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
            callRoot: 'coral.workspace',
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
    memberships: { listWorkspaceOwners: () => ({ owners: [] }) },
    savedFunctions: { listSavedFunctions: () => ({ savedFunctions: [] }) },
    activity: {
      browseActivityRoots: () => ({ roots: [] }),
      browseRequiredActions: () => ({ actions: [] }),
      summarizeActivity: ({ days }) => {
        summaries.push(days)
        return {}
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
    'coral.workspace',
    'coral.providers.weather',
  ])
})

// Catches: a reload showing another period than the URL names.
it.each([
  ['the default period', '', 7],
  ['a period the URL names', '?days=30', 30],
  ['the last 24 hours', '?days=1', 1],
])('summarizes %s', async (_, search, days) => {
  const summaries: number[] = []
  fakeCoral(summaries)
  const result = await overview(search)
  if (result instanceof Response) throw new Error('The workspace page renders')
  expect(result.data.activity.days).toBe(days)
  expect(summaries).toEqual([days])
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

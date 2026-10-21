import { expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { ProviderState } from '@/generated/coral/management/v1/provider_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { loader, shouldRevalidate } from './workspace'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))
const workspace = '00000000-0000-4000-8000-000000000001'
const parent = `workspaces/${workspace}`

it('leaves the Built-in Provider out of the overview, so a new workspace offers Add provider', async () => {
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
      summarizeActivity: () => ({}),
    },
  })
  const result = await loader(
    authRouteTestArgs(new Request(`http://reef/${parent}`), { workspace }, null),
  )
  if (result instanceof Response) throw new Error('The workspace page renders')
  expect(result.data.providers.map((provider) => provider.displayName)).toEqual(['Weather'])
})

// Catches: a period change reloading every card, or a real navigation keeping stale cards.
const overview = `http://reef/workspaces/${workspace}`
it.each([
  ['a new summary period keeps the cards', overview, `${overview}?days=30`, undefined, false],
  ['the default period keeps the cards', `${overview}?days=1`, overview, undefined, false],
  ['a refresh reloads them', `${overview}?days=30`, `${overview}?days=30`, undefined, true],
  ['another query change reloads them', overview, `${overview}?days=30&created=a`, undefined, true],
  ['a submission reloads them', overview, `${overview}?days=30`, 'POST', true],
] as const)('%s', (_case, current, next, formMethod, expected) => {
  expect(
    shouldRevalidate({
      currentUrl: new URL(current),
      nextUrl: new URL(next),
      formMethod,
      defaultShouldRevalidate: true,
    } as Parameters<typeof shouldRevalidate>[0]),
  ).toBe(expected)
})

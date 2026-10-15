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
            callRoot: 'coral.admin',
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
    },
  })
  const result = await loader(
    authRouteTestArgs(new Request(`http://reef/${parent}`), { workspace }, null),
  )
  if (result instanceof Response) throw new Error('The workspace page renders')
  expect(result.data.providers.map((provider) => provider.displayName)).toEqual(['Weather'])
})

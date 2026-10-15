import { ConnectError, Code } from '@connectrpc/connect'
import { expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { ActivityInvocationState } from '@/generated/coral/management/v1/activity_pb'
import { AgentState } from '@/generated/coral/management/v1/agent_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport, type ManagementHandlers } from '@/lib/management.test-support'
import { RECIPE_CHOICES } from '@/lib/provider-recipes/library.server'
import { action, loader } from './agent'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))
const workspace = '00000000-0000-4000-8000-000000000001'
const agent = 'retained-agent'
const personalWorkspace = 'workspaces/00000000-0000-4000-8000-000000000010'
const personalTask = `${personalWorkspace}/tasks/00000000-0000-4000-8000-000000000011`
const personalInvocation = `${personalWorkspace}/invocations/00000000-0000-4000-8000-000000000012`
function api(contextError?: Code, selectionError?: Code) {
  return fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => ({
        name,
        displayName: 'History',
        effectiveRole: WorkspaceRole.OWNER,
        state: WorkspaceState.ACTIVE,
      }),
    },
    agents: {
      getAgent: () => {
        throw new ConnectError('removed', Code.NotFound)
      },
    },
    registrations: {
      getAgentRegistration: () => {
        throw new ConnectError('removed', Code.NotFound)
      },
    },
    activity: {
      inspectAgentHistory: ({ name }) => {
        if (contextError) throw new ConnectError('agent context unavailable', contextError)
        return { name, displayName: 'Retained worker', deleted: true }
      },
      browseActivityRoots: () => {
        if (contextError) throw new ConnectError('agent list unavailable', contextError)
        return {
          roots: [
            {
              record: {
                case: 'task',
                value: {
                  name: `workspaces/${workspace}/tasks/00000000-0000-4000-8000-000000000002`,
                  workspace: `workspaces/${workspace}`,
                  intent: 'Retained goal',
                  outcome: 'open',
                },
              },
            },
          ],
        }
      },
      inspectTask: ({ name }) => {
        if (selectionError) throw new ConnectError('selection unavailable', selectionError)
        return {
          task: {
            name,
            workspace: personalWorkspace,
            intent: 'Still-authorized personal goal',
            outcome: 'open',
          },
        }
      },
      inspectRun: () => {
        // Parent-source context is independently authorized. An unavailable
        // parent must not conceal the selected personal invocation's own evidence.
        throw new ConnectError('parent run unavailable', Code.NotFound)
      },
      inspectInvocation: ({ name }) => {
        if (selectionError) throw new ConnectError('selection unavailable', selectionError)
        return {
          invocation: {
            name,
            workspace: personalWorkspace,
            state: ActivityInvocationState.PENDING,
            run: `${personalWorkspace}/runs/00000000-0000-4000-8000-000000000013`,
          },
          argumentsJson: '{"marker":"personal exact evidence"}',
        }
      },
    },
  })
}
const args = (query = '') =>
  authRouteTestArgs(
    new Request(`http://reef/workspaces/${workspace}/agents/${agent}${query}`),
    { workspace, agent },
    null,
  )
it('renders retained Agent activity without restoring a current Registration or management controls', async () => {
  coral.transport = api()
  const result = await loader(args())
  expect(result.data.mode).toBe('history')
  expect(result.data.activity.rows).toMatchObject([{ intent: 'Retained goal' }])
  expect(result.data).not.toHaveProperty('registration')
  expect(result.data).not.toHaveProperty('form')
})
const startProviderAccountAuthorization = vi.fn()
function currentApi(
  browseActivityRoots: NonNullable<ManagementHandlers['activity']>['browseActivityRoots'] = () => ({
    roots: [],
  }),
  listAgentProviders: NonNullable<ManagementHandlers['accounts']>['listAgentProviders'] = () => ({
    providers: [],
  }),
) {
  return fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => ({
        name,
        displayName: 'Current',
        effectiveRole: WorkspaceRole.OWNER,
        state: WorkspaceState.ACTIVE,
      }),
    },
    agents: {
      getAgent: ({ name }) => ({ name, displayName: 'Current worker', state: AgentState.ENABLED }),
    },
    registrations: {
      getAgentRegistration: () => {
        throw new ConnectError('activity must not read the registration', Code.Internal)
      },
    },
    accounts: {
      listAgentProviders,
      startProviderAccountAuthorization: (request) => {
        startProviderAccountAuthorization(request)
        return {
          name: `workspaces/${workspace}/accountAuthorizations/00000000-0000-4000-8000-000000000002`,
        }
      },
    },
    activity: {
      inspectAgentHistory: ({ name }) => ({ name, displayName: 'Current worker', deleted: false }),
      browseActivityRoots,
    },
  })
}
// Catches: the agent page losing the accounts it shows, or reading the registration it does not.
it('renders a current Agent with its accounts and without its registration', async () => {
  // A catalog provider, so its row shows the recipe's logo.
  const recipe = RECIPE_CHOICES.find((choice) => choice.icons)!
  const provider = `workspaces/${workspace}/providers/00000000-0000-4000-8000-000000000003`
  coral.transport = currentApi(undefined, () => ({
    providers: [
      {
        provider: { name: provider, displayName: 'Catalog provider', namespace: recipe.id },
        accountUse: { case: 'separateAccount', value: { canConnect: true } },
      },
    ],
  }))
  const result = await loader(args())
  expect(result.data).toMatchObject({
    mode: 'current',
    agent: { displayName: 'Current worker' },
    providers: [
      {
        provider,
        name: 'Catalog provider',
        namespace: recipe.id,
        account: { kind: 'separate', state: 'disconnected', canConnect: true },
        blockers: [],
        icons: recipe.icons,
      },
    ],
  })
  expect(result.data).toHaveProperty('form')
  expect(result.data).not.toHaveProperty('registration')
})

// Catches: an accounts outage taking the agent and its activity down with it.
it.each([Code.Unavailable, Code.PermissionDenied, Code.Internal])(
  'keeps the agent page when Coral cannot read its accounts (%s)',
  async (code) => {
    coral.transport = currentApi(undefined, () => {
      throw new ConnectError('accounts unavailable', code)
    })
    const result = await loader(args())
    expect(result.data).toMatchObject({
      mode: 'current',
      agent: { displayName: 'Current worker' },
      providers: null,
      activity: { listUnavailable: false },
    })
  },
)

// Catches: a lapsed sign-in hidden as an accounts outage, so you never sign in again.
it('fails the page when the sign-in lapsed while reading accounts', async () => {
  coral.transport = currentApi(undefined, () => {
    throw new ConnectError('signed out', Code.Unauthenticated)
  })
  await expect(loader(args())).rejects.toMatchObject({ status: 401 })
})

// Local mode accepts provider sign-in callbacks only on a loopback origin.
const origin = 'http://127.0.0.1:3000'
const post = (fields: Record<string, string>) => {
  const form = new FormData()
  for (const [key, value] of Object.entries({ requestId: 'request', ...fields }))
    form.set(key, value)
  // The connect dialog submits through a fetcher, so the request is the route's data URL.
  return action(
    authRouteTestArgs(
      new Request(`${origin}/workspaces/${workspace}/agents/${agent}.data`, {
        method: 'POST',
        headers: { origin },
        body: form,
      }),
      { workspace, agent },
      null,
    ),
  )
}

// Catches: an agent account connection starting for the person instead of the agent, or
// returning to the fetcher's data URL instead of the agent's page once it succeeds.
it('starts the account connection for this agent and returns where to follow it', async () => {
  startProviderAccountAuthorization.mockReset()
  coral.transport = currentApi()
  const result = await post({ intent: 'connectAccount', provider: 'providers/github' })
  expect(startProviderAccountAuthorization).toHaveBeenCalledExactlyOnceWith(
    expect.objectContaining({
      parent: `workspaces/${workspace}`,
      target: {
        case: 'agentAccount',
        value: expect.objectContaining({ agent: `workspaces/${workspace}/agents/${agent}` }),
      },
    }),
  )
  const returnTo = new URLSearchParams({ returnTo: `/workspaces/${workspace}/agents/${agent}` })
  expect(result.data).toMatchObject({
    error: null,
    progress: `/workspaces/${workspace}/account-authorizations/00000000-0000-4000-8000-000000000002/progress?${returnTo}`,
  })
})

// Catches: the agent page accepting a settings change it does not offer.
it('rejects any other intent and changes nothing', async () => {
  startProviderAccountAuthorization.mockReset()
  coral.transport = currentApi()
  await expect(post({ intent: 'delete' })).rejects.toMatchObject({ status: 400 })
  expect(startProviderAccountAuthorization).not.toHaveBeenCalled()
})
const currentTask = {
  record: {
    case: 'task',
    value: {
      name: `workspaces/${workspace}/tasks/00000000-0000-4000-8000-000000000003`,
      workspace: `workspaces/${workspace}`,
      intent: 'Current goal',
      outcome: 'open',
    },
  },
} as const
// Catches: an unreadable history offering setup to an agent that may already call Coral.
it.each([
  ['an empty history', () => ({ roots: [] }), true],
  ['a history with calls', () => ({ roots: [currentTask] }), false],
  ['a history with only earlier calls', () => ({ roots: [], hasBefore: true }), false],
  [
    'a history Coral cannot read',
    () => {
      throw new ConnectError('agent list unavailable', Code.PermissionDenied)
    },
    false,
  ],
] as const)('marks a current Agent idle only for %s', async (_, browse, idle) => {
  coral.transport = currentApi(browse)
  const result = await loader(args())
  expect(result.data).toMatchObject({ mode: 'current', idle })
})
it('keeps retained Agent attribution and history concealed from non-Owners', async () => {
  coral.transport = api(Code.PermissionDenied)
  await expect(loader(args())).rejects.toMatchObject({ status: 403 })
})

it.each([
  ['task', personalTask],
  ['invocation', personalInvocation],
] as const)(
  'keeps an independently authorized personal %s after losing the Agent context',
  async (kind, name) => {
    coral.transport = api()
    const query = `?record=${encodeURIComponent(name)}`
    const before = await loader(args(query))
    expect(before.data.mode).toBe('history')
    expect(before.data.activity.detail?.kind).toBe(kind)

    coral.transport = api(Code.PermissionDenied)
    const after = await loader(args(query))
    expect(after.data.mode).toBe('unavailable')
    expect(after.data.activity.detail?.kind).toBe(kind)
    expect(after.data.activity.listUnavailable).toBe(true)
    expect(after.data.activity.rows).toEqual([])
    for (const field of ['workspace', 'agent', 'displayName', 'providers', 'registration', 'form'])
      expect(after.data).not.toHaveProperty(field)
  },
)

it('conceals selected evidence too when its own authority is lost', async () => {
  coral.transport = api(Code.PermissionDenied, Code.NotFound)
  const result = await loader(args(`?record=${encodeURIComponent(personalTask)}`))
  expect(result.data.activity.detail).toBeNull()
  expect(result.data.activity.unavailable).toBe(true)
  expect(JSON.stringify(result.data)).not.toContain('Still-authorized personal goal')
})

it.each([
  [Code.Unauthenticated, 401],
  [Code.Unavailable, 503],
] as const)(
  'does not disguise context failure %s as an authorization-only fallback',
  async (code, status) => {
    coral.transport = api(code)
    await expect(loader(args(`?record=${encodeURIComponent(personalTask)}`))).rejects.toMatchObject(
      {
        status,
      },
    )
  },
)

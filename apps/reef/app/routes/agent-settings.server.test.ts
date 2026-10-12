import { ConnectError, Code } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { AgentState } from '@/generated/coral/management/v1/agent_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { action, loader } from './agent-settings'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))
const workspace = '00000000-0000-4000-8000-000000000001'
const agent = 'nightly'
const agentResource = `workspaces/${workspace}/agents/${agent}`
const registration = `${agentResource}/registration`
const calls = {
  updateAgent: vi.fn(),
  updateAgentRegistration: vi.fn(),
  rotateAgentCredential: vi.fn(),
  deleteAgent: vi.fn(),
}
let getAgent: () => { name: string; displayName: string; state: AgentState }

beforeEach(() => {
  vi.resetAllMocks()
  getAgent = () => ({ name: agentResource, displayName: 'Nightly', state: AgentState.ENABLED })
  coral.transport = fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => ({
        name,
        displayName: 'Operations',
        effectiveRole: WorkspaceRole.OWNER,
        state: WorkspaceState.ACTIVE,
      }),
    },
    agents: {
      getAgent: () => getAgent(),
      updateAgent: (request) => {
        calls.updateAgent(request)
        return {}
      },
      deleteAgent: (request) => {
        calls.deleteAgent(request)
        return {}
      },
    },
    registrations: {
      getAgentRegistration: ({ name }) => ({ name, enabled: true }),
      updateAgentRegistration: (request) => {
        calls.updateAgentRegistration(request)
        return {}
      },
      rotateAgentCredential: (request) => {
        calls.rotateAgentCredential(request)
        return { credential: 'new-agent-key' }
      },
    },
    accounts: {
      listAgentProviders: () => {
        throw new ConnectError('settings must not read providers', Code.Internal)
      },
    },
    activity: {
      browseActivityRoots: () => {
        throw new ConnectError('settings must not read activity', Code.Internal)
      },
    },
  })
})

// Local mode accepts provider sign-in callbacks only on a loopback origin.
const origin = 'http://127.0.0.1:3000'
const url = `${origin}/workspaces/${workspace}/agents/${agent}/settings`
const load = () => loader(authRouteTestArgs(new Request(url), { workspace, agent }, null))
const post = (fields: Record<string, string>) => {
  const form = new FormData()
  for (const [key, value] of Object.entries({ etag: 'etag', requestId: 'request', ...fields }))
    form.set(key, value)
  return action(
    authRouteTestArgs(
      new Request(url, { method: 'POST', headers: { origin }, body: form }),
      { workspace, agent },
      null,
    ),
  )
}
const noChanges = () => {
  for (const call of Object.values(calls)) expect(call).not.toHaveBeenCalled()
}

// Catches: settings dragging activity or accounts back into their read, or losing a section's data.
it('reads the agent and its registration without reading activity or accounts', async () => {
  const result = await load()
  expect(result.data).toMatchObject({
    agent: {
      name: agentResource,
      state: 'enabled',
      settingsPath: `/workspaces/${workspace}/agents/${agent}/settings`,
    },
    registration: { name: registration, enabled: true },
  })
  expect(result.data).not.toHaveProperty('activity')
})

// Catches: a deleted agent's settings rendering controls for an agent Coral no longer has.
it('does not find the settings of a missing agent', async () => {
  getAgent = () => {
    throw new ConnectError('agent not found', Code.NotFound)
  }
  await expect(load()).rejects.toMatchObject({ status: 404 })
})

// Catches: a control sending Coral the wrong change, or saying the wrong thing about it.
const saves: {
  fields: Record<string, string>
  call: keyof typeof calls
  request: object
  message: string
}[] = [
  {
    fields: { intent: 'agentState', state: 'disabled' },
    call: 'updateAgent',
    request: {
      agent: expect.objectContaining({
        name: agentResource,
        etag: 'etag',
        state: AgentState.DISABLED,
      }),
      updateMask: expect.objectContaining({ paths: ['state'] }),
    },
    message: 'Agent disabled.',
  },
  {
    fields: { intent: 'agentState', state: 'enabled' },
    call: 'updateAgent',
    request: {
      agent: expect.objectContaining({ name: agentResource, state: AgentState.ENABLED }),
      updateMask: expect.objectContaining({ paths: ['state'] }),
    },
    message: 'Agent enabled. Other access restrictions still apply.',
  },
  {
    fields: { intent: 'registrationState', enabled: 'false' },
    call: 'updateAgentRegistration',
    request: {
      registration: expect.objectContaining({ name: registration, etag: 'etag', enabled: false }),
      updateMask: expect.objectContaining({ paths: ['enabled'] }),
    },
    message: 'API access saved. Other access restrictions still apply.',
  },
  {
    fields: { intent: 'expiration', expiration: '2030-01-01T00:00:00' },
    call: 'updateAgentRegistration',
    request: {
      registration: expect.objectContaining({
        name: registration,
        expireTime: expect.objectContaining({ seconds: 1893456000n, nanos: 0 }),
      }),
      updateMask: expect.objectContaining({ paths: ['expire_time'] }),
    },
    message: 'Expiration date saved. Other access restrictions still apply.',
  },
  {
    fields: { intent: 'expiration', expiration: '' },
    call: 'updateAgentRegistration',
    request: {
      registration: expect.not.objectContaining({ expireTime: expect.anything() }),
      updateMask: expect.objectContaining({ paths: ['expire_time'] }),
    },
    message: 'Expiration date saved. Other access restrictions still apply.',
  },
]
it.each(saves)(
  'saves $fields.intent $fields through $call',
  async ({ fields, call, request, message }) => {
    const result = await post(fields)
    expect(calls[call]).toHaveBeenCalledExactlyOnceWith(expect.objectContaining(request))
    expect(result).toMatchObject({ data: { error: null, message } })
  },
)

// Catches: a replaced key that never reaches the page, or a rotation of the wrong Registration.
it('returns the new API key once after replacing it', async () => {
  const result = await post({ intent: 'rotate' })
  expect(calls.rotateAgentCredential).toHaveBeenCalledExactlyOnceWith(
    expect.objectContaining({ name: registration, etag: 'etag', requestId: 'request' }),
  )
  expect(result).toMatchObject({ data: { error: null, credential: 'new-agent-key' } })
})

it('returns to the agent list after deleting the agent', async () => {
  const result = (await post({ intent: 'delete' })) as Response
  expect(calls.deleteAgent).toHaveBeenCalledExactlyOnceWith(
    expect.objectContaining({ name: agentResource, etag: 'etag' }),
  )
  expect(result.headers.get('Location')).toBe(`/workspaces/${workspace}/agents`)
})

// Catches: a malformed form reaching Coral as a change.
it.each([
  [{ intent: 'expiration', expiration: 'not a date' }],
  [{ intent: 'renameAgent' }],
  [{ intent: 'connectAccount' }],
])('rejects %o and changes nothing', async (fields) => {
  await expect(post(fields)).rejects.toMatchObject({ status: 400 })
  noChanges()
})

// Catches: a Coral refusal shown as success, or as a generic outage.
it('keeps a refused change on the page with its reason', async () => {
  getAgent = () => ({ name: agentResource, displayName: 'Nightly', state: AgentState.ENABLED })
  coral.transport = fakeManagementTransport({
    agents: {
      updateAgent: () => {
        throw new ConnectError('stale etag', Code.Aborted)
      },
    },
  })
  const result = await post({ intent: 'agentState', state: 'disabled' })
  expect(result).toMatchObject({
    data: { error: expect.stringMatching(/^Coral could not enable or disable the agent\. /) },
  })
})

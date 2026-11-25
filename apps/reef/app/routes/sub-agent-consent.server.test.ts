import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { ToolAccessState } from '@/generated/coral/management/v1/policy_pb'
import { SubAgentModelProtocol } from '@/generated/coral/management/v1/sub_agent_model_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { coralRefusal, fakeManagementTransport } from '@/lib/management.test-support'
import { action, loader, shouldRevalidate } from './sub-agent-consent'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))

const workspace = '00000000-0000-4000-8000-000000000001'
const modelId = '11111111-1111-4111-8111-111111111111'
let role: WorkspaceRole
let refuse: ConnectError | null
/** Each call that reached the model service. */
let calls: object[]

beforeEach(() => {
  role = WorkspaceRole.OWNER
  refuse = null
  calls = []
  coral.transport = fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => ({ name, state: WorkspaceState.ACTIVE, effectiveRole: role }),
    },
    subAgentModel: {
      getWorkspaceSubAgentModel: (request) => {
        calls.push({ read: request.workspace })
        if (refuse) throw refuse
        return {
          id: modelId,
          settings: {
            protocol: SubAgentModelProtocol.CHAT_COMPLETIONS,
            baseUrl: 'https://models.example.com:8443/v1',
            model: 'large-1',
            requestArgsJson: '{"max_tokens":4096}',
          },
        }
      },
      turnOnSubAgent: ({ workspace: parent, modelId: id, accessState }) => {
        calls.push({ turnOn: parent, modelId: id, accessState })
        if (refuse) throw refuse
        return { accessState }
      },
    },
  })
})

const origin = 'http://127.0.0.1:3000'
const url = `${origin}/workspaces/${workspace}/sub-agent/consent`
const load = async () =>
  (await loader(authRouteTestArgs(new Request(url), { workspace }, null))).data
const post = (fields: Record<string, string>) => {
  const body = new FormData()
  for (const [key, value] of Object.entries(fields)) body.set(key, value)
  return action(
    authRouteTestArgs(
      new Request(url, { method: 'POST', headers: { origin }, body }),
      { workspace },
      null,
    ),
  )
}

// Catches: two destinations that look the same, such as two paths of one gateway, and a setting that
// says nothing of where the data goes, such as the arguments, in the way of what does.
it('reads where the model is and what it is called', async () => {
  expect(await load()).toEqual({
    model: { id: modelId, baseUrl: 'https://models.example.com:8443/v1', name: 'large-1' },
    problem: null,
  })
  expect(calls).toEqual([{ read: `workspaces/${workspace}` }])
})

// Catches: a read of the model under a person who is about to confirm the one they saw.
it('reads the model only when the dialog asks', () => {
  expect(shouldRevalidate()).toBe(false)
})

// Catches: a dialog that opens on an error page, or that does not say why it has no model.
it.each([
  [
    new ConnectError('not found', Code.NotFound),
    'The organization has no model for the sub-agent now. Reload the page.',
  ],
  [new ConnectError('unavailable', Code.Unavailable), 'Coral is not available.'],
])('says why it has no model to show', async (error, problem) => {
  refuse = error
  expect(await load()).toEqual({ model: null, problem })
})

// Catches: a lapsed sign-in shown as a missing model, so the person never signs in again.
it('passes on a lapsed sign-in', async () => {
  refuse = new ConnectError('unauthenticated', Code.Unauthenticated)
  await expect(load()).rejects.toMatchObject({ status: 401 })
})

// Catches: a member who reaches the address getting the model, or turning the tool on.
it('refuses a member', async () => {
  role = WorkspaceRole.MEMBER
  await expect(load()).rejects.toMatchObject({ status: 403 })
  await expect(post({ intent: 'turnOn', modelId, access: 'enabled' })).rejects.toMatchObject({
    status: 403,
  })
  expect(calls).toEqual([])
})

// Catches: a consent that does not carry the workspace, the model and the policy the owner confirmed.
it.each([
  ['enabled', ToolAccessState.ENABLED],
  ['approval_required', ToolAccessState.APPROVAL_REQUIRED],
])('turns the tool on as %s', async (access, accessState) => {
  expect(await post({ intent: 'turnOn', modelId, access })).toMatchObject({ data: { error: null } })
  expect(calls).toEqual([{ turnOn: `workspaces/${workspace}`, modelId, accessState }])
})

// Catches: a malformed form, or one that turns the tool off here, reaching Coral.
it.each(['disabled', 'other', ''])('rejects the policy %j', async (access) => {
  await expect(post({ intent: 'turnOn', modelId, access })).rejects.toMatchObject({ status: 400 })
  expect(calls).toEqual([])
})

// Catches: a refusal that looks like a success, or a changed model shown as a failure that tells
// the owner to reload and not to read the new values.
it.each([
  [
    coralRefusal(Code.Aborted, 'SUB_AGENT_MODEL_CHANGED'),
    409,
    'The model changed after you opened this dialog. Read the new values before you turn on the sub-agent.',
  ],
  [
    new ConnectError('not found', Code.NotFound),
    404,
    'Coral could not turn on the sub-agent. This workspace does not have the tool now. Reload the page.',
  ],
  [
    coralRefusal(Code.FailedPrecondition, 'WORKSPACE_ARCHIVED'),
    409,
    'This workspace is archived. Restore it before you make changes.',
  ],
])('says why Coral refused', async (refusal, status, error) => {
  refuse = refusal
  expect(await post({ intent: 'turnOn', modelId, access: 'enabled' })).toMatchObject({
    data: { error },
    init: { status },
  })
})

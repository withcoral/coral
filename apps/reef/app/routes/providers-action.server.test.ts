import { beforeEach, expect, it, vi } from 'vitest'
import { ConnectError, Code } from '@connectrpc/connect'

import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { ToolAccessState } from '@/generated/coral/management/v1/policy_pb'
import { ProviderBlocker } from '@/generated/coral/management/v1/provider_pb'
import { fakeManagement } from '@/lib/management.test-support'
import { action } from './providers'

const mocks = vi.hoisted(() => ({
  getProvider: vi.fn(),
  updateToolPolicies: vi.fn(),
  workspace: vi.fn(),
}))

vi.mock('@/lib/workspace-route.server', () => ({ authorizedWorkspace: mocks.workspace }))

const workspaceId = '29a180e5-397c-4844-92de-8bbea6066235'
const providerA = '11111111-1111-4111-8111-111111111111'
const providerB = '22222222-2222-4222-8222-222222222222'

beforeEach(() => {
  vi.resetAllMocks()
  mocks.workspace.mockResolvedValue({
    api: fakeManagement({
      policies: { updateToolPolicies: mocks.updateToolPolicies },
      providers: { getProvider: mocks.getProvider },
    }),
    workspace: { name: `workspaces/${workspaceId}`, path: `/workspaces/${workspaceId}` },
  })
  mocks.updateToolPolicies.mockResolvedValue({})
  mocks.getProvider.mockResolvedValue({ blockers: [] })
})

/** Submits `form` as a same-origin browser would. */
const post = (form: FormData) =>
  action(
    authRouteTestArgs(
      new Request(`https://reef.test/workspaces/${workspaceId}/providers`, {
        method: 'POST',
        headers: { origin: 'https://reef.test' },
        body: form,
      }),
      { workspace: workspaceId },
      null,
    ),
  )

// Catches: a selection sent as one call per tool, or one call per
// provider beyond Coral's 100-tool limit.
it('batches one cross-provider policy selection per provider with 100-tool calls', async () => {
  const form = new FormData()
  form.set('intent', 'policy')
  form.set('access', 'enabled')
  for (let index = 0; index < 250; index += 1) {
    form.append(
      'selectedTool',
      JSON.stringify({
        provider: providerA,
        tool: `providers/${providerA}/tools/a-${index}`,
      }),
    )
  }
  for (let index = 0; index < 120; index += 1) {
    form.append(
      'selectedTool',
      JSON.stringify({
        provider: providerB,
        tool: `providers/${providerB}/tools/b-${index}`,
      }),
    )
  }
  await post(form)

  const calls = mocks.updateToolPolicies.mock.calls.map(
    ([call]) =>
      call as {
        parent: string
        policies: { tool: string; accessState: ToolAccessState }[]
      },
  )
  for (const { parent, policies } of calls) {
    expect(policies.length).toBeLessThanOrEqual(100)
    // One provider per call: every tool belongs to the call's parent.
    const provider = parent.split('/').at(-1)
    expect(parent).toBe(`workspaces/${workspaceId}/providers/${provider}`)
    for (const { tool, accessState } of policies) {
      expect(tool).toMatch(new RegExp(`^providers/${provider}/`))
      expect(accessState).toBe(ToolAccessState.ENABLED)
    }
  }
  // Batches are full: each provider sends at most one partial call.
  for (const provider of [providerA, providerB]) {
    const partial = calls.filter(
      ({ parent, policies }) => parent.endsWith(provider) && policies.length < 100,
    )
    expect(partial.length).toBeLessThanOrEqual(1)
  }
  const sent = calls.flatMap(({ policies }) => policies.map(({ tool }) => tool))
  expect(sent.toSorted()).toEqual(
    form
      .getAll('selectedTool')
      .map((value) => (JSON.parse(value as string) as { tool: string }).tool)
      .toSorted(),
  )
})
const policyForm = (targets: { provider: string; tool: string }[]) => {
  const form = new FormData()
  form.set('intent', 'policy')
  form.set('access', 'enabled')
  for (const target of targets) form.append('selectedTool', JSON.stringify(target))
  return form
}

// Catches: a provider whose last tool was disabled showing no blocker, as the
// page no longer reloads the catalog after a saved policy.
it('reports the blockers of each changed provider', async () => {
  mocks.getProvider.mockImplementation(({ name }: { name: string }) => ({
    blockers: name.endsWith(providerA) ? [ProviderBlocker.NO_TOOLS_READY] : [],
  }))
  const result = (await post(
    policyForm([
      { provider: providerA, tool: `providers/${providerA}/tools/a` },
      { provider: providerB, tool: `providers/${providerB}/tools/b` },
    ]),
  )) as unknown as { data: { blockers: Record<string, string[]> } }
  expect(result.data.blockers).toEqual({ [providerA]: ['No tools ready'], [providerB]: [] })
})

// Catches: a selection across many providers starting every read at once.
it('reads blockers four at a time, as it writes', async () => {
  const providers = Array.from(
    { length: 9 },
    (_, index) => `33333333-3333-4333-8333-${String(index).padStart(12, '0')}`,
  )
  let reading = 0
  let most = 0
  mocks.getProvider.mockImplementation(async () => {
    most = Math.max(most, ++reading)
    await new Promise((resolve) => setTimeout(resolve, 1))
    reading -= 1
    return { blockers: [] }
  })
  await post(policyForm(providers.map((provider) => ({ provider, tool: `t-${provider}` }))))
  expect(mocks.getProvider).toHaveBeenCalledTimes(9)
  expect(most).toBe(4)
})

// Catches: a saved policy reported as failed because a read after it failed.
it('leaves the blockers out when it cannot read them, and still reports the save', async () => {
  mocks.getProvider.mockRejectedValue(new ConnectError('down', Code.Unavailable))
  const result = (await post(
    policyForm([{ provider: providerA, tool: `providers/${providerA}/tools/a` }]),
  )) as unknown as { data: { error: null; blockers?: unknown; tools: { status: string }[] } }
  expect(result.data.error).toBeNull()
  expect(result.data.blockers).toBeUndefined()
  expect(result.data.tools.map((tool) => tool.status)).toEqual(['updated'])
})

// Catches: one failed batch reported as a failure of every tool, so the
// page keeps tools Coral already saved in the retry selection.
it('reports each tool on its own when one provider batch fails', async () => {
  const form = policyForm([
    { provider: providerA, tool: `providers/${providerA}/tools/a` },
    { provider: providerB, tool: `providers/${providerB}/tools/b` },
  ])
  mocks.updateToolPolicies.mockImplementation(({ parent }: { parent: string }) =>
    parent.endsWith(providerB)
      ? Promise.reject(new ConnectError('denied', Code.PermissionDenied))
      : Promise.resolve({}),
  )

  const result = (await post(form)) as unknown as {
    data: { error: null; message: string; tools: unknown[] }
  }

  expect(result.data.error).toBeNull()
  expect(result.data.tools).toEqual([
    { provider: providerA, tool: `providers/${providerA}/tools/a`, status: 'updated' },
    {
      provider: providerB,
      tool: `providers/${providerB}/tools/b`,
      status: 'failed',
      reason: expect.stringContaining('permission'),
    },
  ])
  expect(result.data.message).toContain('1 tool not saved')
})

// Catches: a malformed or repeated entry updating fewer or other tools
// than the reader selected.
it('rejects a selection with a malformed or repeated tool', async () => {
  const malformed = policyForm([])
  malformed.append('selectedTool', '{not json')
  await expect(post(malformed)).rejects.toMatchObject({ status: 400 })

  const target = { provider: providerA, tool: `providers/${providerA}/tools/a` }
  await expect(post(policyForm([target, target]))).rejects.toMatchObject({ status: 400 })
  expect(mocks.updateToolPolicies).not.toHaveBeenCalled()
})

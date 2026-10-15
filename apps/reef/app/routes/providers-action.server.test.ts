import { beforeEach, expect, it, vi } from 'vitest'
import { ConnectError, Code } from '@connectrpc/connect'

import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { OperationAccessState } from '@/generated/coral/management/v1/policy_pb'
import { fakeManagement } from '@/lib/management.test-support'
import { action } from './providers'

const mocks = vi.hoisted(() => ({
  updateOperationPolicies: vi.fn(),
  workspace: vi.fn(),
}))

vi.mock('@/lib/workspace-route.server', () => ({ authorizedWorkspace: mocks.workspace }))

const workspaceId = '29a180e5-397c-4844-92de-8bbea6066235'
const providerA = '11111111-1111-4111-8111-111111111111'
const providerB = '22222222-2222-4222-8222-222222222222'

beforeEach(() => {
  vi.resetAllMocks()
  mocks.workspace.mockResolvedValue({
    api: fakeManagement({ policies: { updateOperationPolicies: mocks.updateOperationPolicies } }),
    workspace: { name: `workspaces/${workspaceId}`, path: `/workspaces/${workspaceId}` },
  })
  mocks.updateOperationPolicies.mockResolvedValue({})
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

// Catches: a selection sent as one call per operation, or one call per
// provider beyond Coral's 100-operation limit.
it('batches one cross-provider policy selection per provider with 100-operation calls', async () => {
  const form = new FormData()
  form.set('intent', 'policy')
  form.set('access', 'enabled')
  for (let index = 0; index < 250; index += 1) {
    form.append(
      'selectedOperation',
      JSON.stringify({
        provider: providerA,
        operation: `providers/${providerA}/operations/a-${index}`,
      }),
    )
  }
  for (let index = 0; index < 120; index += 1) {
    form.append(
      'selectedOperation',
      JSON.stringify({
        provider: providerB,
        operation: `providers/${providerB}/operations/b-${index}`,
      }),
    )
  }
  await post(form)

  const calls = mocks.updateOperationPolicies.mock.calls.map(
    ([call]) =>
      call as {
        parent: string
        policies: { operation: string; accessState: OperationAccessState }[]
      },
  )
  for (const { parent, policies } of calls) {
    expect(policies.length).toBeLessThanOrEqual(100)
    // One provider per call: every operation belongs to the call's parent.
    const provider = parent.split('/').at(-1)
    expect(parent).toBe(`workspaces/${workspaceId}/providers/${provider}`)
    for (const { operation, accessState } of policies) {
      expect(operation).toMatch(new RegExp(`^providers/${provider}/`))
      expect(accessState).toBe(OperationAccessState.ENABLED)
    }
  }
  // Batches are full: each provider sends at most one partial call.
  for (const provider of [providerA, providerB]) {
    const partial = calls.filter(
      ({ parent, policies }) => parent.endsWith(provider) && policies.length < 100,
    )
    expect(partial.length).toBeLessThanOrEqual(1)
  }
  const sent = calls.flatMap(({ policies }) => policies.map(({ operation }) => operation))
  expect(sent.toSorted()).toEqual(
    form
      .getAll('selectedOperation')
      .map((value) => (JSON.parse(value as string) as { operation: string }).operation)
      .toSorted(),
  )
})
const policyForm = (targets: { provider: string; operation: string }[]) => {
  const form = new FormData()
  form.set('intent', 'policy')
  form.set('access', 'enabled')
  for (const target of targets) form.append('selectedOperation', JSON.stringify(target))
  return form
}

// Catches: one failed batch reported as a failure of every operation, so the
// page keeps operations Coral already saved in the retry selection.
it('reports each operation on its own when one provider batch fails', async () => {
  const form = policyForm([
    { provider: providerA, operation: `providers/${providerA}/operations/a` },
    { provider: providerB, operation: `providers/${providerB}/operations/b` },
  ])
  mocks.updateOperationPolicies.mockImplementation(({ parent }: { parent: string }) =>
    parent.endsWith(providerB)
      ? Promise.reject(new ConnectError('denied', Code.PermissionDenied))
      : Promise.resolve({}),
  )

  const result = (await post(form)) as unknown as {
    data: { error: null; message: string; operations: unknown[] }
  }

  expect(result.data.error).toBeNull()
  expect(result.data.operations).toEqual([
    { provider: providerA, operation: `providers/${providerA}/operations/a`, status: 'updated' },
    {
      provider: providerB,
      operation: `providers/${providerB}/operations/b`,
      status: 'failed',
      reason: expect.stringContaining('permission'),
    },
  ])
  expect(result.data.message).toContain('1 operation not saved')
})

// Catches: a malformed or repeated entry updating fewer or other operations
// than the reader selected.
it('rejects a selection with a malformed or repeated operation', async () => {
  const malformed = policyForm([])
  malformed.append('selectedOperation', '{not json')
  await expect(post(malformed)).rejects.toMatchObject({ status: 400 })

  const target = { provider: providerA, operation: `providers/${providerA}/operations/a` }
  await expect(post(policyForm([target, target]))).rejects.toMatchObject({ status: 400 })
  expect(mocks.updateOperationPolicies).not.toHaveBeenCalled()
})

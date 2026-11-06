import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { SecretStoreKind } from '@/generated/coral/management/v1/secret_store_pb'
import { WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { action, loader } from './organization-secret-stores'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))
const origin = 'http://127.0.0.1:3000'
const url = `${origin}/organization/secret-stores`
const research = 'workspaces/00000000-0000-4000-8000-000000000001'
const closed = 'workspaces/00000000-0000-4000-8000-000000000002'
const requestId = '00000000-0000-4000-8000-0000000000aa'
const load = () => loader(authRouteTestArgs(new Request(url), {}, null))
let refuse: ConnectError | null
let calls: { method: string; workspace: string; secretStore: string; requestId: string }[]

beforeEach(() => {
  refuse = null
  calls = []
  const change = (method: string) => (request: Omit<(typeof calls)[number], 'method'>) => {
    if (refuse) throw refuse
    calls.push({
      method,
      workspace: request.workspace,
      secretStore: request.secretStore,
      requestId: request.requestId,
    })
    return {}
  }
  coral.transport = fakeManagementTransport({
    secretStores: {
      listSecretStores: () => {
        if (refuse) throw refuse
        return {
          secretStores: [
            { name: 'secretStores/platform', kind: SecretStoreKind.KV_V2 },
            { name: 'secretStores/newer', kind: SecretStoreKind.UNSPECIFIED },
          ],
        }
      },
      listSecretStoreGrants: () => ({
        grants: [
          {
            workspace: research,
            workspaceDisplayName: 'Research',
            workspaceState: WorkspaceState.ACTIVE,
            secretStore: {
              name: 'secretStores/platform',
              kind: SecretStoreKind.KV_V2,
              available: true,
            },
          },
          {
            workspace: closed,
            workspaceDisplayName: 'Closed',
            workspaceState: WorkspaceState.ARCHIVED,
            secretStore: { name: 'secretStores/removed', available: false },
          },
        ],
      }),
      grantSecretStore: change('grant'),
      revokeSecretStore: change('revoke'),
    },
    organizationWorkspaces: {
      listOrganizationWorkspaces: () => ({
        workspaces: [
          { name: research, displayName: 'Research', state: WorkspaceState.ACTIVE, etag: 'v1' },
          { name: closed, displayName: 'Closed', state: WorkspaceState.ARCHIVED, etag: 'v1' },
        ],
      }),
    },
  })
})

function submit(fields: Record<string, string>) {
  const form = new FormData()
  form.set('requestId', requestId)
  for (const [key, value] of Object.entries(fields)) form.set(key, value)
  const request = new Request(url, { method: 'POST', headers: { origin }, body: form })
  return action(authRouteTestArgs(request, {}, null))
}
const grant = { intent: 'grant', name: 'secretStores/platform', workspace: research }

// Catches: an owner seeing a resource name or a raw enum, a newer server's kind
// breaking the page, or a grant whose store config removed vanishing, so nobody
// could see or revoke it.
it('shows each store with its workspaces, and a removed store as unavailable', async () => {
  const result = await load()
  expect(result?.data.stores).toEqual([
    { name: 'newer', kind: 'Unknown', workspaces: [] },
    {
      name: 'platform',
      kind: 'Vault / OpenBao KV v2',
      workspaces: [{ name: research, displayName: 'Research', archived: false }],
    },
    {
      name: 'removed',
      kind: null,
      workspaces: [{ name: closed, displayName: 'Closed', archived: true }],
    },
  ])
})

// Catches: a grant offered for an archived workspace, which Coral refuses.
it('offers only active workspaces', async () => {
  const result = await load()
  expect(result?.data.workspaces).toEqual([{ name: research, displayName: 'Research' }])
})

// Catches: a member who reaches the URL getting anything but Coral's refusal.
it('passes on the refusal when the caller is not an organization owner', async () => {
  refuse = new ConnectError('permission denied', Code.PermissionDenied)
  const failure: unknown = await load().catch((error: unknown) => error)
  expect(failure).toMatchObject({ status: 403 })
})

// Catches: the form's one key reused across rows, so the second change on a page
// is refused as a repeat, or a revocation refused as a repeat of its grant.
it('sends each change with a key of its own', async () => {
  expect((await submit(grant)).data).toMatchObject({
    error: null,
    message: 'Secret store granted.',
  })
  await submit({ ...grant, workspace: closed })
  expect((await submit({ ...grant, intent: 'revoke' })).data).toMatchObject({
    message: 'Grant revoked.',
  })
  expect(calls.map(({ method, workspace }) => [method, workspace])).toEqual([
    ['grant', research],
    ['grant', closed],
    ['revoke', research],
  ])
  expect(new Set(calls.map((call) => call.requestId)).size).toBe(3)
  expect(calls.every((call) => call.secretStore === 'secretStores/platform')).toBe(true)
})

// Catches: a forged field reaching Coral as another kind of resource.
it.each([
  ['an unknown intent', { ...grant, intent: 'delete' }],
  ['a bad store', { ...grant, name: 'workspaces/other' }],
  ['a bad workspace', { ...grant, workspace: 'secretStores/other' }],
  ['no chosen workspace', { ...grant, workspace: '' }],
])('rejects %s without calling Coral', async (_case, fields) => {
  const failure: unknown = await submit(fields).catch((error: unknown) => error)
  expect(failure).toMatchObject({ status: 400 })
  expect(calls).toEqual([])
})

// Catches: an owner told only that something failed when config lost the store.
it('says when config no longer defines the store', async () => {
  refuse = new ConnectError('SECRET_STORE_NOT_DEFINED', Code.FailedPrecondition)
  const result = await submit(grant)
  expect(result.init?.status).toBe(409)
  expect(result.data.error).toBe('Coral has no secret store with this name. Refresh this page.')
})

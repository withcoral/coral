import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { SecretStoreState } from '@/generated/coral/management/v1/secret_store_pb'
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
/** A change Coral refuses, as its method and workspace, while it accepts the others. */
let refuseChange: string | null
let disabled: boolean
let calls: { method: string; workspace: string; secretStore: string; requestId: string }[]

beforeEach(() => {
  refuse = null
  refuseChange = null
  disabled = false
  calls = []
  const change = (method: string) => (request: Omit<(typeof calls)[number], 'method'>) => {
    if (refuse) throw refuse
    if (refuseChange === `${method} ${request.workspace}`)
      throw new ConnectError('SECRET_STORE_NOT_DEFINED', Code.FailedPrecondition)
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
        if (disabled) return { secretStores: [], disabled }
        return {
          secretStores: [
            {
              name: 'secretStores/platform',
              kind: 'kv-v2',
              kindLabel: 'Vault KV v2',
              state: SecretStoreState.AVAILABLE,
            },
            {
              name: 'secretStores/licensed',
              kind: 'kv-v2',
              kindLabel: 'Vault KV v2',
              state: SecretStoreState.NEEDS_ENTERPRISE,
            },
            {
              name: 'secretStores/newer',
              kind: 'newer',
              kindLabel: 'Newer',
              state: 99 as SecretStoreState,
            },
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
              kind: 'kv-v2',
              kindLabel: 'Vault KV v2',
              state: SecretStoreState.AVAILABLE,
            },
          },
          {
            workspace: closed,
            workspaceDisplayName: 'Closed',
            workspaceState: WorkspaceState.ARCHIVED,
            secretStore: { name: 'secretStores/removed', state: SecretStoreState.REMOVED },
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

function submit(fields: Record<string, string | string[]>) {
  const form = new FormData()
  form.set('requestId', requestId)
  for (const [key, value] of Object.entries(fields))
    for (const one of [value].flat()) form.append(key, one)
  const request = new Request(url, { method: 'POST', headers: { origin }, body: form })
  return action(authRouteTestArgs(request, {}, null))
}
const grant = { intent: 'update', name: 'secretStores/platform', grant: research }

// Catches: an owner seeing a resource name, a store Coral cannot read shown as
// usable, a newer server's state breaking the page, or a grant whose store config
// removed vanishing, so nobody could see or revoke it.
it('shows each store with its workspaces and why Coral cannot read it', async () => {
  const result = await load()
  expect(result?.data.stores).toEqual([
    { name: 'licensed', kind: 'Vault KV v2', blockers: ['Needs Enterprise'], workspaces: [] },
    { name: 'newer', kind: 'Newer', blockers: ['Unknown state'], workspaces: [] },
    {
      name: 'platform',
      kind: 'Vault KV v2',
      blockers: [],
      workspaces: [{ name: research, displayName: 'Research', archived: false }],
    },
    {
      name: 'removed',
      kind: null,
      blockers: ['Removed from config'],
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

// Catches: Desktop, which turned secret stores off, still opening the page by its URL.
it('does not exist where the deployment turned secret stores off', async () => {
  disabled = true
  const failure: unknown = await load().catch((error: unknown) => error)
  expect(failure).toMatchObject({ status: 404 })
})

// Catches: the form's one key reused across changes, so the second change is refused
// as a repeat, a revocation refused as a repeat of its grant, or a revocation sent before
// the grants, which takes access away before an edit that fails halfway gives any.
it('saves an edit as one change per workspace, grants first, each with a key of its own', async () => {
  const result = await submit({ ...grant, grant: [research, closed], revoke: research })
  expect(result.data).toMatchObject({ error: null, message: 'Workspaces saved.' })
  expect(calls.map(({ method, workspace }) => [method, workspace])).toEqual([
    ['grant', research],
    ['grant', closed],
    ['revoke', research],
  ])
  expect(new Set(calls.map((call) => call.requestId)).size).toBe(3)
  expect(calls.every((call) => call.secretStore === 'secretStores/platform')).toBe(true)
})

// Catches: an edit that goes on after a refusal, so the page reports one failure but
// saved changes after it that the dialog then offers again.
it('stops at the first change Coral refuses, keeping the ones before it', async () => {
  refuseChange = `revoke ${closed}`
  const result = await submit({ ...grant, revoke: [closed, research] })
  expect(result.init?.status).toBe(409)
  expect(calls.map(({ method, workspace }) => [method, workspace])).toEqual([['grant', research]])
})

// Catches: a forged field reaching Coral as another kind of resource.
it.each([
  ['an unknown intent', { ...grant, intent: 'grant' }],
  ['a bad store', { ...grant, name: 'workspaces/other' }],
  ['a bad workspace to grant', { ...grant, grant: 'secretStores/other' }],
  ['a bad workspace to revoke', { ...grant, revoke: 'secretStores/other' }],
  ['no change', { intent: 'update', name: 'secretStores/platform' }],
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
  expect(result.data.error).toBe(
    'You cannot grant this secret store, because config.toml does not define it. Refresh this page.',
  )
})

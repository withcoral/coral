import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { SecretStoreState } from '@/generated/coral/management/v1/secret_store_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { loader } from './organization-secret-stores'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))
const url = 'http://127.0.0.1:3000/organization/secret-stores'
const load = () => loader(authRouteTestArgs(new Request(url), {}, null))
let refuse: ConnectError | null

beforeEach(() => {
  refuse = null
  coral.transport = fakeManagementTransport({
    secretStores: {
      listSecretStores: () => {
        if (refuse) throw refuse
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
              name: 'secretStores/elsewhere',
              kind: 'other',
              kindLabel: 'other',
              state: SecretStoreState.KIND_MISSING,
            },
            { name: 'secretStores/newer', kindLabel: 'Newer', state: 99 as SecretStoreState },
          ],
        }
      },
    },
  })
})

// Catches: an owner seeing a resource name instead of the store's name, a store
// that cannot read shown as usable, or a newer server's state breaking the page.
it('shows each store by name, kind and why Coral cannot read it', async () => {
  const result = await load()
  expect(result?.data.stores).toEqual([
    { name: 'platform', kind: 'Vault KV v2', blockers: [] },
    { name: 'licensed', kind: 'Vault KV v2', blockers: ['Needs Enterprise'] },
    { name: 'elsewhere', kind: 'other', blockers: ['Not in this build'] },
    { name: 'newer', kind: 'Newer', blockers: ['Unknown state'] },
  ])
})

// Catches: a member who reaches the URL getting anything but Coral's refusal.
it('passes on the refusal when the caller is not an organization owner', async () => {
  refuse = new ConnectError('permission denied', Code.PermissionDenied)
  const failure: unknown = await load().catch((error: unknown) => error)
  expect(failure).toMatchObject({ status: 403 })
})

import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { SecretStoreKind } from '@/generated/coral/management/v1/secret_store_pb'
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
            { name: 'secretStores/platform', kind: SecretStoreKind.KV_V2 },
            { name: 'secretStores/newer', kind: SecretStoreKind.UNSPECIFIED },
          ],
        }
      },
    },
  })
})

// Catches: an owner seeing a resource name or a raw enum instead of the store's
// name and kind, or a newer server's kind breaking the page.
it('shows each store by name and kind', async () => {
  const result = await load()
  expect(result?.data.stores).toEqual([
    { name: 'platform', kind: 'KV v2' },
    { name: 'newer', kind: 'Unknown' },
  ])
})

// Catches: a member who reaches the URL getting anything but Coral's refusal.
it('passes on the refusal when the caller is not an organization owner', async () => {
  refuse = new ConnectError('permission denied', Code.PermissionDenied)
  const failure: unknown = await load().catch((error: unknown) => error)
  expect(failure).toMatchObject({ status: 403 })
})

import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { loader } from './provider-icon'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))

const workspace = '29a180e5-397c-4844-92de-8bbea6066235'
const provider = '11111111-1111-4111-8111-111111111111'
const svg = new TextEncoder().encode('<svg xmlns="http://www.w3.org/2000/svg"><script/></svg>')
let requested: string[]

beforeEach(() => {
  requested = []
  coral.transport = fakeManagementTransport({
    providers: {
      getProviderIcon: ({ name }) => {
        requested.push(name)
        return { mediaType: 'image/svg+xml', content: svg, digest: 'digest-1' }
      },
    },
  })
})

/** Reads the icon as the browser does, whether the loader returns or throws its response. */
async function load(query = '') {
  try {
    return (await loader(
      authRouteTestArgs(
        new Request(`https://reef.test/workspaces/${workspace}/providers/${provider}/icon${query}`),
        { workspace, provider },
      ) as never,
    )) as Response
  } catch (thrown) {
    if (thrown instanceof Response) return thrown
    throw thrown
  }
}

// Catches: an uploaded SVG served so that opening its URL runs its script,
// or a browser sniffing the bytes as another type.
it('serves the image with its type in a sandbox', async () => {
  const response = await load('?v=digest-1')
  expect(requested).toEqual([`workspaces/${workspace}/providers/${provider}`])
  expect(response.status).toBe(200)
  expect(response.headers.get('Content-Type')).toBe('image/svg+xml')
  expect(response.headers.get('X-Content-Type-Options')).toBe('nosniff')
  expect(response.headers.get('Content-Security-Policy')).toBe(
    "default-src 'none'; style-src 'unsafe-inline'; sandbox",
  )
  expect(new Uint8Array(await response.arrayBuffer())).toEqual(svg)
})

// Catches: a browser keeping an old icon after an owner replaces it.
it.each([
  ['the current digest', '?v=digest-1', 'private, max-age=31536000, immutable'],
  ['an earlier digest', '?v=digest-0', 'private, no-store'],
  ['no digest', '', 'private, no-store'],
])('caches for good only under %s', async (_, query, cacheControl) => {
  expect((await load(query)).headers.get('Cache-Control')).toBe(cacheControl)
})

it('answers 404 when the provider has no uploaded icon', async () => {
  coral.transport = fakeManagementTransport({
    providers: {
      getProviderIcon: () => {
        throw new ConnectError('PROVIDER_ICON_NOT_FOUND', Code.NotFound)
      },
    },
  })
  const response = await load('?v=digest-1')
  expect(response.status).toBe(404)
  expect(response.headers.get('Cache-Control')).toBe('private, no-store')
})

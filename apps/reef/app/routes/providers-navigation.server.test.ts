import type { RouteConfigEntry } from '@react-router/dev/routes'
import { Code, ConnectError } from '@connectrpc/connect'
import {
  createMemoryRouter,
  isRouteErrorResponse,
  matchRoutes,
  type RouteObject,
} from 'react-router'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { authTestContext } from '@/auth/server-context.test-helper'
import { fakeManagement } from '@/lib/management.test-support'
import routes from '../routes'
import * as providers from './providers'
import { CredentialRoute_Kind } from '@/generated/coral/management/v1/provider_pb'

const TOOL_COUNT = 1229
let toolCount = TOOL_COUNT

const mocks = vi.hoisted(() => ({
  workspace: vi.fn(),
  tools: vi.fn(),
  providers: vi.fn(),
  policies: vi.fn(),
  userAccounts: vi.fn(),
}))
vi.mock('@/lib/workspace-route.server', () => ({ authorizedWorkspace: mocks.workspace }))
vi.mock('@/views/providers/workspace-providers-page', () => ({
  WorkspaceProvidersPage: () => null,
}))

const workspace = '29a180e5-397c-4844-92de-8bbea6066235'
const providerId = '11111111-1111-4111-8111-111111111111'
const base = `/workspaces/${workspace}/providers/${providerId}`
const provider = `workspaces/${workspace}/providers/11111111-1111-4111-8111-111111111111`
const api = fakeManagement({
  providers: { listProviders: mocks.providers, listTools: mocks.tools },
  policies: { getToolPolicies: mocks.policies },
  accounts: { listUserAccounts: mocks.userAccounts },
})
const routers: ReturnType<typeof createMemoryRouter>[] = []

beforeEach(() => {
  vi.resetAllMocks()
  toolCount = TOOL_COUNT
  mocks.workspace.mockResolvedValue({
    api,
    workspace: {
      name: `workspaces/${workspace}`,
      path: `/workspaces/${workspace}`,
      effectiveRole: 'owner',
      state: 'active',
    },
  })
  mocks.providers.mockResolvedValue({
    providers: [
      {
        name: provider,
        displayName: 'GitHub',
        namespace: 'github',
        state: 1,
        credentialRoute: { kind: 1 },
        blockers: [],
        etag: 'test',
      },
    ],
    nextPageToken: '',
  })
  mocks.userAccounts.mockResolvedValue({ accounts: [] })
  mocks.policies.mockImplementation(({ tools }: { tools: string[] }) => ({
    policies: tools.map((tool) => ({ tool, accessState: 3, explicit: true })),
  }))
  // 1,229 tools span several pages of the loader's page size.
  mocks.tools.mockImplementation(
    ({ pageToken = '', pageSize }: { pageToken: string; pageSize: number }) => {
      const start = Number(pageToken)
      const end = Math.min(start + pageSize, toolCount)
      return Promise.resolve({
        tools: Array.from({ length: end - start }, (_, i) => ({
          name: `${provider}/tools/tool${start + i}`,
          relativePath: `actions.tool${start + i}`,
          summary: `Tool ${start + i}`,
          effect: 3,
          available: true,
          diagnostics: [],
          tags: ['actions'],
        })),
        nextPageToken: end < toolCount ? String(end) : '',
      })
    },
  )
})
afterEach(() => {
  for (const router of routers.splice(0)) router.dispose()
  vi.useRealTimers()
})

function find(entries: RouteConfigEntry[]): RouteConfigEntry[] {
  return entries.flatMap((entry) =>
    entry.file === 'routes/providers.tsx' ? [entry] : find(entry.children ?? []),
  )
}

async function convert(entry: RouteConfigEntry): Promise<RouteObject> {
  const module =
    entry.file === 'routes/providers.tsx' ? providers : await import('./providers-selection')
  return {
    id: entry.id ?? entry.file,
    path: entry.path,
    index: entry.index,
    loader: module.loader,
    shouldRevalidate: module.shouldRevalidate,
    children: entry.children && (await Promise.all(entry.children.map(convert))),
  } as RouteObject
}

async function makeRouter(entry = `${base}/tools/actions.tool0`) {
  const config = await routes
  const router = createMemoryRouter(await Promise.all(find(config).map(convert)), {
    initialEntries: [entry],
    getContext: () => authTestContext(null),
  })
  routers.push(router)
  await new Promise<void>((resolve) => {
    if (router.state.initialized) return resolve()
    const unsubscribe = router.subscribe((state) => {
      if (state.initialized) {
        unsubscribe()
        resolve()
      }
    })
  })
  return router
}

// Catches: initial loading serializing a second catalog-sized set of policy reads.
it.each([101, TOOL_COUNT])(
  'loads %i tools and saved policies within one paging pass plus one request',
  async (count) => {
    toolCount = count
    vi.useFakeTimers()
    const requestLatency = 50
    for (const mock of [mocks.tools, mocks.policies]) {
      const respond = mock.getMockImplementation()!
      mock.mockImplementation(async (...args: unknown[]) => {
        await new Promise((resolve) => setTimeout(resolve, requestLatency))
        return respond(...args)
      })
    }
    const start = Date.now()
    const loading = providers.loader({
      request: new Request(`https://reef.test/workspaces/${workspace}/providers`),
      context: authTestContext(null),
      params: { workspace },
    } as never)
    await vi.runAllTimersAsync()
    const loaded = await loading
    const tools = loaded!.data.providers[0]!.tools
    expect(tools).toHaveLength(count)
    expect(tools.every((tool: { policy: string }) => tool.policy === 'disabled')).toBe(true)
    expect(Date.now() - start).toBeLessThanOrEqual((Math.ceil(count / 100) + 1) * requestLatency)
  },
)

// Catches: a failed overlapping read displaying a partial catalog or policy defaults.
it.each([
  ['catalog', Code.Unavailable, 503],
  ['policy', Code.PermissionDenied, 403],
  ['policy', Code.Unavailable, 503],
] as const)(
  'rejects the page when a later %s read fails with %s',
  async (service, code, status) => {
    const mock = service === 'catalog' ? mocks.tools : mocks.policies
    const respond = mock.getMockImplementation()!
    mock.mockImplementationOnce(respond).mockRejectedValueOnce(new ConnectError('failed', code))
    const router = await makeRouter()
    const [error] = Object.values(router.state.errors ?? {})
    expect(isRouteErrorResponse(error) && error.status).toBe(status)
    expect(router.state.loaderData.providers).toBeUndefined()
  },
)

// Catches: a tool without a policy decision displaying a guessed default.
it('rejects the page when Coral leaves a tool without a policy decision', async () => {
  const respond = mocks.policies.getMockImplementation()!
  mocks.policies.mockImplementation(async (request: { tools: string[] }) => {
    const { policies } = await respond(request)
    return { policies: policies.slice(1) }
  })
  const router = await makeRouter()
  expect(Object.values(router.state.errors ?? {})).toHaveLength(1)
  expect(router.state.loaderData.providers).toBeUndefined()
})

it('keeps the 1,229-tool tree out of tool navigation responses, but reauthorizes each click', async () => {
  const router = await makeRouter()
  expect(router.state.errors).toBeNull()
  const tree = router.state.loaderData.providers
  expect(tree.providers[0].tools).toHaveLength(TOOL_COUNT)
  const initialAuthCalls = mocks.workspace.mock.calls.length
  await router.navigate(`${base}/tools/actions.tool1`)
  expect(router.state.errors).toBeNull()
  expect(router.state.loaderData.providers).toBe(tree)
  expect(mocks.workspace.mock.calls.length).toBeGreaterThan(initialAuthCalls)
  expect(JSON.stringify(router.state.loaderData['providers-tool']).length).toBeLessThan(2000)

  mocks.workspace.mockRejectedValueOnce(new Response('Forbidden', { status: 403 }))
  await router.navigate(`${base}/tools/actions.tool2`)
  expect(router.state.errors).not.toBeNull()
})

it('reloads the catalog on an explicit refresh', async () => {
  const router = await makeRouter()
  expect(router.state.errors).toBeNull()
  const tree = router.state.loaderData.providers
  await router.revalidate()
  expect(router.state.loaderData.providers).not.toBe(tree)
})

it('never suppresses mutation-driven revalidation', () => {
  expect(
    providers.shouldRevalidate({
      currentUrl: new URL(`https://reef.test${base}/tools/actions.tool0`),
      nextUrl: new URL(`https://reef.test${base}/tools/actions.tool1`),
      formMethod: 'POST',
      defaultShouldRevalidate: true,
    } as never),
  ).toBe(true)
})

function decide(from: string, to: string) {
  return providers.shouldRevalidate({
    currentUrl: new URL(`https://reef.test${from}`),
    nextUrl: new URL(`https://reef.test${to}`),
    defaultShouldRevalidate: true,
  } as never)
}

// Catches: the parent loader re-fetching the whole catalog when the rail or a
// row only changes which provider or tool the address names.
it('reuses the tree across selection changes on the page, but not on leaving it', async () => {
  const router = await makeRouter()
  expect(router.state.errors).toBeNull()
  const tree = router.state.loaderData.providers
  const page = `${base.replace(/\/[^/]+$/, '')}`
  const other = `${page}/22222222-2222-4222-8222-222222222222`
  for (const next of [other, page, base]) {
    await router.navigate(next)
    expect(router.state.errors).toBeNull()
    expect(router.state.loaderData.providers).toBe(tree)
  }
  expect(decide(page, `${page}/new`)).toBe(true)
  expect(decide(`${page}/new`, page)).toBe(true)
  expect(decide(page, page.replace('providers', 'agents'))).toBe(true)
  // Settings is a page of its own; returning from it reloads the catalog.
  expect(decide(`${base}/settings`, base)).toBe(true)
})

// Catches: an address the catalog cannot name rendering a page.
it.each([`/workspaces/${workspace}/providers/not-a-uuid`, `${base}/tools/missingTool`])(
  'answers 404 for %s',
  async (path) => {
    const router = await makeRouter(path)
    const [error] = Object.values(router.state.errors ?? {})
    expect(isRouteErrorResponse(error) && error.status).toBe(404)
  },
)

function toObjects(entries: RouteConfigEntry[]): RouteObject[] {
  return entries.map(
    (entry) =>
      ({
        path: entry.path,
        index: entry.index,
        children: entry.children && toObjects(entry.children),
      }) as RouteObject,
  )
}

// Catches: an organization-wide provider page coming back. Providers belong
// to one workspace, so no route serves them at the organization.
it.each([
  '/organization/providers',
  '/organization/providers/new',
  `/organization/providers/${providerId}`,
])('serves no page at %s', async (path) => {
  expect(matchRoutes(toObjects(await routes), path)).toBeNull()
})

// Catches: the page losing your account state, or folding it into Coral's
// provider blockers.
it('reads your own account apart from the provider blockers', async () => {
  const listed = await mocks.providers()
  mocks.providers.mockResolvedValue({
    ...listed,
    providers: [{ ...listed.providers[0], credentialRoute: { kind: CredentialRoute_Kind.CALLER } }],
  })
  mocks.userAccounts.mockResolvedValue({ accounts: [{ provider, account: {} }] })
  const router = await makeRouter()
  const [view] = router.state.loaderData.providers.providers
  expect(view.blockers).toEqual([])
  expect(view.personalAccountAction).toBe('Connect your account')
})

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
import * as tools from './tools'
import { CredentialRoute_Kind } from '@/generated/coral/management/v1/provider_pb'
import { selectionKey } from '@/views/providers/selection'

const TOOL_COUNT = 1229
let toolCount = TOOL_COUNT

const mocks = vi.hoisted(() => ({
  workspace: vi.fn(),
  tools: vi.fn(),
  tool: vi.fn(),
  providers: vi.fn(),
  userAccounts: vi.fn(),
  summary: vi.fn(),
  privateTools: vi.fn(),
  sharedTools: vi.fn(),
  provider: vi.fn(),
}))
vi.mock('@/lib/workspace-route.server', () => ({ authorizedWorkspace: mocks.workspace }))
vi.mock('@/views/tools/tools-page', () => ({
  ToolsPage: () => null,
}))

const workspace = '29a180e5-397c-4844-92de-8bbea6066235'
const providerId = '11111111-1111-4111-8111-111111111111'
const base = `/workspaces/${workspace}/providers/${providerId}`
const provider = `workspaces/${workspace}/providers/11111111-1111-4111-8111-111111111111`
const privateProvider = `workspaces/${workspace}/providers/99999999-9999-4999-8999-999999999999`
const sharedProvider = `workspaces/${workspace}/providers/88888888-8888-4888-8888-888888888888`
const tabs = `/workspaces/${workspace}/tools`
const api = fakeManagement({
  providers: { listProviders: mocks.providers, getTool: mocks.tool, getProvider: mocks.provider },
  policies: { listToolAccess: mocks.tools },
  privateTools: { listPrivateTools: mocks.privateTools },
  sharedTools: { listSharedTools: mocks.sharedTools },
  accounts: { listUserAccounts: mocks.userAccounts },
  activity: { summarizeActivity: mocks.summary },
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
  mocks.summary.mockResolvedValue({})
  // The shared and the private group, which these tests leave empty.
  mocks.privateTools.mockResolvedValue({ privateProvider })
  mocks.sharedTools.mockResolvedValue({ sharedProvider })
  mocks.provider.mockImplementation(({ name }: { name: string }) => ({
    name,
    displayName: name === sharedProvider ? 'Shared' : 'Private',
    state: 1,
  }))
  mocks.tool.mockImplementation(({ relativePath }: { relativePath: string }) =>
    relativePath.startsWith('actions.tool')
      ? { summary: `Tool ${relativePath}`, description: 'The prose.' }
      : Promise.reject(new ConnectError('NOT_FOUND', Code.NotFound)),
  )
  // 1,229 tools span two pages of the loader's page size.
  mocks.tools.mockImplementation(
    ({
      parent,
      pageToken = '',
      pageSize,
    }: {
      parent: string
      pageToken: string
      pageSize: number
    }) => {
      if (parent === privateProvider || parent === sharedProvider)
        return Promise.resolve({ tools: [] })
      const start = Number(pageToken)
      const end = Math.min(start + pageSize, toolCount)
      return Promise.resolve({
        tools: Array.from({ length: end - start }, (_, i) => ({
          tool: `${provider}/tools/${Buffer.from(`actions/tool${start + i}`).toString('base64url')}`,
          relativePath: `actions.tool${start + i}`,
          accessState: 3,
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
    entry.file === 'routes/tools.tsx' ? [entry] : find(entry.children ?? []),
  )
}

async function convert(entry: RouteConfigEntry): Promise<RouteObject> {
  const module = entry.file === 'routes/tools.tsx' ? tools : await import('./tools-selection')
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

// Catches: the load going back to one read per hundred tools, or to a second
// catalog-sized set of policy reads.
it.each([
  [101, 1],
  [TOOL_COUNT, 2],
])('loads %i tools and their policies in %i reads', async (count, reads) => {
  toolCount = count
  const loaded = await tools.loader({
    request: new Request(`https://reef.test${tabs}`),
    context: authTestContext(null),
    params: { workspace },
  } as never)
  const listed = loaded!.data.providers[0]!.tools
  expect(listed).toHaveLength(count)
  expect(listed.every((tool: { policy: string }) => tool.policy === 'disabled')).toBe(true)
  expect(mocks.tools.mock.calls.filter(([call]) => call.parent === provider)).toHaveLength(reads)
})

const toolName = (key: string) => `${provider}/tools/${Buffer.from(key).toString('base64url')}`
const loadTree = async () =>
  (await tools.loader({
    request: new Request(`https://reef.test${tabs}`),
    context: authTestContext(null),
    params: { workspace },
  } as never))!.data

// Catches: a tool's calls credited to the tool that holds its Call Path now, a window
// other than the 30 days the column names, or a failed summary that blanks the catalog
// or shows zero calls.
it('counts calls by provider and Tool Key, and leaves them out when the summary is unavailable', async () => {
  toolCount = 2
  mocks.summary.mockResolvedValue({
    tools: [
      // Called when its path was `actions.tool1`, which another tool holds now.
      {
        tool: toolName('actions/tool0'),
        callPath: 'coral.providers.github.actions.tool1',
        calls: 5n,
        daily: [1n, 0n, 4n],
      },
    ],
  })
  const tree = await loadTree()
  expect(mocks.summary.mock.lastCall?.[0]).toMatchObject({
    workspace: `workspaces/${workspace}`,
    days: 30,
  })
  const usage = (key: string) => tree.toolCalls?.tools[selectionKey(providerId, key)]
  expect(tree.toolCalls?.days).toBe(30)
  expect([usage('actions/tool0'), usage('actions/tool1')]).toEqual([
    { calls: 5, daily: [1, 0, 4], callers: [] },
    undefined,
  ])

  mocks.summary.mockRejectedValue(new ConnectError('unavailable', Code.Unavailable))
  const unavailable = await loadTree()
  expect(unavailable.toolCalls).toBeNull()
  expect(unavailable.providers[0]!.tools).toHaveLength(2)
})

// Catches: a failed later page displaying a partial catalog.
it.each([
  [Code.Unavailable, 503],
  [Code.PermissionDenied, 403],
] as const)('rejects the page when a later read fails with %s', async (code, status) => {
  const respond = mocks.tools.getMockImplementation()!
  mocks.tools.mockImplementation((request: { parent: string; pageToken: string }) =>
    request.parent === provider && request.pageToken
      ? Promise.reject(new ConnectError('failed', code))
      : respond(request),
  )
  const router = await makeRouter()
  const [error] = Object.values(router.state.errors ?? {})
  expect(isRouteErrorResponse(error) && error.status).toBe(status)
  expect(router.state.loaderData.tools).toBeUndefined()
})

it('keeps the 1,229-tool tree out of tool navigation responses, but reauthorizes each click', async () => {
  const router = await makeRouter()
  expect(router.state.errors).toBeNull()
  const tree = router.state.loaderData.tools
  expect(tree.providers[0].tools).toHaveLength(TOOL_COUNT)
  const initialAuthCalls = mocks.workspace.mock.calls.length
  await router.navigate(`${base}/tools/actions.tool1`)
  expect(router.state.errors).toBeNull()
  expect(router.state.loaderData.tools).toBe(tree)
  expect(mocks.workspace.mock.calls.length).toBeGreaterThan(initialAuthCalls)
  expect(JSON.stringify(router.state.loaderData['tools-provider-tool']).length).toBeLessThan(2000)
  expect(router.state.loaderData['tools-provider-tool'].selectedToolDetail).toEqual({
    kind: 'provider',
    relativePath: 'actions.tool1',
    summary: 'Tool actions.tool1',
    description: 'The prose.',
  })
  expect(mocks.tool.mock.lastCall?.[0]).toMatchObject({
    parent: provider,
    relativePath: 'actions.tool1',
  })

  mocks.workspace.mockRejectedValueOnce(new Response('Forbidden', { status: 403 }))
  await router.navigate(`${base}/tools/actions.tool2`)
  expect(router.state.errors).not.toBeNull()
})

it('reloads the catalog on an explicit refresh', async () => {
  const router = await makeRouter()
  expect(router.state.errors).toBeNull()
  const tree = router.state.loaderData.tools
  await router.revalidate()
  expect(router.state.loaderData.tools).not.toBe(tree)
})

function policyForm() {
  const form = new FormData()
  form.set('intent', 'policy')
  return form
}
const updated = { provider: providerId, tool: 'tool', status: 'updated' }

// Catches: a policy toggle reloading the whole catalog, or a result the page
// cannot apply leaving it to show what Coral did not save.
it.each([
  ['any other mutation', new FormData(), { error: null, message: 'Saved.' }, true],
  ['a saved policy', policyForm(), { error: null, tools: [updated], blockers: {} }, false],
  ['a policy without blockers', policyForm(), { error: null, tools: [updated] }, true],
  [
    'a policy partly saved',
    policyForm(),
    {
      error: null,
      blockers: {},
      tools: [updated, { ...updated, status: 'failed', reason: 'denied' }],
    },
    true,
  ],
  ['a rejected policy', policyForm(), { error: 'Denied.' }, true],
  ['a lost response', policyForm(), undefined, true],
])('after %s, reloads the catalog: %#', (_label, formData, actionResult, reloads) => {
  expect(
    tools.shouldRevalidate({
      currentUrl: new URL(`https://reef.test${base}/tools/actions.tool0`),
      nextUrl: new URL(`https://reef.test${base}/tools/actions.tool1`),
      formMethod: 'POST',
      formData,
      actionResult,
      defaultShouldRevalidate: true,
    } as never),
  ).toBe(reloads)
})

function decide(from: string, to: string) {
  return tools.shouldRevalidate({
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
  const tree = router.state.loaderData.tools
  const page = `${base.replace(/\/[^/]+$/, '')}`
  for (const next of [tabs, `${tabs}/private`, `${tabs}/shared`, base, tabs]) {
    await router.navigate(next)
    expect(router.state.errors).toBeNull()
    expect(router.state.loaderData.tools).toBe(tree)
  }
  expect(decide(tabs, `${page}/new`)).toBe(true)
  expect(decide(`${page}/new`, tabs)).toBe(true)
  expect(decide(tabs, tabs.replace('tools', 'agents'))).toBe(true)
  // An agent's private tool is a selection on the page, as your own is.
  expect(decide(`${tabs}/private`, `${tabs}/private/people.greet/agents/worker`)).toBe(false)
  expect(decide(`${tabs}/private/people.greet/agents/worker`, `${tabs}/shared/mail.remind`)).toBe(
    false,
  )
  // Another workspace's page holds another catalog.
  expect(decide(tabs, tabs.replace(workspace, workspace.replace('2', '3')))).toBe(true)
  // Settings is a page of its own; returning from it reloads the catalog.
  expect(decide(`${base}/settings`, base)).toBe(true)
})

// Catches: an address the catalog cannot name rendering a page.
it.each([
  `/workspaces/${workspace}/providers/not-a-uuid`,
  `/workspaces/${workspace}/providers/22222222-2222-4222-8222-222222222222`,
  `${base}/tools/missingTool`,
  `${tabs}/unknown`,
  `${tabs}/shared/missingTool`,
  `${tabs}/private/missingTool`,
  `${tabs}/private/missingTool/agents/worker`,
])('answers 404 for %s', async (path) => {
  const router = await makeRouter(path)
  const [error] = Object.values(router.state.errors ?? {})
  expect(isRouteErrorResponse(error) && error.status).toBe(404)
})

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
  const [view] = router.state.loaderData.tools.providers
  expect(view.blockers).toEqual([])
  expect(view.personalAccountAction).toBe('Connect your account')
})

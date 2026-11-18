/** @vitest-environment happy-dom */
import type { MessageInitShape } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { act, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import {
  createMemoryRouter,
  RouterProvider,
  useActionData,
  useLoaderData,
  type ActionFunctionArgs,
  type LoaderFunctionArgs,
} from 'react-router'
import { afterEach, expect, it, onTestFinished, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import type { SummarizeActivityResponseSchema } from '@/generated/coral/management/v1/activity_pb'
import { ToolAccessState } from '@/generated/coral/management/v1/policy_pb'
import { ProviderState } from '@/generated/coral/management/v1/provider_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import type { Route } from './+types/tools'
import type { Route as SelectionRoute } from './+types/tools-selection'
import ToolsRoute, { action, loader, shouldRevalidate } from './tools'
import ToolsSelectionRoute, {
  loader as selectionLoader,
  shouldRevalidate as selectionRevalidate,
} from './tools-selection'

// The Tools page through its own loaders and action, over a Coral whose usage summary
// answers only when a test says so. The catalog, its search, a selected tool and a policy
// change never wait for the summary, and a summary that fails reads as unavailable.
// Catches: a catalog that waits for a slow summary, a policy change held behind it, and
// usage shown as zero calls while it loads or once it failed.
// activity-summary.server.test.ts owns which failures read as unavailable.

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))

// happy-dom lays nothing out, so the virtualizer would mount no tool rows.
const isTable = (element: HTMLElement) => element.getAttribute('aria-label') === 'Tools'
Object.defineProperty(HTMLElement.prototype, 'clientHeight', {
  get() {
    return isTable(this) ? 2000 : 0
  },
})
Object.defineProperty(HTMLElement.prototype, 'clientWidth', {
  get() {
    return isTable(this) ? 1000 : 0
  },
})

afterEach(() => vi.restoreAllMocks())

const id = '00000000-0000-4000-8000-000000000001'
const parent = `workspaces/${id}`
const tools = `/workspaces/${id}/tools`
const githubId = '00000000-0000-4000-8000-0000000000a1'
const github = `${parent}/providers/${githubId}`
const toolName = (key: string) => `${github}/tools/${Buffer.from(key).toString('base64url')}`
type Summary = MessageInitShape<typeof SummarizeActivityResponseSchema>
/** `calls` spread over `days`, the remainder on the last day. */
const series = (calls: number, days: number) =>
  Array.from({ length: days }, (_, day) =>
    BigInt(Math.floor(calls / days) + (day === days - 1 ? calls % days : 0)),
  )

interface Read {
  /**
   * Answers with `calls` to `issues.list` across the read's days, as Coral would once the
   * summary is ready: one count per day, oldest first, that add up to `calls`.
   */
  answer: (calls: number) => Promise<void>
  /** Fails, as Coral would. */
  fail: (code: Code) => Promise<void>
}

// The server's arguments, as a same-origin request with auth off. The DOM's Request drops
// an Origin header, so the browser's one is added as a server would read it.
const server = ({ request, params }: LoaderFunctionArgs | ActionFunctionArgs) => {
  const origin = new URL(request.url).origin
  const read = request.headers.get.bind(request.headers)
  request.headers.get = (name) => (name.toLowerCase() === 'origin' ? origin : read(name))
  return authRouteTestArgs(request, params as never, null) as never
}
function Tree() {
  const props = { loaderData: useLoaderData() } as unknown as Route.ComponentProps
  return <ToolsRoute {...props} />
}
function Selection() {
  const props = {
    loaderData: useLoaderData(),
    actionData: useActionData(),
  } as unknown as SelectionRoute.ComponentProps
  return <ToolsSelectionRoute {...props} />
}

/** A selection route of the page, as the route config names one. */
const selection = (path: string) => ({
  path,
  loader: (args: LoaderFunctionArgs) => selectionLoader(server(args)),
  action: (args: ActionFunctionArgs) => action(server(args)),
  shouldRevalidate: selectionRevalidate,
  Component: Selection,
})

/** Serves the Tools page at `entry` from an in-memory Coral that holds every summary read. */
async function renderTools(entry = tools) {
  const reads: Read[] = []
  const saved: { tool: string; accessState: ToolAccessState }[] = []
  coral.transport = fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => ({
        name,
        displayName: 'Research',
        effectiveRole: WorkspaceRole.OWNER,
        state: WorkspaceState.ACTIVE,
        etag: 'etag',
      }),
    },
    providers: {
      listProviders: () => ({
        providers: [
          { name: github, displayName: 'GitHub', namespace: 'github', state: ProviderState.ACTIVE },
        ],
      }),
      getProvider: ({ name }) => ({ name, displayName: name, state: ProviderState.ACTIVE }),
      getTool: ({ relativePath }) => ({ relativePath, summary: `Runs ${relativePath}` }),
    },
    policies: {
      listToolAccess: ({ parent: provider }) => ({
        tools:
          provider === github
            ? ['issues.list', 'issues.create', 'repos.get'].map((path) => ({
                tool: toolName(path),
                relativePath: path,
                available: true,
                accessState: ToolAccessState.APPROVAL_REQUIRED,
              }))
            : [],
      }),
      updateToolPolicies: ({ policies }) => {
        for (const { tool, accessState } of policies) saved.push({ tool, accessState })
        return {}
      },
    },
    accounts: { listUserAccounts: () => ({ accounts: [] }) },
    privateTools: { listPrivateTools: () => ({ privateProvider: `${parent}/providers/p` }) },
    sharedTools: { listSharedTools: () => ({ sharedProvider: `${parent}/providers/s` }) },
    activity: {
      summarizeActivity: ({ days }) =>
        new Promise<Summary>((resolve, reject) =>
          reads.push({
            answer: (calls) =>
              act(async () =>
                resolve({
                  tools: [
                    {
                      tool: toolName('issues.list'),
                      calls: BigInt(calls),
                      daily: series(calls, days),
                    },
                  ],
                }),
              ),
            fail: (code) => act(async () => reject(new ConnectError('summary', code))),
          }),
        ),
    },
  })
  const router = createMemoryRouter(
    [
      {
        id: 'tools',
        loader: (args) => loader(server(args)),
        action: (args) => action(server(args)),
        shouldRevalidate,
        Component: Tree,
        children: [
          selection('/workspaces/:workspace/tools'),
          selection('/workspaces/:workspace/providers/:provider'),
          selection('/workspaces/:workspace/providers/:provider/tools/:tool'),
        ],
      },
    ],
    { initialEntries: [entry] },
  )
  onTestFinished(() => router.dispose())
  render(<RouterProvider router={router} />)
  await screen.findByRole('table', { name: 'Tools' })
  // The summary settles once the page's effects run.
  await act(async () => {})
  return { reads, router, saved }
}

const user = userEvent.setup()
const table = () => screen.getByRole('table', { name: 'Tools' })
// happy-dom splits a name at its bolded matches, so links are found by their text.
const linkOf = (tool: string) =>
  within(table())
    .getAllByRole('link')
    .find((link) => link.textContent === tool)!
const rowOf = (tool: string) => linkOf(tool).closest<HTMLElement>('[role="row"]')!
/** The tools the table lists; a group's heading links to its group, not a tool. */
const rowNames = () =>
  within(table())
    .queryAllByRole('link')
    .filter((link) => /\/providers\/[^/]+\/tools\//.test(link.getAttribute('href')!))
    .map((link) => link.textContent)
/** The Usage cell of `tool`'s row. */
const usageOf = (tool: string) => {
  const column = within(table())
    .getAllByRole('columnheader')
    .findIndex((header) => header.textContent?.startsWith('Usage'))
  return within(rowOf(tool)).getAllByRole('cell')[column]!.textContent
}
const loadingUsage = () => within(table()).queryByRole('status')
const drawerUsage = () => within(screen.getByRole('dialog')).getByRole('region', { name: 'Usage' })

it('lists, searches and opens the whole catalog while the usage loads, then shows it', async () => {
  const { reads, router } = await renderTools()
  expect(reads).toHaveLength(1)
  expect(rowNames()).toEqual(['issues.create', 'issues.list', 'repos.get'])
  expect(loadingUsage()).toHaveTextContent('Loading usage…')
  expect(usageOf('issues.list')).toBe('')

  await user.type(screen.getByRole('searchbox', { name: 'Filter tools' }), 'issues')
  expect(rowNames()).toEqual(['issues.create', 'issues.list'])

  await user.click(linkOf('issues.list'))
  // The router commits a navigation in a transition, after its state already names the tool.
  await screen.findByRole('dialog')
  expect(router.state.location.pathname).toBe(
    `/workspaces/${id}/providers/${githubId}/tools/issues.list`,
  )
  expect(drawerUsage()).toHaveTextContent('Loading usage…')

  await reads[0]!.answer(42)
  expect(loadingUsage()).toBeNull()
  expect(usageOf('issues.list')).toBe('42 calls')
  expect(usageOf('issues.create')).toBe('0 calls')
  expect(within(drawerUsage()).getByRole('group', { name: 'Calls in 30 days' })).toHaveTextContent(
    '42',
  )
})

it('saves a policy change while the usage loads, and still shows the usage after', async () => {
  const { reads, router, saved } = await renderTools()
  await user.click(within(rowOf('repos.get')).getByRole('radio', { name: 'Allow' }))
  await vi.waitFor(() => expect(saved).toHaveLength(1))
  await vi.waitFor(() => expect([...router.state.fetchers.values()]).toEqual([]))
  expect(saved).toEqual([{ tool: toolName('repos.get'), accessState: ToolAccessState.ENABLED }])
  expect(within(rowOf('repos.get')).getByRole('radio', { name: 'Allow' })).toBeChecked()
  // A saved policy patches the page; it reads neither the catalog nor the summary again.
  expect(reads).toHaveLength(1)
  expect(loadingUsage()).toHaveTextContent('Loading usage…')

  await reads[0]!.answer(7)
  expect(usageOf('issues.list')).toBe('7 calls')
})

it.each([
  ['Coral cannot give the summary now', Code.Unavailable],
  ['the summary read fails', Code.Internal],
])('keeps the catalog and shows usage as not available when %s', async (_, code) => {
  const { reads } = await renderTools(`/workspaces/${id}/providers/${githubId}/tools/issues.list`)
  await reads[0]!.fail(code)
  expect(rowNames()).toEqual(['issues.create', 'issues.list', 'repos.get'])
  expect(loadingUsage()).toBeNull()
  expect(usageOf('issues.list')).toBe('—Not available')
  expect(drawerUsage()).toHaveTextContent('Usage is not available. Try again later.')
  expect(within(table()).queryByRole('button', { name: 'Sort by usage' })).toBeNull()
})

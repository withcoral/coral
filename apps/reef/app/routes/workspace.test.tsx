/** @vitest-environment happy-dom */
import type { MessageInitShape } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { act, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import {
  createMemoryRouter,
  RouterProvider,
  useLoaderData,
  type ActionFunctionArgs,
  type LoaderFunctionArgs,
} from 'react-router'
import { afterEach, expect, it, onTestFinished, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import type { SummarizeActivityResponseSchema } from '@/generated/coral/management/v1/activity_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import type { Route } from './+types/workspace'
import WorkspaceRoute, { action, ErrorBoundary, loader } from './workspace'

// The Overview through its own loader and action, over a Coral whose summary answers only
// when a test says so. The page, a change you save and the page's refresh never wait for
// the summary, and a summary that fails never takes the page with it.
// Catches: a page that waits for a slow summary, zeros or old numbers shown as the period
// you chose, a late answer for an earlier read that replaces the current one, and a failed
// summary read that replaces the page. activity-summary.server.test.ts owns which failures
// read as unavailable.

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

const id = '00000000-0000-4000-8000-000000000001'
const path = `/workspaces/${id}`
type Summary = MessageInitShape<typeof SummarizeActivityResponseSchema>

interface Read {
  days: number
  /** Answers with `answered` calls, as Coral would once the summary is ready. */
  answer: (answered: number) => Promise<void>
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
function Page() {
  const props = { loaderData: useLoaderData() } as unknown as Route.ComponentProps
  return <WorkspaceRoute {...props} />
}

/** Serves the Overview at `entry` from an in-memory Coral that holds every summary read. */
async function renderOverview(entry = path) {
  const reads: Read[] = []
  const coralState = { displayName: 'Demo', pageReads: 0 }
  coral.transport = fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => {
        coralState.pageReads++
        return {
          name,
          displayName: coralState.displayName,
          effectiveRole: WorkspaceRole.OWNER,
          state: WorkspaceState.ACTIVE,
          etag: 'etag',
        }
      },
      updateWorkspace: ({ workspace }) => {
        coralState.displayName = workspace!.displayName
        return { name: workspace!.name, displayName: workspace!.displayName, etag: 'etag' }
      },
    },
    agents: { listAgents: () => ({ agents: [] }) },
    providers: { listProviders: () => ({ providers: [] }) },
    memberships: { listWorkspaceOwners: () => ({ owners: [] }) },
    secretStores: { listWorkspaceSecretStores: () => ({ secretStores: [] }) },
    activity: {
      summarizeActivity: ({ days }) =>
        new Promise<Summary>((resolve, reject) =>
          reads.push({
            days,
            answer: (answered) =>
              act(async () => resolve({ calls: { answered: BigInt(answered) } })),
            fail: (code) => act(async () => reject(new ConnectError('summary', code))),
          }),
        ),
    },
  })
  const router = createMemoryRouter(
    [
      {
        id: 'workspace',
        path: '/workspaces/:workspace',
        loader: (args) => loader(server(args)),
        action: (args) => action(server(args)),
        shouldRevalidate: () => true,
        Component: Page,
        ErrorBoundary,
      },
    ],
    { initialEntries: [entry] },
  )
  onTestFinished(() => router.dispose())
  render(<RouterProvider router={router} />)
  await screen.findByRole('region', { name: 'Activity summary' })
  // The summary is read once the page's effects run.
  await act(async () => {})
  return { reads, router, served: coralState }
}

const region = () => screen.getByRole('region', { name: 'Activity summary' })
const calls = () => within(region()).getByRole('tab', { name: /^Calls/ })
const choose = (period: string) =>
  userEvent.click(within(region()).getByRole('radio', { name: period }))
const unavailable = 'The summary is not available. Try again later.'

/** The page shows the workspace `name`, so the summary never replaced it. */
const expectPage = (name = 'Demo') =>
  expect(screen.getByRole('heading', { name, level: 1 })).toBeVisible()
/** The summary shows `answered` calls as the chosen period's. */
function expectCurrent(answered: number) {
  expect(calls().textContent).toBe(`Calls${answered}100% answered`)
  expect(region()).toHaveAttribute('aria-busy', 'false')
}
/** The summary still shows `answered` calls from before, marked as loading. */
function expectPending(answered: number) {
  expect(calls().textContent).toBe(`Calls${answered}100% answered`)
  expect(region()).toHaveAttribute('aria-busy', 'true')
}
/** The summary is unavailable: no numbers, no zeros. */
function expectUnavailable() {
  expect(within(region()).getByText(unavailable)).toBeVisible()
  expect(within(region()).queryByRole('tablist')).toBeNull()
  expect(region()).toHaveAttribute('aria-busy', 'false')
}
const visible = () => vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible')

it('renders the page before the summary arrives, then shows the summary', async () => {
  const { reads } = await renderOverview()
  expectPage()
  expect(within(region()).getByRole('status')).toHaveTextContent('Loading activity…')
  expect(region()).toHaveAttribute('aria-busy', 'true')
  expect(within(region()).queryByRole('tablist')).toBeNull()
  expect(reads.map((read) => read.days)).toEqual([7])

  await reads[0].answer(12)
  expectCurrent(12)
  expect(within(region()).queryByRole('status')).toBeNull()
})

it('reads the period the URL names', async () => {
  const { reads } = await renderOverview(`${path}?days=30`)
  expect(reads.map((read) => read.days)).toEqual([30])
  expect(within(region()).getByRole('radio', { name: '30 days' })).toBeChecked()
})

// Catches: a failed summary that replaces the page, or reads as zeros.
it.each([
  ['Coral cannot give the summary now', Code.Unavailable],
  ['the summary read fails', Code.Internal],
])('keeps the page when %s', async (_, code) => {
  const { reads } = await renderOverview()
  await reads[0].fail(code)
  expectPage()
  expectUnavailable()
})

it('keeps the page when a later read of the summary fails', async () => {
  visible()
  vi.useFakeTimers({ shouldAdvanceTime: true })
  const { reads } = await renderOverview()
  await reads[0].answer(12)
  await act(async () => vi.advanceTimersByTime(30_000))
  await reads.at(-1)!.fail(Code.Internal)
  expectPage()
  expectUnavailable()
})

it('keeps the numbers it shows, marked as loading, until the period you chose arrives', async () => {
  const { reads, router } = await renderOverview()
  await reads[0].answer(12)

  await choose('30 days')
  expect(router.state.location.search).toBe('?days=30')
  expect(within(region()).getByRole('radio', { name: '30 days' })).toBeChecked()
  expect(reads.map((read) => read.days)).toEqual([7, 30])
  expectPending(12)

  await reads[1].answer(340)
  expectCurrent(340)
})

it('never shows an earlier period’s late answer as the period you chose', async () => {
  const { reads } = await renderOverview()
  await reads[0].answer(12)
  await choose('30 days')
  await choose('24 hours')
  expect(reads.map((read) => read.days)).toEqual([7, 30, 1])

  await reads[2].answer(2)
  expectCurrent(2)
  await reads[1].answer(340)
  expectCurrent(2)
})

it('reads the page and the summary again while you look, keeping the numbers until it answers', async () => {
  visible()
  vi.useFakeTimers({ shouldAdvanceTime: true })
  const { reads } = await renderOverview()
  await reads[0].answer(12)

  await act(async () => vi.advanceTimersByTime(30_000))
  expect(reads.map((read) => read.days)).toEqual([7, 7])
  expectPending(12)

  await reads[1].answer(15)
  expectCurrent(15)
})

it('never shows a refresh’s late answer as the period you chose', async () => {
  visible()
  vi.useFakeTimers({ shouldAdvanceTime: true })
  const { reads } = await renderOverview()
  await reads[0].answer(12)
  await act(async () => vi.advanceTimersByTime(30_000))
  await choose('30 days')
  expect(reads.map((read) => read.days)).toEqual([7, 7, 30])

  await reads[1].answer(15)
  expectPending(12)
  await reads[2].answer(340)
  expectCurrent(340)
})

// Catches: a page that stops reading itself, or a saved change that waits, while the
// summary has not answered.
it('refreshes the page while the summary has not answered', async () => {
  visible()
  vi.useFakeTimers({ shouldAdvanceTime: true })
  const { router, served } = await renderOverview()
  const before = served.pageReads
  await act(async () => vi.advanceTimersByTime(30_000))
  await act(async () => {})
  expect(served.pageReads).toBeGreaterThan(before)
  expect(router.state.revalidation).toBe('idle')
  expect(within(region()).getByRole('status')).toHaveTextContent('Loading activity…')
})

it('saves a change while the summary has not answered', async () => {
  const { router } = await renderOverview()
  const formData = new FormData()
  formData.set('intent', 'rename')
  formData.set('etag', 'etag')
  formData.set('displayName', 'Renamed')
  await act(async () => void router.navigate(path, { formMethod: 'post', formData }))
  await act(async () => {})
  expect(router.state.navigation.state).toBe('idle')
  expect(router.state.actionData?.workspace).toMatchObject({ message: 'Workspace name saved.' })
  expectPage('Renamed')
})

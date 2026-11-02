/** @vitest-environment happy-dom */
import { act, cleanup, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { useEffect, useState, type ReactNode } from 'react'
import { useLoaderData, type ActionFunctionArgs, type LoaderFunctionArgs } from 'react-router'
import { describe, expect, it, vi } from 'vitest'
import type { ActionCount, ActionCounts } from '@/lib/activity-live-types'
import { ActionSignalContext, type ActionSignal } from '@/lib/use-action-signal'
import { renderLivePage, renderRoute, type RouteOptions } from '@/test-support/route'
import {
  approval,
  approvalContext,
  approvalRequest,
  invocationRow,
  run,
  workspace,
} from '../activity/fixtures'
import type {
  ApprovalReading,
  PendingAction,
  SettledApproval,
  SettledPage,
} from '../activity/types'
import { ApprovalsErrorBoundary, ApprovalsPage } from './approvals-page'

const agentAction: PendingAction = { action: approval, run }
const ownAction: PendingAction = {
  action: { ...approval, name: `${workspace}/approvals/own`, callPath: 'calendar.create' },
  run: { ...run, name: `${workspace}/runs/own`, agent: '', actorLabel: 'You', intent: '' },
}

const nextAction: PendingAction = {
  action: {
    ...approval,
    name: `${workspace}/approvals/next`,
    callPath: 'files.delete',
    deadline: '2099-01-01T00:00:00Z',
  },
  run: { ...run, name: `${workspace}/runs/next`, intent: 'Delete old files' },
}

const path = `/${workspace}`

/** The exact read of `pending`, as the loader supplies it for an open row. */
const readingOf = (
  { action, run: waiting }: PendingAction,
  overrides: Partial<ApprovalReading['detail']> = {},
): ApprovalReading => ({
  detail: {
    ...approvalRequest,
    name: action.name,
    callPath: action.callPath,
    deadline: action.deadline,
    arguments: `{"exact":"${action.callPath}"}`,
    providerLabel: 'Mail',
    ...overrides,
  },
  context: {
    ...approvalContext,
    run: waiting,
    source: `return await coral.providers.${action.callPath}({})`,
  },
  call: invocationRow(waiting.name, `call-of-${action.name.split('/').at(-1)}`, {
    agent: waiting.agent,
    callPath: action.callPath,
  }),
})
const addressOf = ({ action }: PendingAction) =>
  `/approvals?${new URLSearchParams({ request: action.name })}`
const openedRequest = (router: { state: { location: { search: string } } }) =>
  new URLSearchParams(router.state.location.search).get('request')

function renderPage(approvals: PendingAction[]) {
  return renderRoute(<ApprovalsPage approvals={approvals} workspacePath={path} />)
}

const counted = (approvals: string, connections = 'connections'): ActionCount => ({
  total: '1',
  approvals: '1',
  connections: '0',
  revisions: { approvals, connections },
})
const signalOf = (revision: string, other: ActionCounts = {}): ActionSignal => ({
  counts: { [path]: counted(revision), ...other },
  connected: true,
})

interface LoadedPage {
  approvals: PendingAction[]
  settled?: SettledPage | null
  revision?: string
  selected?: string | null
  review?: ApprovalReading | null
}
interface Coral {
  read: LoadedPage
  /** Each exact read Coral answers, by request name. */
  reviews?: Record<string, ApprovalReading>
  /** What the loader throws while Coral refuses or cannot answer. */
  failure?: unknown
}
/** Coral with the one agent request pending at `revision`. */
const pendingAt = (revision: string): Coral => ({ read: { approvals: [agentAction], revision } })

function LoadedRoute() {
  return <ApprovalsPage {...useLoaderData<LoadedPage>()} workspacePath={path} />
}

/** The route as the app shell mounts it: its rows come from a loader that reads `coral`,
 * under a signal the test moves as the stream would. */
async function renderLive(coral: Coral, first: ActionSignal, options: RouteOptions = {}) {
  const loader = vi.fn(({ request }: LoaderFunctionArgs) => {
    if (coral.failure) throw coral.failure
    const selected = new URL(request.url).searchParams.get('request')
    return { ...coral.read, selected, review: (selected && coral.reviews?.[selected]) || null }
  })
  let send!: (signal: ActionSignal) => void
  function Shell({ children }: { children: ReactNode }) {
    const [signal, setSignal] = useState(first)
    useEffect(() => {
      send = setSignal
    }, [])
    return <ActionSignalContext value={signal}>{children}</ActionSignalContext>
  }
  const { router } = renderRoute(<LoadedRoute />, {
    ...options,
    loader,
    ErrorBoundary: ApprovalsErrorBoundary,
    wrapper: Shell,
  })
  await screen.findByRole('table', { name: 'Pending approvals' })
  return { loader, router, signal: (next: ActionSignal) => act(async () => send(next)) }
}

it('asks each request as a question and names its tool, agent and deadline', () => {
  renderPage([agentAction])
  const row = within(screen.getByRole('table', { name: 'Pending approvals' })).getAllByRole(
    'row',
  )[1]
  expect(within(row).getByRole('link', { name: 'Send invoice reminders?' })).toBeInTheDocument()
  expect(within(row).getByText('mail.send')).toBeInTheDocument()
  expect(row).toHaveTextContent('Collections agent')
  expect(row).toHaveTextContent(`Deadline: ${approval.deadline}`)
})

it('shows each call by its provider logo and tool, and the whole call path otherwise', () => {
  const linear: PendingAction = {
    ...ownAction,
    action: { ...ownAction.action, callPath: 'coral.providers.linear.saveComment' },
  }
  renderRoute(
    <ApprovalsPage
      approvals={[linear, agentAction]}
      workspacePath={path}
      providers={[{ callRoot: 'coral.providers.linear', displayName: 'Linear' }]}
    />,
  )
  const [, first, second] = within(
    screen.getByRole('table', { name: 'Pending approvals' }),
  ).getAllByRole('row')
  expect(within(first).getByRole('img', { name: 'Linear' })).toBeInTheDocument()
  expect(within(first).getByText('saveComment')).toBeInTheDocument()
  expect(first).not.toHaveTextContent('coral.providers.linear')
  // No provider of the workspace owns this call, so it shows whole.
  expect(within(second).queryByRole('img')).not.toBeInTheDocument()
  expect(within(second).getByText('mail.send')).toBeInTheDocument()
})

it('counts what waits on you in the heading', () => {
  renderPage([agentAction, ownAction])
  expect(screen.getByRole('columnheader', { name: 'Waiting on you · 2' })).toBeInTheDocument()
})

it('names no agent on your own run, and asks a plain question when the run has no intent', () => {
  renderPage([ownAction])
  expect(screen.getByRole('link', { name: 'Approve this tool call?' })).toBeInTheDocument()
  expect(screen.getByRole('table', { name: 'Pending approvals' })).not.toHaveTextContent('You')
})

it('offers each decision on the row, named for the request it decides', () => {
  renderPage([agentAction])
  for (const decision of ['Approve', 'Decline'])
    expect(
      screen.getByRole('button', { name: `${decision}: Send invoice reminders?` }),
    ).toBeEnabled()
})

it('opens each row in the panel here, for your own run and an agent run alike', () => {
  renderPage([agentAction, ownAction])
  for (const [action, question] of [
    [agentAction, 'Send invoice reminders?'],
    [ownAction, 'Approve this tool call?'],
  ] as const) {
    const href = screen.getByRole('link', { name: `Review: ${question}` }).getAttribute('href')!
    const address = new URL(href, 'http://reef.test')
    expect(address.pathname).toBe('/')
    expect(address.searchParams.get('request')).toBe(action.action.name)
  }
})

it('reaches the rows with the arrow keys and opens the review with Enter', async () => {
  const { router } = renderPage([agentAction, ownAction])
  await userEvent.keyboard('{ArrowDown}{ArrowDown}')
  const second = screen.getByRole('link', { name: 'Approve this tool call?' })
  expect(second).toHaveFocus()
  await userEvent.keyboard('{Enter}')
  expect(openedRequest(router)).toBe(ownAction.action.name)
})

it('says there is nothing to approve when no request waits', () => {
  renderPage([])
  expect(screen.getByText('Nothing to approve.')).toBeInTheDocument()
  expect(screen.queryByRole('link', { name: /^Review/ })).not.toBeInTheDocument()
})

const settledRow = (
  request: string,
  outcome: SettledApproval['outcome'],
  settledAt: string,
  approverLabel = '',
): SettledApproval => ({
  name: `${workspace}/approvals/${request}`,
  outcome,
  settledAt,
  approverLabel,
  callPath: `mail.${request}`,
  providerLabel: 'Mail',
  run: { ...run, name: `${workspace}/runs/${request}`, intent: `Handle ${request}` },
})
const approvedRow = settledRow('approved', 'approved', '2026-09-22T10:00:00.000Z', 'Ada Lovelace')
const expiredRow = settledRow('expired', 'expired', '2026-09-22T09:00:00.000Z')
const historyOf = (...approvals: SettledApproval[]): SettledPage => ({ approvals, next: '' })
/** The settled rows, without the heading or the row that loads more. */
const historyRows = () =>
  within(screen.getByRole('table', { name: 'Approval history' }))
    .getAllByRole('row')
    .filter((row) => within(row).queryAllByRole('link').length > 0)

/** A row's cells, as text. */
const cells = (row: HTMLElement) =>
  within(row)
    .getAllByRole('cell')
    .map((cell) => cell.textContent)

describe('history', () => {
  it('follows the pending requests, newest first, with outcome, reviewer and time', () => {
    renderRoute(
      <ApprovalsPage
        approvals={[agentAction]}
        settled={historyOf(approvedRow, expiredRow)}
        workspacePath={path}
      />,
    )
    const [pending, history] = screen.getAllByRole('table')
    expect(pending).toHaveAccessibleName('Pending approvals')
    expect(history).toHaveAccessibleName('Approval history')
    // Each row is one line under its UTC day: time, request, tool, who asked, outcome, approver.
    expect(within(history).getByRole('rowgroup', { name: 'Tue 22 Sep 2026' })).toBeInTheDocument()
    const [approved, expired] = historyRows()
    expect(within(approved).getByRole('link', { name: 'Handle approved?' })).toBeInTheDocument()
    expect(cells(approved)).toEqual([
      '10:00:00',
      'Handle approved?',
      'mail.approved',
      'Collections agent',
      'Approved',
      'Ada Lovelace',
    ])
    // Nobody decided an expired request. Its time is its deadline.
    expect(cells(expired)).toEqual([
      '09:00:00',
      'Handle expired?',
      'mail.expired',
      'Collections agent',
      'Expired',
      '—',
    ])
    // Settled rows take no decision.
    expect(within(history).queryByRole('button', { name: /^Approve/ })).not.toBeInTheDocument()
  })

  it('says "Declined", never "Denied"', () => {
    renderRoute(
      <ApprovalsPage
        approvals={[]}
        settled={historyOf(settledRow('declined', 'declined', approvedRow.settledAt, 'You'))}
        workspacePath={path}
      />,
    )
    expect(historyRows()[0]).toHaveTextContent('Declined')
    expect(document.body.textContent).not.toMatch(/denied/i)
  })

  it('starts with "Nothing to approve." above history when nothing waits', () => {
    renderRoute(
      <ApprovalsPage approvals={[]} settled={historyOf(approvedRow)} workspacePath={path} />,
    )
    const pending = screen.getByRole('table', { name: 'Pending approvals' })
    expect(within(pending).getByText('Nothing to approve.')).toBeInTheDocument()
    expect(
      within(pending).queryByText(/Requests that need your approval appear here/),
    ).not.toBeInTheDocument()
    expect(historyRows()).toHaveLength(1)
  })

  it('explains the empty page when there are no requests at all', () => {
    renderPage([])
    expect(screen.getByText(/Requests that need your approval appear here/)).toBeInTheDocument()
    expect(screen.queryByRole('table', { name: 'Approval history' })).not.toBeInTheDocument()
  })

  it('opens a settled row in the panel, and the arrows reach it after the pending rows', async () => {
    const { router } = renderRoute(
      <ApprovalsPage
        approvals={[agentAction]}
        settled={historyOf(approvedRow)}
        workspacePath={path}
      />,
    )
    await userEvent.keyboard('{ArrowDown}{ArrowDown}')
    expect(screen.getByRole('link', { name: 'Handle approved?' })).toHaveFocus()
    await userEvent.keyboard('{Enter}')
    expect(openedRequest(router)).toBe(approvedRow.name)
  })

  // Older rows you loaded must not vanish when a decision refreshes the page.
  it('loads older rows, and a refresh updates the newest rows and keeps them', async () => {
    const older = settledRow('older', 'declined', '2026-09-21T09:00:00.000Z', 'You')
    const history = vi.fn(({ request }: LoaderFunctionArgs) => {
      expect(new URL(request.url).searchParams.get('after')).toBe('page-2')
      return historyOf(older)
    })
    const coral: Coral = {
      read: {
        approvals: [agentAction],
        settled: { approvals: [approvedRow, expiredRow], next: 'page-2' },
        revision: 'one',
      },
    }
    const { signal } = await renderLive(coral, signalOf('one'), {
      routes: [{ path: `${path}/approvals/history`, loader: history }],
    })
    await userEvent.click(screen.getByRole('button', { name: 'Load older approvals' }))
    expect(await screen.findByRole('link', { name: 'Handle older?' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Load older approvals' })).not.toBeInTheDocument()

    // The agent's request settles: it leaves the pending rows and tops the history.
    const decided = settledRow('one', 'approved', '2026-09-22T11:00:00.000Z', 'You')
    coral.read = {
      approvals: [],
      settled: { approvals: [decided, approvedRow], next: 'page-2' },
      revision: 'two',
    }
    await signal(signalOf('two'))
    await screen.findByText('Nothing to approve.')
    expect(historyRows().map((row) => within(row).getAllByRole('link')[0].textContent)).toEqual([
      'Handle one?',
      'Handle approved?',
      'Handle expired?',
      'Handle older?',
    ])
    expect(history).toHaveBeenCalled()
  })

  it('keeps the pending rows and says so when the history cannot be read', () => {
    renderRoute(<ApprovalsPage approvals={[agentAction]} settled={null} workspacePath={path} />)
    expect(screen.getByRole('link', { name: 'Send invoice reminders?' })).toBeInTheDocument()
    expect(screen.getByText('Approval history is unavailable.')).toHaveAttribute('role', 'status')
    expect(screen.queryByRole('table', { name: 'Approval history' })).not.toBeInTheDocument()
  })

  // A page loaded for one workspace must not show up in the next one's history.
  it('starts over in another workspace', async () => {
    const older = settledRow('older', 'declined', '2026-09-21T09:00:00.000Z', 'You')
    const { update } = renderLivePage(
      ({ settled, workspacePath }: { settled: SettledPage; workspacePath: string }) => (
        <ApprovalsPage approvals={[]} settled={settled} workspacePath={workspacePath} />
      ),
      { settled: { approvals: [approvedRow], next: 'page-2' }, workspacePath: path },
      { routes: [{ path: `${path}/approvals/history`, loader: () => historyOf(older) }] },
    )
    await userEvent.click(screen.getByRole('button', { name: 'Load older approvals' }))
    await screen.findByRole('link', { name: 'Handle older?' })
    await update({ settled: historyOf(expiredRow), workspacePath: '/workspaces/other' })
    expect(historyRows().map((row) => within(row).getAllByRole('link')[0].textContent)).toEqual([
      'Handle expired?',
    ])
  })

  it('keeps the rows and offers a retry when older rows cannot load', async () => {
    let answer: unknown = { unavailable: true }
    const coral: Coral = {
      read: { approvals: [], settled: { approvals: [approvedRow], next: 'page-2' } },
    }
    await renderLive(coral, signalOf('one'), {
      routes: [{ path: `${path}/approvals/history`, loader: () => answer }],
    })
    await userEvent.click(screen.getByRole('button', { name: 'Load older approvals' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('Older approvals are unavailable.')
    expect(historyRows()).toHaveLength(1)
    answer = historyOf(expiredRow)
    await userEvent.click(screen.getByRole('button', { name: 'Try again' }))
    expect(await screen.findByRole('link', { name: 'Handle expired?' })).toBeInTheDocument()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })
})

it('refreshes in place when the approval revision changes, and keeps your place', async () => {
  const coral = pendingAt('one')
  const { loader, signal } = await renderLive(coral, signalOf('one'))
  const reads = loader.mock.calls.length
  await userEvent.keyboard('{ArrowDown}')
  const row = screen.getByRole('link', { name: 'Send invoice reminders?' })
  expect(row).toHaveFocus()

  coral.read = { approvals: [agentAction, ownAction], revision: 'two' }
  await signal(signalOf('two'))

  expect(await screen.findByRole('link', { name: 'Approve this tool call?' })).toBeInTheDocument()
  expect(loader.mock.calls.length - reads).toBe(1)
  // A remount would replace the row and lose its focus.
  expect(screen.getByRole('link', { name: 'Send invoice reminders?' })).toBe(row)
  expect(row).toHaveFocus()
})

it.each([
  ['the connection revision changes', { counts: { [path]: counted('one', 'moved') } }],
  [
    'another workspace changes',
    { counts: { [path]: counted('one'), '/workspaces/other': counted('moved') } },
  ],
])('does not refresh when %s', async (_, change) => {
  const coral = pendingAt('one')
  const { loader, signal } = await renderLive(coral, signalOf('one'))
  const reads = loader.mock.calls.length
  await signal({ ...change, connected: true })
  // The real change proves that the refresh would have shown by now.
  coral.read = { approvals: [], revision: 'two' }
  await signal(signalOf('two'))
  await screen.findByText('Nothing to approve.')
  expect(loader.mock.calls.length - reads).toBe(1)
})

it('refreshes once when the stream moved before the page mounted', async () => {
  const reads = [pendingAt('old').read, { approvals: [ownAction], revision: 'new' }]
  await renderLive(
    {
      get read() {
        return reads.length > 1 ? reads.shift()! : reads[0]
      },
    },
    signalOf('new'),
  )
  expect(await screen.findByRole('link', { name: 'Approve this tool call?' })).toBeInTheDocument()
})

it('stops at one extra read when the stream lags behind the page', async () => {
  const { loader, signal } = await renderLive(pendingAt('new'), signalOf('old'))
  await waitFor(() => expect(loader).toHaveBeenCalledTimes(2))
  await signal(signalOf('new'))
  expect(loader).toHaveBeenCalledTimes(2)
})

it('removes a saving row when the decision is recorded', async () => {
  const coral = pendingAt('one')
  const { signal } = await renderLive(coral, signalOf('one'), {
    routes: [{ path: '/activity/resolve', action: () => ({ recording: true }) }],
  })
  await userEvent.click(screen.getByRole('button', { name: 'Approve: Send invoice reminders?' }))
  expect(await screen.findByText('Saving your decision…')).toBeInTheDocument()

  coral.read = { approvals: [], revision: 'two' }
  await signal(signalOf('two'))

  await waitFor(() => expect(screen.queryByText('Saving your decision…')).not.toBeInTheDocument())
  expect(screen.queryByRole('link', { name: 'Send invoice reminders?' })).not.toBeInTheDocument()
})

it.each<[string, ActionSignal]>([
  ['the stream drops', { ...signalOf('one'), connected: false }],
  ['this workspace count cannot be read', { counts: { [path]: null }, connected: true }],
  ['the stream no longer lists this workspace', { counts: {}, connected: true }],
])('says the page is unavailable and takes no decision when %s', async (_, down) => {
  const { signal } = await renderLive(pendingAt('one'), signalOf('one'))
  expect(screen.queryByText('Approvals are unavailable.')).not.toBeInTheDocument()

  await signal(down)
  expect(screen.getAllByRole('status').map((status) => status.textContent)).toContain(
    'Approvals are unavailable.',
  )
  expect(screen.getByRole('button', { name: 'Approve: Send invoice reminders?' })).toBeDisabled()

  await signal(signalOf('one'))
  expect(screen.queryByText('Approvals are unavailable.')).not.toBeInTheDocument()
  expect(screen.getByRole('button', { name: 'Approve: Send invoice reminders?' })).toBeEnabled()
})

/** The boundary rereads on the timer and when you return to the tab. */
const returnToTab = () => act(async () => window.dispatchEvent(new Event('focus')))

// The stream can recover at the same revision, so a retry that only follows the revision
// would leave the page on the error until a manual reload.
it('returns from a failed refresh once Coral answers, at the same revision', async () => {
  const coral = pendingAt('one')
  const { signal } = await renderLive(coral, signalOf('one'))

  coral.failure = new Error('Coral is down')
  await signal(signalOf('two'))
  await screen.findByText('Coral is unavailable. Reload the page to try again.')

  coral.failure = undefined
  coral.read = { approvals: [ownAction], revision: 'two' }
  await returnToTab()
  expect(await screen.findByRole('link', { name: 'Approve this tool call?' })).toBeInTheDocument()
})

it('does not retry a refusal', async () => {
  const coral = pendingAt('one')
  const { loader, signal } = await renderLive(coral, signalOf('one'))

  coral.failure = new Response('Not found', { status: 404 })
  await signal(signalOf('two'))
  await screen.findByText('This page does not exist.')
  const reads = loader.mock.calls.length
  await returnToTab()
  expect(loader.mock.calls.length - reads).toBe(0)
})

it('does not say nothing waits while it cannot tell', async () => {
  const { signal } = await renderLive({ read: { approvals: [], revision: 'one' } }, signalOf('one'))
  expect(screen.getByText('Nothing to approve.')).toBeInTheDocument()
  await signal({ ...signalOf('one'), connected: false })
  expect(screen.queryByText('Nothing to approve.')).not.toBeInTheDocument()
})

const twoPending = (): Coral => ({
  read: { approvals: [agentAction, nextAction], revision: 'one' },
  reviews: {
    [agentAction.action.name]: readingOf(agentAction),
    [nextAction.action.name]: readingOf(nextAction),
  },
})
const panel = (callPath: string) => screen.findByRole('region', { name: `Review ${callPath}` })
const commentBox = () => screen.getByRole('textbox', { name: 'Comment (optional)' })
const decisions = (answer: () => object | Promise<object>, forms: FormData[] = []) => ({
  routes: [
    {
      path: '/activity/resolve',
      action: async ({ request }: ActionFunctionArgs) => {
        forms.push(await request.formData())
        return answer()
      },
    },
  ],
})
async function addComment(text: string) {
  await userEvent.click(commentBox())
  await userEvent.paste(text)
}

describe('the review panel', () => {
  it('opens the exact arguments of a row, and a reload of its address opens them again', async () => {
    const { router } = await renderLive(twoPending(), signalOf('one'))
    expect(screen.queryByRole('region', { name: /^Review / })).not.toBeInTheDocument()

    await userEvent.click(screen.getByRole('link', { name: 'Send invoice reminders?' }))
    expect(await panel('mail.send')).toHaveTextContent('"mail.send"')
    const address = router.state.location

    router.dispose()
    cleanup()
    await renderLive(twoPending(), signalOf('one'), {
      entry: `${address.pathname}${address.search}`,
    })
    expect(await panel('mail.send')).toHaveTextContent('"mail.send"')
  })

  it('shows the source of the call beside the review, and opens the call in its activity', async () => {
    await renderLive(twoPending(), signalOf('one'), { entry: addressOf(agentAction) })
    await panel('mail.send')
    expect(screen.getByRole('region', { name: 'Accepted source' })).toHaveTextContent(
      'return await coral.providers.mail.send({})',
    )
    const href = screen.getByRole('link', { name: 'Open in activity' }).getAttribute('href')!
    const address = new URL(href, 'http://reef.test')
    // An Agent's call opens on its Agent's page, where an owner reads it.
    expect(address.pathname).toBe(`/${run.agent}`)
    expect(address.searchParams.get('record')).toBe(`${workspace}/invocations/call-of-one`)
    expect(address.searchParams.get('request')).toBe(agentAction.action.name)
  })

  // The page lists only approvals, and a pending one shows its deadline and decision.
  it('heads the review with the provider and the call, and no kind or pending status', async () => {
    await renderLive(twoPending(), signalOf('one'), { entry: addressOf(agentAction) })
    const open = await panel('mail.send')
    const heading = within(open).getByRole('heading', { name: 'Mail mail.send' })
    expect(within(heading).getByRole('img', { name: 'Mail' })).toBeInTheDocument()
    expect(open).not.toHaveTextContent('approval request')
    expect(open).not.toHaveTextContent('Approval required')
    expect(open).not.toHaveTextContent('Provider:')
  })

  it('says a request is unavailable, and keeps the list, when Coral shows no review', async () => {
    await renderLive({ read: { approvals: [agentAction] } }, signalOf('one'), {
      entry: addressOf(nextAction),
    })
    expect(
      await within(await screen.findByRole('dialog', { name: 'Review approval' })).findByRole(
        'status',
      ),
    ).toHaveTextContent('This request is unavailable.')
    expect(screen.getByRole('link', { name: 'Send invoice reminders?' })).toBeInTheDocument()
  })

  it('closes back to the list', async () => {
    const { router } = await renderLive(twoPending(), signalOf('one'), {
      entry: addressOf(agentAction),
    })
    await panel('mail.send')
    await userEvent.click(screen.getByRole('button', { name: 'Close review' }))
    await waitFor(() => expect(openedRequest(router)).toBeNull())
    await waitFor(() =>
      expect(screen.queryByRole('region', { name: /^Review / })).not.toBeInTheDocument(),
    )
  })

  // Catches: a move that starts before the blocker lets go of the sent comment, which asks you
  // to discard a comment you already sent.
  it('moves to the next request after your decision, without asking about the sent comment', async () => {
    const coral = twoPending()
    const { router } = await renderLive(coral, signalOf('one'), {
      entry: addressOf(agentAction),
      ...decisions(() => ({ state: 'approved' })),
    })
    await panel('mail.send')
    await addComment('Looks right')
    coral.read = { approvals: [nextAction], revision: 'two' }
    await userEvent.click(screen.getByRole('button', { name: 'Approve' }))

    const next = await panel('files.delete')
    expect(next).toHaveTextContent('"files.delete"')
    expect(openedRequest(router)).toBe(nextAction.action.name)
    // The decided request's controls held focus. It stays in the panel for the next decision.
    await waitFor(() => expect(document.activeElement).not.toBe(document.body))
    expect(screen.queryByRole('dialog', { name: 'Discard your comment?' })).not.toBeInTheDocument()
  })

  it.each<[string, PendingAction[], PendingAction[], string, object, string | null]>([
    [
      'back to an earlier request after the last one',
      [agentAction, nextAction],
      [agentAction],
      'Decline',
      { state: 'declined' },
      agentAction.action.name,
    ],
    [
      'out of the panel when nothing else waits',
      [nextAction],
      [],
      'Approve',
      { state: 'approved' },
      null,
    ],
  ])('moves %s', async (_, before, after, control, answer, expected) => {
    const coral: Coral = { ...twoPending(), read: { approvals: before, revision: 'one' } }
    const opened = before.at(-1)!
    const { router } = await renderLive(coral, signalOf('one'), {
      entry: addressOf(opened),
      ...decisions(() => answer),
    })
    await panel(opened.action.callPath)
    coral.read = { approvals: after, revision: 'two' }
    await userEvent.click(screen.getByRole('button', { name: control }))
    await waitFor(() => expect(openedRequest(router)).toBe(expected))
  })

  it('leaves the open request to its review, which keeps your comment', async () => {
    await renderLive(twoPending(), signalOf('one'), { entry: addressOf(agentAction) })
    await panel('mail.send')
    expect(
      screen.queryByRole('button', { name: 'Approve: Send invoice reminders?' }),
    ).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Approve: Delete old files?' })).toBeEnabled()
  })

  it('waits for the read when Coral only holds your decision, then moves on', async () => {
    const coral = twoPending()
    const { signal } = await renderLive(coral, signalOf('one'), {
      entry: addressOf(agentAction),
      ...decisions(() => ({ recording: true })),
    })
    const open = await panel('mail.send')
    await userEvent.click(screen.getByRole('button', { name: 'Approve' }))
    expect(await screen.findByText('Saving your decision…')).toBeInTheDocument()
    expect(open).toBeInTheDocument()

    coral.read = { approvals: [nextAction], revision: 'two' }
    coral.reviews![agentAction.action.name] = readingOf(agentAction, {
      state: 'approved',
      actionable: false,
    })
    await signal(signalOf('two'))
    await panel('files.delete')
  })

  it('stays and shows the winner when another decision beats one Coral only held', async () => {
    const coral = twoPending()
    const { router, signal } = await renderLive(coral, signalOf('one'), {
      entry: addressOf(agentAction),
      ...decisions(() => ({ recording: true })),
    })
    await panel('mail.send')
    await userEvent.click(screen.getByRole('button', { name: 'Approve' }))
    await screen.findByText('Saving your decision…')

    coral.read = { approvals: [nextAction], revision: 'two' }
    coral.reviews![agentAction.action.name] = readingOf(agentAction, {
      state: 'declined',
      actionable: false,
    })
    await signal(signalOf('two'))
    expect(await within(await panel('mail.send')).findByText('Declined')).toBeInTheDocument()
    expect(openedRequest(router)).toBe(agentAction.action.name)
  })

  // Catches: a remembered decision that sends you away again whenever you reopen the request.
  it('stays on a request you decided when you open it again', async () => {
    const coral = twoPending()
    const { router } = await renderLive(coral, signalOf('one'), {
      entry: addressOf(agentAction),
      ...decisions(() => ({ state: 'approved' })),
    })
    await panel('mail.send')
    coral.reviews![agentAction.action.name] = readingOf(agentAction, {
      state: 'approved',
      actionable: false,
    })
    await userEvent.click(screen.getByRole('button', { name: 'Approve' }))
    await panel('files.delete')

    await act(async () => {
      await router.navigate(addressOf(agentAction))
    })
    await panel('mail.send')
    await act(async () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve())))
    expect(openedRequest(router)).toBe(agentAction.action.name)
  })

  it('stays on the open request after a decision on another row', async () => {
    const coral = twoPending()
    const { router } = await renderLive(coral, signalOf('one'), {
      entry: addressOf(agentAction),
      ...decisions(() => ({ state: 'approved' })),
    })
    await panel('mail.send')
    coral.read = { approvals: [agentAction], revision: 'two' }
    await userEvent.click(screen.getByRole('button', { name: 'Approve: Delete old files?' }))
    await waitFor(() =>
      expect(screen.queryByRole('link', { name: 'Delete old files?' })).not.toBeInTheDocument(),
    )
    expect(openedRequest(router)).toBe(agentAction.action.name)
  })

  it('stays and shows the winner when another decision beats yours', async () => {
    const coral = twoPending()
    const { router } = await renderLive(coral, signalOf('one'), {
      entry: addressOf(agentAction),
      ...decisions(() => ({ state: 'declined' })),
    })
    await panel('mail.send')
    await userEvent.click(screen.getByRole('button', { name: 'Approve' }))
    expect(await screen.findByText('Declined')).toBeInTheDocument()
    expect(openedRequest(router)).toBe(agentAction.action.name)
  })

  it.each([
    ['approves', 'approved', 'Approved'],
    ['declines', 'declined', 'Declined'],
    ['lets it expire', 'expired', 'Expired'],
  ])('stays and shows the settled state when someone else %s it', async (_, state, label) => {
    const coral = twoPending()
    const { router, signal } = await renderLive(coral, signalOf('one'), {
      entry: addressOf(agentAction),
    })
    await panel('mail.send')

    coral.read = { approvals: [nextAction], revision: 'two' }
    coral.reviews![agentAction.action.name] = readingOf(agentAction, {
      state,
      actionable: false,
    })
    await signal(signalOf('two'))

    const open = await panel('mail.send')
    expect(await within(open).findByText(label)).toBeInTheDocument()
    expect(openedRequest(router)).toBe(agentAction.action.name)
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
  })

  // Catches: a refresh that remounts the panel, which loses the comment and the panel's focus.
  it('keeps your typing, focus and open panel across a refresh', async () => {
    const coral = twoPending()
    const { router, signal } = await renderLive(coral, signalOf('one'), {
      entry: addressOf(agentAction),
    })
    await panel('mail.send')
    await addComment('Half a thought')
    const box = commentBox()

    coral.read = { approvals: [agentAction, nextAction, ownAction], revision: 'two' }
    await signal(signalOf('two'))

    await screen.findByRole('link', { name: 'Approve this tool call?' })
    expect(commentBox()).toBe(box)
    expect(box).toHaveValue('Half a thought')
    expect(box).toHaveFocus()
    expect(openedRequest(router)).toBe(agentAction.action.name)
    expect(screen.queryByRole('dialog', { name: 'Discard your comment?' })).not.toBeInTheDocument()
  })

  it.each([
    ['closing the panel', () => screen.getByRole('button', { name: 'Close review' })],
    ['opening another row', () => screen.getByRole('link', { name: 'Delete old files?' })],
  ])('asks before %s drops an unsent comment', async (_, control) => {
    const { router } = await renderLive(twoPending(), signalOf('one'), {
      entry: addressOf(agentAction),
    })
    await panel('mail.send')
    await addComment('Half a thought')
    await userEvent.click(control())
    expect(await screen.findByRole('dialog', { name: 'Discard your comment?' })).toBeInTheDocument()
    await userEvent.click(screen.getByRole('button', { name: 'Keep editing' }))
    expect(commentBox()).toHaveValue('Half a thought')
    expect(openedRequest(router)).toBe(agentAction.action.name)
  })

  it('takes no decision while the page is unavailable', async () => {
    const { signal } = await renderLive(twoPending(), signalOf('one'), {
      entry: addressOf(agentAction),
    })
    await panel('mail.send')
    await signal({ ...signalOf('one'), connected: false })
    expect(screen.getByRole('button', { name: 'Approve' })).toBeDisabled()
  })
})

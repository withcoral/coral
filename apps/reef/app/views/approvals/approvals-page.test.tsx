/** @vitest-environment happy-dom */
import { act, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { useEffect, useState, type ReactNode } from 'react'
import { useLoaderData } from 'react-router'
import { expect, it, vi } from 'vitest'
import type { ActionCount, ActionCounts } from '@/lib/activity-live-types'
import { ActionSignalContext, type ActionSignal } from '@/lib/use-action-count'
import { renderRoute, type RouteOptions } from '@/test-support/route'
import { approval, run, workspace } from '../activity/fixtures'
import type { PendingAction } from '../activity/types'
import { ApprovalsErrorBoundary, ApprovalsPage } from './approvals-page'

const agentAction: PendingAction = { action: approval, run }
const ownAction: PendingAction = {
  action: { ...approval, name: `${workspace}/approvals/own`, callPath: 'calendar.create' },
  run: { ...run, name: `${workspace}/runs/own`, agent: '', actorLabel: 'You', intent: '' },
}

const path = `/${workspace}`

function renderPage(approvals: PendingAction[]) {
  return renderRoute(<ApprovalsPage approvals={approvals} workspace={path} />)
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
  revision?: string
}
interface Coral {
  read: LoadedPage
  down?: boolean
}

function LoadedRoute() {
  const { approvals, revision } = useLoaderData<LoadedPage>()
  return <ApprovalsPage approvals={approvals} workspace={path} revision={revision} />
}

/** The route as the app shell mounts it: its rows come from a loader that reads `coral`,
 * under a signal the test moves as the stream would. */
async function renderLive(coral: Coral, first: ActionSignal, options: RouteOptions = {}) {
  const loader = vi.fn(() => {
    if (coral.down) throw new Error('Coral is down')
    return coral.read
  })
  let send!: (signal: ActionSignal) => void
  function Shell({ children }: { children: ReactNode }) {
    const [signal, setSignal] = useState(first)
    useEffect(() => {
      send = setSignal
    }, [])
    return <ActionSignalContext value={signal}>{children}</ActionSignalContext>
  }
  renderRoute(<LoadedRoute />, {
    ...options,
    loader,
    ErrorBoundary: () => <ApprovalsErrorBoundary workspace={path} />,
    wrapper: Shell,
  })
  await screen.findByRole('table', { name: 'Pending approvals' })
  return { loader, signal: (next: ActionSignal) => act(async () => send(next)) }
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

it.each([
  ['an agent run on its agent page', agentAction, 'Send invoice reminders?', `/${run.agent}?`],
  ['your own run in activity', ownAction, 'Approve this tool call?', `/${workspace}/activity?`],
])('reviews %s', (_, action, question, page) => {
  renderPage([action])
  const review = screen.getByRole('link', { name: `Review: ${question}` })
  expect(review.getAttribute('href')).toContain(page)
  expect(
    new URL(review.getAttribute('href')!, 'http://reef.test').searchParams.get('request'),
  ).toBe(action.action.name)
})

it('reaches the rows with the arrow keys and opens the review with Enter', async () => {
  const { router } = renderPage([agentAction, ownAction])
  await userEvent.keyboard('{ArrowDown}{ArrowDown}')
  const second = screen.getByRole('link', { name: 'Approve this tool call?' })
  expect(second).toHaveFocus()
  await userEvent.keyboard('{Enter}')
  expect(router.state.location.pathname).toBe(`/${workspace}/activity`)
  expect(router.state.location.search).toContain('request=')
})

it('says there is nothing to approve when no request waits', () => {
  renderPage([])
  expect(screen.getByText('Nothing to approve.')).toBeInTheDocument()
  expect(screen.queryByRole('link', { name: /^Review/ })).not.toBeInTheDocument()
})

it('refreshes in place when the approval revision changes, and keeps your place', async () => {
  const coral = { read: { approvals: [agentAction], revision: 'one' } as LoadedPage }
  const { loader, signal } = await renderLive(coral, signalOf('one'))
  expect(loader).toHaveBeenCalledTimes(1)
  await userEvent.keyboard('{ArrowDown}')
  const row = screen.getByRole('link', { name: 'Send invoice reminders?' })
  expect(row).toHaveFocus()

  coral.read = { approvals: [agentAction, ownAction], revision: 'two' }
  await signal(signalOf('two'))

  expect(await screen.findByRole('link', { name: 'Approve this tool call?' })).toBeInTheDocument()
  expect(loader).toHaveBeenCalledTimes(2)
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
  ['the stream reports no count for this workspace', { counts: {} }],
])('does not refresh when %s', async (_, change) => {
  const coral = { read: { approvals: [agentAction], revision: 'one' } as LoadedPage }
  const { loader, signal } = await renderLive(coral, signalOf('one'))
  await signal({ ...change, connected: true })
  // The real change proves that the refresh would have shown by now.
  coral.read = { approvals: [], revision: 'two' }
  await signal(signalOf('two'))
  await screen.findByText('Nothing to approve.')
  expect(loader).toHaveBeenCalledTimes(2)
})

it('refreshes once when the stream moved before the page mounted', async () => {
  const coral = { read: { approvals: [agentAction], revision: 'old' } as LoadedPage }
  const { loader } = await renderLive(coral, signalOf('new'))
  await waitFor(() => expect(loader).toHaveBeenCalledTimes(2))
})

it('stops at one extra read when the stream lags behind the page', async () => {
  const coral = { read: { approvals: [agentAction], revision: 'new' } as LoadedPage }
  const { loader, signal } = await renderLive(coral, signalOf('old'))
  await waitFor(() => expect(loader).toHaveBeenCalledTimes(2))
  await signal(signalOf('new'))
  expect(loader).toHaveBeenCalledTimes(2)
})

it('removes a saving row when the decision is recorded', async () => {
  const coral = { read: { approvals: [agentAction], revision: 'one' } as LoadedPage }
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
])('says the page is unavailable and takes no decision when %s', async (_, down) => {
  const coral = { read: { approvals: [agentAction], revision: 'one' } as LoadedPage }
  const { signal } = await renderLive(coral, signalOf('one'))
  expect(screen.queryByText('Approvals are unavailable.')).not.toBeInTheDocument()

  await signal(down)
  expect(screen.getByText('Approvals are unavailable.')).toHaveAttribute('role', 'status')
  expect(screen.getByRole('button', { name: 'Approve: Send invoice reminders?' })).toBeDisabled()

  await signal(signalOf('one'))
  expect(screen.queryByText('Approvals are unavailable.')).not.toBeInTheDocument()
  expect(screen.getByRole('button', { name: 'Approve: Send invoice reminders?' })).toBeEnabled()
})

// The boundary replaces the page, so without its own subscription the page would stay on
// the error until a manual reload.
it('returns from a failed refresh on the next revision, with one retry between', async () => {
  const coral: Coral = { read: { approvals: [agentAction], revision: 'one' } }
  const { loader, signal } = await renderLive(coral, signalOf('one'))

  coral.down = true
  await signal(signalOf('two'))
  await screen.findByText('Coral is unavailable. Reload the page to try again.')
  await waitFor(() => expect(loader).toHaveBeenCalledTimes(3))

  coral.down = false
  coral.read = { approvals: [ownAction], revision: 'three' }
  await signal(signalOf('three'))
  expect(await screen.findByRole('link', { name: 'Approve this tool call?' })).toBeInTheDocument()
  expect(loader).toHaveBeenCalledTimes(4)
})

it('does not say nothing waits while it cannot tell', async () => {
  const coral = { read: { approvals: [], revision: 'one' } as LoadedPage }
  const { signal } = await renderLive(coral, signalOf('one'))
  expect(screen.getByText('Nothing to approve.')).toBeInTheDocument()
  await signal({ ...signalOf('one'), connected: false })
  expect(screen.queryByText('Nothing to approve.')).not.toBeInTheDocument()
})

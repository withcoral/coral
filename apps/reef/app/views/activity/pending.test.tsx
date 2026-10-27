/** @vitest-environment happy-dom */
import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it } from 'vitest'
import { markupRoute, renderLivePage, renderRoute } from '@/test-support/route'
import { Activity } from './activity'
import { ActivityArchivedContext } from './live'
import { ActionReviewSession } from '../actions/review'
import { approval, pendingPage, run } from './fixtures'
import type { ActivityPage } from './types'

function markup(value: ActivityPage, path = '/activity') {
  return markupRoute(
    <ActionReviewSession>
      <Activity page={value} />
    </ActionReviewSession>,
    path,
  )
}
/** Renders the page and returns the pending list. */
function renderPending(value: ActivityPage, entry = '/activity') {
  const view = renderRoute(
    <ActionReviewSession>
      <Activity page={value} />
    </ActionReviewSession>,
    { entry },
  )
  return {
    ...view,
    pending: screen.getByRole('region', { name: 'Needs your action' }),
  }
}
it('asks each pending request as its intent, above personal history, without its arguments', () => {
  const { container, pending } = renderPending(pendingPage)
  expect(within(pending).getByText('Send invoice reminders?')).toBeInTheDocument()
  // The row names the operation and the agent, not the arguments.
  expect(within(pending).getByText('mail.send')).toBeInTheDocument()
  // The page names the Workspace, so a row names only the Agent.
  const line = within(pending).getByText(/Collections agent ·/)
  expect(line).not.toHaveTextContent('Finance')
  expect(within(pending).getByRole('link', { name: 'Review' })).toHaveAttribute(
    'href',
    expect.stringContaining('request=workspaces%2Ffinance%2Fapprovals%2Fone'),
  )
  // Pending work comes before your own history.
  const history = screen.getByRole('heading', { name: 'Your recent activity' })
  expect(pending.compareDocumentPosition(history) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
  expect(container).not.toHaveTextContent('exact-recipient')
  expect(container).not.toHaveTextContent('Cancel run')
})
it('decides an approval from its row, bound to that exact request', () => {
  const html = markup(pendingPage)
  expect(html).toContain('action="/activity/resolve"')
  expect(html).toContain('name="name" value="workspaces/finance/approvals/one"')
  expect(html).toContain('aria-label="Approve: Send invoice reminders?"')
  expect(html).toContain('aria-label="Deny: Send invoice reminders?"')
})
// Catches: an archived workspace offering decisions that Coral refuses.
it('an archived workspace keeps the review link but drops the row decisions', () => {
  renderRoute(
    <ActivityArchivedContext value="owner">
      <ActionReviewSession>
        <Activity page={pendingPage} />
      </ActionReviewSession>
    </ActivityArchivedContext>,
    { entry: '/activity' },
  )
  const pending = screen.getByRole('region', { name: 'Needs your action' })
  expect(within(pending).getByRole('link', { name: 'Review' })).toBeInTheDocument()
  expect(within(pending).queryByRole('button', { name: /^(Approve|Deny):/ })).toBeNull()
})
it('lists parallel calls to one Operation as separate rows with separate decisions', () => {
  const second = { ...approval, name: 'workspaces/finance/approvals/two' }
  const html = markup({
    ...pendingPage,
    pending: {
      actions: [
        { action: approval, run },
        { action: second, run },
      ],
      actionCount: '2',
      next: '',
      unavailable: false,
    },
  })
  expect(html).toContain('name="name" value="workspaces/finance/approvals/one"')
  expect(html).toContain('name="name" value="workspaces/finance/approvals/two"')
  expect(html).not.toContain('2 approvals')
})
it('words each question like the MCP prompt', () => {
  const connection = {
    kind: 'account_connection' as const,
    name: 'workspaces/finance/accountConnections/one',
    callPath: '',
    providerLabel: 'Linear',
    deadline: approval.deadline,
  }
  const html = markup({
    ...pendingPage,
    pending: {
      actions: [
        { action: approval, run: { ...run, intent: 'Check my invoices.' } },
        {
          action: { ...approval, name: 'workspaces/finance/approvals/two' },
          run: { ...run, intent: '' },
        },
        { action: connection, run: { ...run, agent: '' } },
      ],
      actionCount: '3',
      next: '',
      unavailable: false,
    },
  })
  expect(html).toContain('Check my invoices?')
  expect(html).not.toContain('invoices.?')
  expect(html).toContain('Approve this operation?')
  expect(html).toContain('Connect your Linear account to continue.')
  expect(html).toContain('Connect account')
  expect(html).not.toContain('name="name" value="workspaces/finance/accountConnections/one"')
})
it('hides known empty pending work but distinguishes unavailable data and off-page actions', () => {
  expect(
    markup({
      ...pendingPage,
      pending: { actions: [], actionCount: '0', next: '', unavailable: false },
    }),
  ).not.toContain('Needs your action')
  expect(
    markup({
      ...pendingPage,
      pending: { actions: [], actionCount: '0', next: '', unavailable: true },
    }),
  ).toContain('Pending actions are unavailable.')
  const html = markup({
    ...pendingPage,
    pending: {
      actions: [],
      actionCount: '3',
      next: 'cursor',
      unavailable: false,
    },
  })
  expect(html).toContain('No actions remain on this page')
  expect(html).toContain('pendingAfter=cursor')
  expect(html).toContain('More actions')
})
// Catches: a row decision that Coral holds offered again, or its status outliving the row.
it('says a row decision is saving until the live read removes the row', async () => {
  const { update } = renderLivePage(
    (page) => (
      <ActionReviewSession>
        <Activity page={page} />
      </ActionReviewSession>
    ),
    pendingPage,
    {
      entry: '/activity',
      routes: [{ path: '/activity/resolve', action: () => ({ recording: true }) }],
    },
  )
  const pending = screen.getByRole('region', { name: 'Needs your action' })
  await userEvent.click(
    within(pending).getByRole('button', { name: 'Approve: Send invoice reminders?' }),
  )

  expect(await within(pending).findByText('Saving your decision…')).toHaveAttribute(
    'role',
    'status',
  )
  expect(
    within(pending).getByRole('button', { name: 'Deny: Send invoice reminders?' }),
  ).toBeDisabled()

  update({
    ...pendingPage,
    pending: { actions: [], actionCount: '0', next: '', unavailable: false },
  })
  expect(screen.queryByText('Saving your decision…')).toBeNull()
})

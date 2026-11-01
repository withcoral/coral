/** @vitest-environment happy-dom */
import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it } from 'vitest'
import { renderRoute } from '@/test-support/route'
import { approval, run, workspace } from '../activity/fixtures'
import type { PendingAction } from '../activity/types'
import { ApprovalsPage } from './approvals-page'

const agentAction: PendingAction = { action: approval, run }
const ownAction: PendingAction = {
  action: { ...approval, name: `${workspace}/approvals/own`, callPath: 'calendar.create' },
  run: { ...run, name: `${workspace}/runs/own`, agent: '', actorLabel: 'You', intent: '' },
}

function renderPage(approvals: PendingAction[]) {
  return renderRoute(<ApprovalsPage approvals={approvals} />)
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
])('reviews %s', (_, action, question, path) => {
  renderPage([action])
  const review = screen.getByRole('link', { name: `Review: ${question}` })
  expect(review.getAttribute('href')).toContain(path)
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

it('says nothing needs a decision when no request waits', () => {
  renderPage([])
  expect(screen.getByText('Nothing needs your decision.')).toBeInTheDocument()
  expect(screen.queryByRole('link', { name: /^Review/ })).not.toBeInTheDocument()
})

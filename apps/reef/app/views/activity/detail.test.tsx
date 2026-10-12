/** @vitest-environment happy-dom */
import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import { markupRoute, renderRoute } from '@/test-support/route'
import { ActivityDetail } from './detail'
import { ActionReviewSession } from '../actions/review'
import { approvalRequest, pendingPage, run, workspace } from './fixtures'
import { ActivityConnectedContext } from './live'
import type { ActivityPage, InvocationRow, RequestDetail } from './types'

function markup(value: ActivityPage) {
  return markupRoute(
    <ActionReviewSession>
      <ActivityDetail page={value} />
    </ActionReviewSession>,
    '/activity',
  )
}
it('keeps exact evidence and a receipt after the selected request leaves pending work', () => {
  const selected = {
    ...run,
    actionPage: { total: '0', actions: [], next: '' },
    requiresAction: false,
  }
  const html = markup({
    ...pendingPage,
    selected: run.name,
    selectedRow: selected,
    pending: { actions: [], actionCount: '0', next: '', unavailable: false },
    detail: {
      kind: 'run',
      run: selected,
      request: {
        ...approvalRequest,
        actionable: false,
        state: 'approved',
        approver: 'reviewer',
        approverLabel: 'You',
      },
      source: 'return null;',
      linkedFunctions: [],
      terminal: { kind: 'unavailable' },
    },
  })
  expect(html).toContain('exact-recipient')
  expect(html).toContain('Approved')
  expect(html).not.toContain('Approve this call')
  expect(html).toContain('Run state: running')
})
function runDetail(actions: typeof run.actionPage, request: typeof approvalRequest) {
  return markup({
    ...pendingPage,
    selected: run.name,
    selectedRow: { ...run, actionPage: actions },
    detail: {
      kind: 'run',
      run: { ...run, actionPage: actions },
      request,
      source: 'return null;',
      linkedFunctions: [],
      terminal: { kind: 'unavailable' },
    },
  })
}

it('offers what the Run waits on now beside a settled selection, but not beside a current one', () => {
  const next = {
    ...run.actionPage!.actions[0],
    name: 'workspaces/finance/approvals/two',
  }
  const settled = runDetail(
    { total: '1', actions: [next], next: '' },
    {
      ...approvalRequest,
      actionable: false,
      state: 'approved',
      approver: 'reviewer',
    },
  )
  expect(settled).toContain('Approved')
  expect(settled).toContain('Review mail.send')
  expect(settled).toContain('request=workspaces%2Ffinance%2Fapprovals%2Ftwo')
  const current = runDetail(
    { total: '2', actions: [run.actionPage!.actions[0], next], next: '' },
    approvalRequest,
  )
  expect(current).toContain('Approve this call')
  expect(current).not.toContain('request=workspaces%2Ffinance%2Fapprovals%2Ftwo')
})
it('shows purpose and caller once, and keeps Run diagnostics behind one technical disclosure', () => {
  const selected = { ...run, actorLabel: 'You', agent: '' }
  const html = markup({
    ...pendingPage,
    selected: run.name,
    selectedRow: selected,
    detail: {
      kind: 'run',
      run: selected,
      request: approvalRequest,
      source: 'exact-program-source',
      linkedFunctions: [],
      terminal: { kind: 'unavailable' },
    },
  })
  const container = document.createElement('div')
  container.innerHTML = html
  expect(html.match(/Send invoice reminders/g)).toHaveLength(1)
  // The caller is named once, beside the workspace.
  expect(within(container).getByText(/\bYou\b/)).toHaveTextContent('You · Finance')
  expect(html).not.toContain('Requested purpose:')
  expect(html).not.toContain('1 action needs you.')
  expect(html).not.toContain('Approval needed')
  const technical = [...container.querySelectorAll('details')].find(
    (element) => element.querySelector('summary')?.textContent === 'Technical details',
  )!
  expect(technical.hasAttribute('open')).toBe(false)
  expect(technical.textContent).toContain(run.name)
  expect(technical.textContent).toContain('exact-program-source')
  expect(technical.textContent).toContain('Cancel run')
  expect(html.indexOf('Approve this call')).toBeLessThan(html.indexOf('Technical details'))
  const history = [...container.querySelectorAll('a')].find(
    (link) => link.textContent === 'View in history',
  )!
  const url = new URL(history.getAttribute('href')!, 'http://reef')
  expect(url.pathname).toBe('/workspaces/finance/activity')
  expect(url.searchParams.get('record')).toBeNull()
  expect(url.searchParams.get('anchor')).toBe(run.name)
})
it.each([true, false])(
  'exposes approval controls only after exact review when current availability is %s',
  (actionable) => {
    const call: InvocationRow = {
      ...run,
      name: 'workspaces/finance/invocations/one',
      run: run.name,
      state: 'pending',
      outcome: 'unknown',
      dispatchedAt: null,
      waitingNanos: '0',
      provider: 'workspaces/finance/providers/mail',
      operation: 'send',
      callPath: 'coral.providers.mail.send',
    }
    const html = markup({
      ...pendingPage,
      selected: call.name,
      selectedRow: { ...call, kind: 'invocation' },
      detail: {
        kind: 'invocation',
        context: {
          kind: 'run',
          run,
          source: 'return null;',
          linkedFunctions: [],
          terminal: { kind: 'unavailable' },
        },
        invocation: call,
        arguments: '{"logical":"exact-native"}',
        result: { kind: 'unknown' },
        requests: [],
        requestUnavailable: false,
        request: {
          ...approvalRequest,
          invocation: call.name,
          callPath: call.callPath,
          arguments: '{"logical":"review-native"}',
          actionable,
        },
      },
    })
    expect(html).toContain('review-native')
    expect(html).not.toContain('exact-native')
    expect(html).toContain('Arguments to review')
    expect(html).not.toContain('Retained request detail')
    expect(html.indexOf(call.callPath)).toBeLessThan(html.indexOf('review-native'))
    expect(html.includes('Approve this call')).toBe(actionable)
    expect(html.includes('Deny this call')).toBe(actionable)
    if (actionable)
      expect(html.indexOf('review-native')).toBeLessThan(html.indexOf('Approve this call'))
    else expect(html).toContain('Approval unavailable')
  },
)

const runActions = () => screen.getByRole('region', { name: 'Run actions' })

describe('an account connection the Run waits on', () => {
  const connection: Extract<RequestDetail, { kind: 'account_connection' }> = {
    kind: 'account_connection',
    name: `${workspace}/accountConnections/one`,
    provider: `${workspace}/providers/linear`,
    state: 'pending',
    expiresAt: '2030-09-22T09:15:00Z',
  }
  const waiting = {
    ...run,
    actionPage: {
      total: '1',
      next: '',
      actions: [
        {
          kind: 'account_connection' as const,
          name: connection.name,
          callPath: '',
          providerLabel: 'Linear',
          deadline: connection.expiresAt!,
        },
      ],
    },
  }

  function renderConnection(
    request: typeof connection,
    { connected = true, decline }: { connected?: boolean; decline?: (form: FormData) => void } = {},
  ) {
    const settled = request.state !== 'pending'
    const selected = settled
      ? { ...waiting, actionPage: { total: '0', next: '', actions: [] } }
      : waiting
    return renderRoute(
      <ActivityConnectedContext value={connected}>
        <ActionReviewSession>
          <ActivityDetail
            page={{
              ...pendingPage,
              selected: selected.name,
              selectedRow: selected,
              detail: {
                kind: 'run',
                run: selected,
                request,
                source: 'return null;',
                linkedFunctions: [],
                terminal: { kind: 'unavailable' },
              },
            }}
          />
        </ActionReviewSession>
      </ActivityConnectedContext>,
      {
        entry: '/activity',
        csrf: 'test-csrf',
        routes: [
          {
            path: '/activity/resolve',
            action: async ({ request: submitted }) => {
              decline?.(await submitted.formData())
              // The decline stays in flight.
              return new Promise(() => {})
            },
          },
        ],
      },
    )
  }

  it.each([
    ['connected', 'Account connected.'],
    ['declined', 'Connection declined.'],
  ] as const)('states a %s connection and offers nothing more to do', (state, outcome) => {
    renderConnection({ ...connection, state })
    expect(within(runActions()).getByRole('status')).toHaveTextContent(outcome)
    expect(within(runActions()).queryByRole('button')).not.toBeInTheDocument()
  })

  it('holds Connect and Decline while the live view reconnects', () => {
    renderConnection(connection, { connected: false })
    expect(screen.getByRole('button', { name: 'Connect Linear' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Decline' })).toBeDisabled()
  })

  it('declines this exact connection once, and holds Decline while it is in flight', async () => {
    const declines: FormData[] = []
    renderConnection(connection, { decline: (form) => declines.push(form) })
    await userEvent.click(screen.getByRole('button', { name: 'Decline' }))
    expect(declines.map((form) => Object.fromEntries(form))).toEqual([
      {
        csrf: 'test-csrf',
        name: connection.name,
        intent: 'decline_connection',
      },
    ])
    expect(screen.getByRole('button', { name: 'Decline' })).toBeDisabled()
    await userEvent.click(screen.getByRole('button', { name: 'Decline' }))
    expect(declines).toHaveLength(1)
  })
})

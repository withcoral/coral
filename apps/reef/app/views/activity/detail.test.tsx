/** @vitest-environment happy-dom */
import { act, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { renderToStaticMarkup } from 'react-dom/server'
import { markupRoute, renderLivePage, renderRoute } from '@/test-support/route'
import { ActivityDetail } from './detail'
import { ActionReviewSession } from '../actions/review'
import { approvalRequest, pendingPage, run, workspace } from './fixtures'
import { stored } from './discovery/fixtures'
import { discoveryView } from './discovery/response'
import { ActivityConnectedContext } from './live'
import { ContextImpact } from './context-impact'
import type {
  ActionPage,
  ActivityPage,
  ActivityRow,
  DiscoveryContext,
  InteractionEvidence,
  InteractionRow,
  InvocationDetail,
  InvocationRow,
  RequestDetail,
  RunDetail,
  RunEvent,
} from './types'

/** A time reads in UTC; its element keeps the exact retained instant. */
function expectInstant(value: Element | null, instant: string, text: string) {
  expect(value).toHaveTextContent(text)
  expect(value).not.toHaveTextContent(instant)
  expect(value!.querySelector('time')).toHaveAttribute('dateTime', instant)
}
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
  // The review's own decision leads; no badge repeats the Run's state under it.
  expect(html).not.toContain('Run state')
  expect(html).not.toContain('program run:')
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
it('shows purpose and caller once, with nothing but the call to decide on', () => {
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
  // The approval review names the caller once, beside the workspace, in place of a Run heading.
  expect(within(container).getByText(/\bYou\b/)).toHaveTextContent('You · Finance')
  expect(within(container).queryByRole('heading', { level: 2 })).not.toBeInTheDocument()
  expect(html).not.toContain('Requested purpose:')
  expect(html).not.toContain('1 action needs you.')
  expect(html).not.toContain('Approval needed')
  // No technical details, no raw request ID, and no program body, history link or Run cancel.
  expect(html).not.toContain('Technical details')
  // The decision form still posts the exact request; it just is not shown.
  expect(container.textContent).not.toContain(approvalRequest.name)
  for (const gone of ['View in history', 'Cancel run', 'Program body', 'Accepted program source'])
    expect(container.textContent).not.toContain(gone)
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
    expect(html).toContain('Arguments (1)')
    expect(html).not.toContain('Retained request detail')
    expect(html.indexOf(call.callPath)).toBeLessThan(html.indexOf('review-native'))
    expect(html.includes('Approve this call')).toBe(actionable)
    expect(html.includes('Deny this call')).toBe(actionable)
    if (actionable)
      expect(html.indexOf('review-native')).toBeLessThan(html.indexOf('Approve this call'))
    else expect(html).toContain('Approval unavailable')
  },
)

it.each([true, false])(
  'leads with the selected call and exact arguments with Run context %s',
  (withRun) => {
    const call: InvocationRow = {
      ...run,
      name: `${workspace}/invocations/selected`,
      run: run.name,
      state: 'ended',
      outcome: 'upstream_outcome',
      startedAt: '2026-09-22T09:00:05Z',
      endedAt: '2026-09-22T09:00:07Z',
      durationNanos: '2000000000',
      dispatchedAt: '2026-09-22T09:00:06Z',
      waitingNanos: '1000000',
      provider: `${workspace}/providers/mail`,
      operation: 'send',
      callPath: 'coral.providers.mail.send',
    }
    const container = document.createElement('div')
    container.innerHTML = markup({
      ...pendingPage,
      selected: call.name,
      selectedRow: { ...call, kind: 'invocation' },
      detail: {
        kind: 'invocation',
        invocation: call,
        arguments: '{"recipient":"exact","count":2}',
        result: { kind: 'unknown' },
        requests: [],
        request: null,
        requestUnavailable: false,
        context: withRun
          ? {
              kind: 'run',
              run,
              source: 'exact-source',
              linkedFunctions: [],
              terminal: { kind: 'unavailable' },
            }
          : undefined,
      },
    })
    expect(within(container).getByRole('heading', { level: 2 })).toHaveTextContent(call.callPath)
    // The provider's resource name is an ID, not a name you read: the call path names it.
    expect(container.textContent).not.toContain('Provider:')
    expect(container.textContent).not.toContain(call.provider)
    const label = within(container).getByText('recipient', { selector: 'dt' })
    expect(label.nextElementSibling).toHaveTextContent('"exact"')
    // The call's own timing reads the same with or without its Run.
    const fact = (term: string) =>
      within(container).getByText(term, { selector: 'dt' }).nextElementSibling
    expectInstant(fact('Started'), call.startedAt, '22 Sep 2026, 09:00:05 UTC')
    expect(fact('Duration')).toHaveTextContent('2s')
    expectInstant(fact('Ended'), call.endedAt!, '22 Sep 2026, 09:00:07 UTC')
    expect(within(container).queryByText('Initiated by')).toBeNull()
    // Under a second of waiting reads as none: no time shows that has not passed.
    expect(fact('Waiting time')).toHaveTextContent('0s')
    expectInstant(fact('Dispatch'), call.dispatchedAt!, '22 Sep 2026, 09:00:06 UTC')
    // The header badge states the call's outcome; no State row repeats it.
    expect(within(container).queryByText('State', { selector: 'dt' })).toBeNull()
    expect(within(container.querySelector('header')!).getByText('completed')).toBeTruthy()
    expect(container.textContent).not.toContain('upstream')
    expect(container.textContent).not.toContain('Technical details')
    expect(container.textContent).not.toContain(call.name)
    if (withRun) {
      // The Run's intent leads the list; its controls stay in view.
      expect(fact('Run intent')).toHaveTextContent('Send invoice reminders')
      expect(
        within(container).getByRole('button', { name: 'Cancel run' }).closest('details'),
      ).toBeNull()
      expect(container.textContent).not.toContain('Whole-run controls')
    } else expect(container.textContent).not.toContain('Send invoice reminders')
  },
)

// Catches: an operation badge that shows Coral's internal outcome name, or that calls a response
// a success or failure that its retained evidence does not prove. Coral classes a 2xx against
// the statuses the Operation declares, which Reef never receives, so nothing here proves success.
it.each([
  [
    'an HTTP status outside 2xx',
    '{"status":404,"data":{"message":"Not found"},"metadata":{"kind":"http"}}',
    'failed',
  ],
  [
    'an HTTP 2xx, which may still be undeclared',
    '{"status":200,"data":{"ok":false},"metadata":{"kind":"http"}}',
    'completed',
  ],
  [
    'an MCP isError, which errs only for a tool call',
    '{"data":{"content":[],"isError":true},"metadata":{"kind":"mcp"}}',
    'completed',
  ],
  ['a status without the HTTP envelope', '{"status":500,"data":{}}', 'completed'],
  ['an unclassified payload', '{"error":"looks bad","success":true}', 'completed'],
  ['malformed JSON', '{"status":', 'completed'],
] as const)('names a call answered with %s by what its response proves', (_, value, label) => {
  const container = show({
    ...invocationPanel({}),
    result: { kind: 'upstream_outcome', value },
  })
  const header = container.querySelector('header')!
  expect(within(header).getByText(label)).toBeTruthy()
  expect(header.textContent).not.toMatch(/succeeded|upstream/)
  // The response itself shows exactly as retained.
  expect(within(container).getByRole('region', { name: 'Provider response' })).toBeTruthy()
})

// Catches: an operation badge that shows Coral's outcome names instead of words you read. The
// card under the header keeps the exact distinction.
it.each([
  [
    'coral_failure',
    { kind: 'coral_failure', code: 'operation_disabled', message: 'Disabled.' },
    'failed',
    'Coral failure: operation_disabled',
  ],
  [
    'run_terminated',
    { kind: 'run_terminated' },
    'run ended',
    'The run terminated this call before dispatch.',
  ],
  [
    'indeterminate',
    { kind: 'indeterminate' },
    'result unknown',
    'External effects may be uncertain.',
  ],
  ['unknown', { kind: 'unknown' }, 'result unknown', ''],
] as const)('names a call that ended with %s in plain words', (outcome, result, label, card) => {
  const container = show({ ...invocationPanel({ outcome }), result })
  const header = container.querySelector('header')!
  expect(within(header).getByText(label)).toBeTruthy()
  expect(header.textContent).not.toMatch(/coral failure|run terminated|indeterminate|_/)
  if (card) expect(container.textContent).toContain(card)
})

// Catches: a log or emission header with a badge that says nothing about it.
it.each([
  [
    'log',
    { kind: 'program.logged', level: 'info', message: 'sent 2' },
    'console.info #4program log',
  ],
  ['emission', { kind: 'program.emitted', value: '{"sent":2}' }, 'coral.emit #4program output'],
] as const)('heads a %s with its own name and kind, and no badge', (_, value, text) => {
  const container = show(event({ ...at, ...value } as RunEvent))
  expect(container.querySelector('header')!.textContent).toBe(text)
})

// Catches: a Run failure headed by Coral's code, or a heading that drops the exact code, message
// or frames.
it.each([
  ['program_rejected', 'The program threw an error'],
  ['invalid_program_value', 'The program returned a value Coral cannot store'],
  ['resource_limit_exceeded', 'A value or the output was too large'],
  ['memory_limit_exceeded', 'The program ran out of memory'],
  ['program_execution_failed', 'The program failed'],
  ['run_authority_lost', 'The run lost the access it needed'],
] as const)(
  'heads a Run that failed with %s in plain words, its code a diagnostic',
  (code, heading) => {
    const container = show({
      ...runPanel({ state: 'failed', endedAt: '2026-09-22T09:05:00Z' }),
      terminal: {
        kind: 'failed',
        code,
        message: 'exact-run-message',
        error: {
          name: 'TypeError',
          message: 'exact-error',
          frames: [{ source: 'program.ts', line: 3, column: 7 }],
          operationCode: 'upstream_rejected',
          operationTraceId: 'exact-trace',
          operationStatus: 0,
          operationBody: '',
          operationBodyTruncated: false,
          cause: null,
          errors: [],
          truncated: false,
          causeOmitted: false,
          omittedErrors: 0,
          circular: false,
        },
      },
    } as RunDetail)
    const card = within(container).getByRole('region', { name: 'Error' })
    const strong =
      card.querySelector('strong, b, [class*="Strong"]') ??
      card.firstElementChild!.firstElementChild!
    expect(strong.textContent).toBe(heading)
    for (const exact of [
      'exact-run-message',
      `Code: ${code}`,
      'program.ts:3:7',
      'upstream_rejected',
      'exact-trace',
    ])
      expect(card.textContent).toContain(exact)
  },
)

it('heads a Run that failed for a reason this Reef does not know, without a code line', () => {
  const container = show({
    ...runPanel({ state: 'failed', endedAt: '2026-09-22T09:05:00Z' }),
    terminal: { kind: 'failed', code: 'unknown', message: 'exact-run-message', error: null },
  } as RunDetail)
  const card = within(container).getByRole('region', { name: 'Error' })
  expect(card.textContent).toBe('The run failedexact-run-message')
})

it('keeps the supplied Run evidence and source beside a selected call, apart from its outcome', () => {
  const failed = { ...run, state: 'failed' as const, endedAt: '2026-09-22T09:01:00Z' }
  const call: InvocationRow = {
    ...failed,
    name: `${workspace}/invocations/evidence`,
    run: failed.name,
    state: 'ended',
    outcome: 'upstream_outcome',
    dispatchedAt: failed.startedAt,
    waitingNanos: '0',
    provider: `${workspace}/providers/mail`,
    operation: 'send',
    callPath: 'coral.providers.mail.send',
  }
  const submission = `${workspace}/tasks/one/interactions/exec`
  const container = document.createElement('div')
  container.innerHTML = markup({
    ...pendingPage,
    selected: call.name,
    selectedRow: { ...call, kind: 'invocation' },
    detail: {
      kind: 'invocation',
      invocation: call,
      arguments: '{}',
      result: { kind: 'upstream_outcome', value: '{"call-outcome":202}' },
      requests: [],
      request: null,
      requestUnavailable: false,
      context: {
        kind: 'run',
        run: failed,
        submission: {
          name: submission,
          agent: '',
          tool: 'exec',
          intent: 'Send invoice reminders',
          outcome: 'succeeded',
          startedAt: failed.startedAt,
          completedAt: failed.startedAt,
          acceptedRun: failed.name,
        },
        source: 'exact-accepted-source',
        linkedFunctions: [
          { savedFunction: 'snapshot-id', path: 'billing.remind', source: 'exact-snapshot' },
        ],
        terminal: {
          kind: 'failed',
          code: 'program_execution_failed',
          message: 'exact-run-failure',
          error: null,
        },
      },
    },
  })
  const disclosure = (title: string) =>
    within(container).getByText(title, { selector: 'summary' }).closest('details')!
  // The call's outcome and the Run's terminal evidence are visible, each in its own card.
  const upstream = within(container).getByRole('region', { name: 'Provider response' })
  expect(upstream).toHaveTextContent('call-outcome')
  expect(upstream).not.toHaveTextContent('exact-run-failure')
  expect(within(container).getByText('program run: failed')).toBeTruthy()
  expect(container.textContent).not.toContain('Run state')
  const failure = container.querySelector('[aria-label="Error"]')!
  expect(failure).toHaveTextContent('program_execution_failed')
  expect(failure).toHaveTextContent('exact-run-failure')
  expect(failure).not.toHaveTextContent('call-outcome')
  expect(failure.closest('details')).toBeNull()
  // Source, snapshots, and the original submission stay retained, folded in Details.
  const details = disclosure('Details')
  expect(details).not.toHaveAttribute('open')
  expect(disclosure('Accepted program source')).toHaveTextContent('exact-accepted-source')
  expect(disclosure('Saved-function snapshot: billing.remind')).toHaveTextContent('exact-snapshot')
  for (const title of ['Accepted program source', 'Saved-function snapshot: billing.remind'])
    expect(details.contains(disclosure(title))).toBe(true)
  const original = within(details).getByText('Inspect original exec submission')
  expect(new URL(original.getAttribute('href')!, 'http://reef').searchParams.get('record')).toBe(
    submission,
  )
  // The Run's own times and IDs belong to the Run's panel.
  for (const gone of ['Run ended', 'Run ID', 'Technical details', failed.name])
    expect(container.textContent).not.toContain(gone)
})

it.each([
  { outcome: 'open', withFeedback: false },
  { outcome: 'success', withFeedback: true },
  { outcome: 'success', withFeedback: false },
  { outcome: 'failure', withFeedback: true },
  { outcome: 'failure', withFeedback: false },
])(
  'states a $outcome Task in the requested order without technical details (feedback: $withFeedback)',
  ({ outcome, withFeedback }) => {
    const task = {
      name: `${workspace}/tasks/invoices`,
      workspace,
      agent: '',
      actor: 'opaque-principal',
      intent: 'Find invoices',
      outcome,
      startedAt: '2026-09-22T09:00:00Z',
      endedAt: outcome === 'open' ? '' : '2026-09-22T09:05:00Z',
    }
    const feedback = 'No export operation.\nSearch returned nothing.'
    const container = document.createElement('div')
    container.innerHTML = markup({
      ...pendingPage,
      selected: task.name,
      selectedRow: { ...task, kind: 'task' },
      detail: { kind: 'task', task, ...(withFeedback && { feedback }) },
    })
    const list = container.querySelector('dl')!
    const value = (term: string) =>
      within(list).getByText(term, { selector: 'dt' }).nextElementSibling!
    const header = container.querySelector('header')!
    // The record kind names the Task beside its title, once; the list below has no heading.
    expect(within(header).getByRole('heading', { level: 2 })).toHaveTextContent(task.intent)
    expect(within(header).getByText('task')).toBeTruthy()
    expect(within(container).queryByRole('heading', { name: 'Task' })).toBeNull()
    expect(within(container).getAllByText('task')).toHaveLength(1)
    // The outcome appears once in the header badge, not in the Task list.
    expect(header.querySelector('svg')).not.toBeNull()
    expect(within(header).getByText(task.outcome)).toBeTruthy()
    expect(within(list).queryByText('Status', { selector: 'dt' })).toBeNull()
    expectInstant(value('Started'), task.startedAt, '22 Sep 2026, 09:00:00 UTC')
    expect(within(list).queryByText('Initiated by')).toBeNull()
    if (task.endedAt) expectInstant(value('Ended'), task.endedAt, '22 Sep 2026, 09:05:00 UTC')
    expect(
      within(list)
        .getAllByRole('term')
        .map((term) => term.textContent),
    ).toEqual([
      'Context',
      'Started',
      ...(task.endedAt ? ['Ended'] : []),
      ...(withFeedback ? ['Feedback'] : []),
    ])
    expect(within(container).queryByText('Technical details')).toBeNull()
    expect(container.textContent).not.toContain(task.name)
    if (withFeedback) {
      // Feedback reads like every other value: same typography, line breaks kept.
      expect(value('Feedback').className).toBe(value('Started').className)
      expect(value('Feedback').textContent).toBe(feedback)
    } else expect(within(list).queryByText('Feedback', { selector: 'dt' })).toBeNull()
    for (const gone of ['Goal outcome', 'Completion feedback', 'accepts no new tool calls'])
      expect(container.textContent).not.toContain(gone)
  },
)

// Catches: a Task detail that drops, zeroes or re-formats the context total its table row shows.
it.each([
  {
    outcome: 'success',
    bytes: '-2200000',
    partial: false,
    label: '−2.2 MB',
    meaning: '2.2 MB saved',
  },
  {
    outcome: 'failure',
    bytes: '14500',
    partial: false,
    label: '+14.5 kB',
    meaning: '14.5 kB added',
  },
  {
    outcome: 'success',
    bytes: '0',
    partial: false,
    label: '0 B',
    meaning: 'No net context change',
  },
  {
    outcome: 'open',
    bytes: undefined,
    partial: false,
    label: '—',
    meaning: 'Context impact unavailable',
  },
  {
    outcome: 'open',
    bytes: '-55500',
    partial: true,
    label: '−55.5 kB',
    meaning: '55.5 kB saved so far',
  },
])(
  'states a $outcome Task context total of $bytes bytes as its table row does (partial: $partial)',
  ({ outcome, bytes, partial, label, meaning }) => {
    const task = {
      name: `${workspace}/tasks/invoices`,
      workspace,
      agent: '',
      actor: 'opaque-principal',
      intent: 'Find invoices',
      outcome,
      startedAt: '2026-09-22T09:00:00Z',
      endedAt: outcome === 'open' ? null : '2026-09-22T09:05:00Z',
      contextImpactBytes: bytes,
      contextImpactPartial: partial,
    }
    const container = document.createElement('div')
    container.innerHTML = markup({
      ...pendingPage,
      selected: task.name,
      selectedRow: { ...task, kind: 'task' },
      detail: { kind: 'task', task },
    })
    const list = container.querySelector('dl')!
    const value = within(list).getByText('Context', { selector: 'dt' }).nextElementSibling!
    const impact = within(value as HTMLElement).getByLabelText(meaning)
    expect(impact.textContent).toBe(label)
    // The same element as the table's Context cell: sign, rounding, colour and tooltip.
    expect(value.innerHTML).toBe(
      renderToStaticMarkup(<ContextImpact bytes={bytes} partial={partial} />),
    )
    expect(within(list).getAllByRole('term')[0].textContent).toBe('Context')
    // Bytes of context, never a price or a token count.
    expect(list.textContent).not.toMatch(/[$€]|tokens?\b/)
  },
)

/** A page that selects `task` and shows its detail, as the loader returns it. */
function taskDetailPage(task: Omit<ActivityRow & { kind: 'task' }, 'kind'>): ActivityPage {
  return {
    ...pendingPage,
    selected: task.name,
    selectedRow: { ...task, kind: 'task' },
    detail: { kind: 'task', task },
  }
}

// Catches: a live page that refreshes the Task but leaves its context total stale.
it('replaces the Task context total when a live page brings a new one', () => {
  const task = {
    name: `${workspace}/tasks/invoices`,
    workspace,
    agent: '',
    actor: 'opaque-principal',
    intent: 'Find invoices',
    outcome: 'open',
    startedAt: '2026-09-22T09:00:00Z',
    endedAt: null,
    contextImpactBytes: '-55500',
    contextImpactPartial: true,
  }
  const { update } = renderLivePage(
    (page) => (
      <ActionReviewSession>
        <ActivityDetail page={page} />
      </ActionReviewSession>
    ),
    taskDetailPage(task),
    { entry: '/activity' },
  )
  const before = screen.getByLabelText('55.5 kB saved so far')
  expect(before).toHaveTextContent('−55.5 kB')
  update(
    taskDetailPage({
      ...task,
      outcome: 'success',
      endedAt: '2026-09-22T09:05:00Z',
      contextImpactBytes: '-2200000',
      contextImpactPartial: false,
    }),
  )
  expect(screen.queryByLabelText('55.5 kB saved so far')).toBeNull()
  expect(screen.getByLabelText('2.2 MB saved')).toHaveTextContent('−2.2 MB')
})

const interaction: InteractionRow = {
  agent: '',
  name: `${workspace}/tasks/one/interactions/one`,
  tool: 'search',
  intent: 'Find mail operations',
  outcome: 'succeeded',
  startedAt: run.startedAt,
  completedAt: run.startedAt,
  acceptedRun: '',
}
const searched = (query: string | null): InteractionEvidence => ({
  kind: 'discovery',
  context: { kind: 'search', query, provider: null, limit: 5, cursor: null },
  response: '{}',
  view: { kind: 'raw' },
})
type Detail = NonNullable<ActivityPage['detail']>
const tool = (overrides: Partial<InteractionRow>, evidence: InteractionEvidence): Detail => ({
  kind: 'tool',
  task: `${workspace}/tasks/one`,
  interaction: { ...interaction, ...overrides },
  evidence,
})
const runPanel = (overrides: Partial<typeof run>, submission?: InteractionRow): RunDetail => ({
  kind: 'run',
  run: { ...run, ...overrides, actionPage: { total: '0', actions: [], next: '' } },
  submission,
  request: null,
  source: 'return null;',
  linkedFunctions: [],
  terminal: { kind: 'unavailable' },
})
const invocationPanel = (overrides: Partial<InvocationRow>): InvocationDetail => ({
  kind: 'invocation',
  invocation: {
    ...run,
    name: `${workspace}/invocations/one`,
    run: run.name,
    state: 'ended',
    outcome: 'upstream_outcome',
    dispatchedAt: run.startedAt,
    waitingNanos: '0',
    provider: `${workspace}/providers/mail`,
    operation: 'send',
    callPath: 'coral.providers.mail.send',
    ...overrides,
  },
  arguments: '{}',
  result: { kind: 'unknown' },
  requests: [],
  request: null,
  requestUnavailable: false,
})
const event = (value: RunEvent): Extract<Detail, { kind: 'output' }> => ({
  kind: 'output',
  run: run.name,
  event: value,
})
const at = { sequence: '4', occurredAt: run.startedAt }

it.each<[string, Detail, string, string]>([
  [
    'a Run by its submission intent',
    runPanel({}, { ...interaction, tool: 'exec', intent: 'Remind overdue owners' }),
    'Remind overdue owners',
    'program run',
  ],
  ['a Run by its own intent', runPanel({}), run.intent!, 'program run'],
  [
    'a Run without an intent, by its kind alone',
    runPanel({ intent: '' }),
    'Program run',
    'program run',
  ],
  [
    'an operation call by its call path',
    invocationPanel({}),
    'coral.providers.mail.send',
    'operation',
  ],
  ['an operation call without a call path', invocationPanel({ callPath: '' }), 'send', 'operation'],
  ['a search by its intent', tool({}, searched('mail send')), interaction.intent, 'search'],
  [
    'a search without an intent, by its query',
    tool({ intent: '' }, searched('mail send')),
    'mail send',
    'search',
  ],
  ['a search without a query', tool({}, searched(null)), interaction.intent, 'search'],
  ['a search with nothing specific', tool({ intent: '' }, { kind: 'unknown' }), 'search', 'search'],
  [
    'a describe call by the exact call path',
    tool(
      { tool: 'describe' },
      {
        kind: 'discovery',
        context: { kind: 'describe', callPath: 'coral.providers.mail.send', types: [] },
        response: '{}',
        view: { kind: 'raw' },
      },
    ),
    'coral.providers.mail.send',
    'describe',
  ],
  [
    'an exec submission by its intent',
    tool({ tool: 'exec', intent: 'Send reminders' }, { kind: 'run', reference: run.name }),
    'Send reminders',
    'exec submission',
  ],
  [
    // A save states no target: its reference is an ID that no page opens.
    'a saved function without an intent by its tool, not its ID',
    tool(
      { tool: 'save_function', intent: '' },
      { kind: 'function', reference: `${workspace}/savedFunctions/reminders` },
    ),
    'Save function',
    'save function',
  ],
  [
    'a deleted function without an intent by its tool, not its ID',
    tool(
      { tool: 'delete_function', intent: '' },
      { kind: 'function', reference: `${workspace}/savedFunctions/reminders` },
    ),
    'Delete function',
    'delete function',
  ],
  [
    'a deleted function by its intent',
    tool({ tool: 'delete_function', intent: 'Remove the old batch' }, { kind: 'unknown' }),
    'Remove the old batch',
    'delete function',
  ],
  [
    'another tool with nothing specific by its tool',
    tool({ tool: 'inspect', intent: '' }, { kind: 'unknown' }),
    'inspect',
    'tool interaction',
  ],
  [
    'a log by its console level and record number, never its message',
    event({
      ...at,
      kind: 'program.logged',
      level: 'error',
      message: 'Fetched 3 invoices\nNext page',
    }),
    'console.error #4',
    'program log',
  ],
  [
    'a log at a level this Reef does not know, as stored',
    event({ ...at, kind: 'program.logged', level: 'unknown', message: '  ' }),
    'console.unknown #4',
    'program log',
  ],
  [
    'an emission by its call and record number, never its payload',
    event({ ...at, kind: 'program.emitted', value: '{"intent":"misleading"}' }),
    'coral.emit #4',
    'program output',
  ],
  ['a run start', event({ ...at, kind: 'run.started' }), 'Program run started', 'run event'],
  [
    'a run end by its state',
    event({ ...at, kind: 'run.ended', state: 'timed_out' }),
    'Program run ended: timed out',
    'run event',
  ],
  [
    'a cancellation request',
    event({ ...at, kind: 'run.cancellation_requested', actor: 'you', actorKind: 'user' }),
    'Cancellation requested',
    'run event',
  ],
  [
    'an operation call start',
    event({ ...at, kind: 'invocation.started', invocation: `${workspace}/invocations/one` }),
    'Operation call started',
    'run event',
  ],
  [
    'an operation call that got a response, in plain words',
    event({
      ...at,
      kind: 'invocation.ended',
      invocation: `${workspace}/invocations/one`,
      outcome: 'upstream_outcome',
    }),
    'Operation call ended: completed',
    'run event',
  ],
  [
    'an operation call the Run ended, in plain words',
    event({
      ...at,
      kind: 'invocation.ended',
      invocation: `${workspace}/invocations/one`,
      outcome: 'run_terminated',
    }),
    'Operation call ended: run ended',
    'run event',
  ],
  [
    'an operation call with no complete result, in plain words',
    event({
      ...at,
      kind: 'invocation.ended',
      invocation: `${workspace}/invocations/one`,
      outcome: 'indeterminate',
    }),
    'Operation call ended: result unknown',
    'run event',
  ],
  [
    'an operation call end by its outcome',
    event({
      ...at,
      kind: 'invocation.ended',
      invocation: `${workspace}/invocations/one`,
      outcome: 'coral_failure',
    }),
    'Operation call ended: failed',
    'run event',
  ],
  ['an unknown event', event({ ...at, kind: 'unknown' }), 'Unknown run event', 'run event'],
])('names %s, with its record kind under the title', (_, detail, title, kind) => {
  const container = document.createElement('div')
  container.innerHTML = markup({ ...pendingPage, selected: null, selectedRow: null, detail })
  const header = container.querySelector('header')!
  expect(within(header).getByRole('heading', { level: 2 }).textContent).toBe(title)
  const subtitles = within(header)
    .getAllByText(kind)
    .filter((line) => !line.closest('h2'))
  expect(subtitles).toHaveLength(1)
})

it.each<[string, RunDetail, string | null]>([
  ['running', runPanel({ requiresAction: false }), null],
  [
    'succeeded',
    {
      ...runPanel({ state: 'succeeded', endedAt: '2026-09-22T09:05:00Z', requiresAction: false }),
      terminal: { kind: 'succeeded', finalValue: '{"reminded":24}' },
    },
    '"reminded"',
  ],
  [
    'failed',
    {
      ...runPanel({ state: 'failed', endedAt: '2026-09-22T09:05:00Z', requiresAction: false }),
      terminal: {
        kind: 'failed',
        code: 'program_execution_failed',
        message: 'exact-run-failure',
        error: null,
      },
    },
    'exact-run-failure',
  ],
])(
  'leads a %s Run with its timing and outcome, without retained internals',
  (state, base, outcome) => {
    const detail: RunDetail = {
      ...base,
      submission: {
        ...interaction,
        tool: 'exec',
        name: `${workspace}/tasks/one/interactions/exec`,
      },
      source: 'exact-accepted-source',
      linkedFunctions: [
        { savedFunction: 'snapshot-id', path: 'billing.remind', source: 'exact-snapshot' },
      ],
    }
    const container = document.createElement('div')
    container.innerHTML = markup({ ...pendingPage, selected: null, selectedRow: null, detail })
    const timing = container.querySelector('dl')!
    // The timing list comes straight after the header.
    expect(container.querySelector('header')!.nextElementSibling).toBe(timing.closest('section'))
    const ended = detail.run.endedAt
    expect(
      within(timing)
        .getAllByRole('term')
        .map((term) => term.textContent),
    ).toEqual(['Duration', 'Started', ...(ended ? ['Ended'] : [])])
    const fact = (term: string) =>
      within(timing).getByText(term, { selector: 'dt' }).nextElementSibling
    expectInstant(fact('Started'), detail.run.startedAt, '22 Sep 2026, 09:00:00 UTC')
    if (ended) expectInstant(fact('Ended'), ended, '22 Sep 2026, 09:05:00 UTC')
    for (const gone of [
      'Context',
      'Requested by',
      'Run state',
      'View in history',
      'Technical details',
      'Inspect original exec submission',
      'Accepted program source',
      'exact-accepted-source',
      'Saved-function snapshot',
      'exact-snapshot',
      detail.run.name,
    ])
      expect(container.textContent).not.toContain(gone)
    if (outcome) expect(container.textContent).toContain(outcome)
    // A returned value shows directly in its own card, never behind a disclosure.
    const returned = container.querySelector('[aria-label="Final value"]')
    expect(Boolean(returned)).toBe(state === 'succeeded')
    if (returned) expect(returned.textContent).toBe(returned.querySelector('pre')!.textContent)
    expect(within(container).queryByText('Final value', { selector: 'summary' })).toBeNull()
    expect(Boolean(within(container).queryByRole('button', { name: 'Cancel run' }))).toBe(
      state === 'running',
    )
  },
)

const searchContext = {
  kind: 'search',
  query: 'github pulls',
  provider: null,
  limit: 5,
  cursor: null,
} as const
const discovered = (context: DiscoveryContext, response: string): InteractionEvidence => ({
  kind: 'discovery',
  context,
  response,
  view: discoveryView(context, response),
})
/** A retained search; `current` stands in for the loader resolving today's catalog links. */
const searchPanel = (response: string, current: (index: number) => string | null = () => null) => {
  const evidence = discovered(searchContext, response)
  if (evidence.kind === 'discovery' && evidence.view.kind === 'search')
    evidence.view.page.hits.forEach((hit, index) => (hit.currentHref = current(index)))
  const container = document.createElement('div')
  container.innerHTML = markup({
    ...pendingPage,
    selected: null,
    selectedRow: null,
    detail: tool({ tool: 'search' }, evidence),
  })
  return container
}
const factIn = (list: Element, term: string) =>
  within(list as HTMLElement).getByText(term, { selector: 'dt' }).nextElementSibling
const terms = (list: Element) =>
  within(list as HTMLElement)
    .getAllByRole('term')
    .map((term) => term.textContent)

it('leads a search with what it asked, then its intent and timing, and only its results', () => {
  const container = searchPanel(stored.complete)
  const [asked, call] = container.querySelectorAll('dl')
  // What the agent asked for comes first, under one Search heading, and only once. The heading
  // sits one level under the panel's, so the outline skips no level.
  expect(within(container).getByRole('heading', { level: 2 })).toBeTruthy()
  expect(asked.closest('section')!.querySelector('h3')).toHaveTextContent('Search')
  expect(terms(asked)).toEqual(['Query', 'Provider', 'Page'])
  expect(factIn(asked, 'Query')).toHaveTextContent('“github pulls”')
  expect(factIn(asked, 'Provider')).toHaveTextContent('all providers')
  expect(factIn(asked, 'Page')).toHaveTextContent('first page, up to 5 results')
  expect(within(container).getAllByText('Query', { selector: 'dt' })).toHaveLength(1)
  // Then the intent and the call's own times, without a heading.
  expect(call.closest('section')!.querySelector('h4')).toBeNull()
  // The intent is the title, so no row repeats it.
  expect(within(container).getByRole('heading', { level: 2 }).textContent).toBe(interaction.intent)
  expect(terms(call)).toEqual(['Started', 'Completed'])
  expectInstant(factIn(call, 'Started'), interaction.startedAt, '22 Sep 2026, 09:00:00 UTC')
  expectInstant(factIn(call, 'Completed'), interaction.completedAt!, '22 Sep 2026, 09:00:00 UTC')
  for (const heading of ['Context', 'Call'])
    expect(within(container).queryByRole('heading', { name: heading })).toBeNull()
  for (const gone of ['Submission outcome', 'Raw response', 'Technical details', 'searched'])
    expect(container.textContent).not.toContain(gone)
  // One Results heading counts the page and leads the ranked results. A trimmed page
  // states no per-kind coverage, and its heading already says more remain.
  const results = container.querySelector('[aria-label="Results in rank order"]')!
  expect(results.querySelectorAll(':scope > li')).toHaveLength(5)
  const heading = within(container).getByRole('heading', { name: 'Results (5+)' })
  expect(heading.closest('section')!.contains(results)).toBe(true)
  expect(container.querySelector('[aria-label="Search coverage"]')).toBeNull()
  expect(container.textContent).not.toContain('More results were available')
})

/** A stored trimmed search page with `count` operation hits. */
const trimmedPage = (count: number, more: boolean, statuses?: unknown[]) =>
  JSON.stringify({
    hits: Array.from({ length: count }, (_, index) => ({
      call_path: `coral.providers.github.pulls.op${index}`,
      signature_line: `op${index}(input: {}): Promise<unknown>`,
      description_snippet: '',
    })),
    has_more: more,
    ...(statuses && { retriever_statuses: statuses }),
  })
const failedOperations = {
  retriever: 'operation',
  state: 'error',
  coverage: {
    eligible_units: 3,
    searched_units: 0,
    failed_units: 3,
    returned_count: 0,
    has_more: false,
  },
}

// Catches: a heading that counts the limit, invents a total, or drops a coverage warning.
// Coral stores no total match count: only the page's hits and whether more remain.
it.each<[string, string, typeof searchContext | undefined, string, string[]]>([
  ['a complete page', trimmedPage(2, false), undefined, 'Results (2)', []],
  ['a page with more after it', stored.complete, undefined, 'Results (5+)', []],
  ['an empty page', trimmedPage(0, false), undefined, 'Results (0)', []],
  [
    'a later page, counting only its own results',
    trimmedPage(3, true),
    { ...searchContext, cursor: 'next-page' } as unknown as typeof searchContext,
    'Results (3+)',
    [],
  ],
  [
    'partial coverage',
    stored.partial,
    undefined,
    'Results (3)',
    ['1 of 3 providers failed', 'Results may be missing.'],
  ],
  [
    'a kind that could not be searched',
    stored.error,
    undefined,
    'Results (1)',
    ['could not search', 'Results are missing.'],
  ],
  [
    'an empty page whose search failed',
    trimmedPage(0, false, [failedOperations]),
    undefined,
    'Results (0)',
    ['could not search', 'Results are missing.'],
  ],
  [
    'an untrimmed page with per-kind counts',
    stored.untrimmed,
    undefined,
    'Results (5+)',
    ['100+ found', 'none match'],
  ],
])('heads %s with its own count', (_, response, context, heading, coverage) => {
  const evidence = discovered(context ?? searchContext, response)
  const container = document.createElement('div')
  container.innerHTML = markup({
    ...pendingPage,
    selected: null,
    selectedRow: null,
    detail: tool({ tool: 'search' }, evidence),
  })
  const view =
    evidence.kind === 'discovery' && evidence.view.kind === 'search' ? evidence.view : null
  const title = within(container).getByRole('heading', { name: heading })
  const section = title.closest('section')!
  // The count is the results Coral returned on this page, not the page limit.
  expect(section.querySelectorAll('[aria-label="Results in rank order"] > li')).toHaveLength(
    view!.page.hits.length,
  )
  if (view!.page.hits.length === 0) expect(section).toHaveTextContent('This page has no results.')
  const strip = section.querySelector('[aria-label="Search coverage"]')
  if (coverage.length === 0) expect(strip).toBeNull()
  for (const text of coverage) expect(strip).toHaveTextContent(text)
  expect(container.textContent).not.toContain('More results were available')
  expect(container.textContent).not.toMatch(/Results \(\d+\/\d+\)/)
})

/** Today's catalog page for each result, except the second, which is gone. */
const catalogHref = (index: number) =>
  index === 1 ? null : `/${workspace}/providers/github/ops/pulls.${index}`
it("links a result to today's catalog with a labelled icon beside its call path, or not at all", () => {
  const container = searchPanel(stored.complete, catalogHref)
  const hits = container.querySelectorAll('[aria-label="Results in rank order"] > li')
  hits.forEach((hit, index) => {
    const path = hit.querySelector('code')!.textContent!
    const link = hit.querySelector('a')
    if (catalogHref(index) === null) {
      // A target gone from today's catalog keeps its call path and gets no link.
      expect(link).toBeNull()
      return
    }
    expect(link).toHaveAttribute('href', catalogHref(index))
    expect(link).toHaveAttribute('aria-label', `Open current operation ${path}`)
    expect(link).not.toHaveAttribute('target')
    // The icon sits beside the exact call path, not in a text line of its own.
    expect(link!.parentElement!.textContent).toBe(path)
    expect(link!.querySelector('svg')).not.toBeNull()
  })
  expect(container.textContent).not.toContain('Open current operation')
})

it.each([
  ['operations', stored.complete],
  ['operations and saved functions', stored.mixed],
])('shows each %s result as one compact row in its retained order', (_, response) => {
  const view = discoveryView(searchContext, response)
  if (view.kind !== 'search') throw new Error('expected a search page')
  const hits = view.page.hits
  const container = searchPanel(response)
  const rows = [...container.querySelectorAll('[aria-label="Results in rank order"] > li')]
  expect(rows.map((row) => row.querySelector('code')!.textContent)).toEqual(
    hits.map((hit) => hit.callPath),
  )
  rows.forEach((row, index) => {
    const hit = hits[index]
    expect(row).toHaveTextContent(hit.title)
    expect(row).toHaveTextContent(hit.kind === 'operation' ? 'operation' : 'saved function')
    if (hit.matched.fields.length + hit.matched.tags.length > 0)
      expect(row).toHaveTextContent('Matched in')
    // Approval shows as the Providers page's Needs approval hand, named, never as a text badge.
    const approval = row.querySelector('[role="img"][aria-label^="Needs approval"]')
    expect(Boolean(approval)).toBe(Boolean(hit.operation?.approvalRequired))
    if (approval) expect(approval.querySelector('svg')).not.toBeNull()
    expect(row.textContent).not.toContain('approval required')
    if (hit.operation?.deprecated) expect(row).toHaveTextContent('deprecated')
    for (const [hint, label] of [
      ['readOnly', 'read-only'],
      ['destructive', 'destructive'],
      ['idempotent', 'idempotent'],
      ['openWorld', 'open world'],
    ] as const) {
      const declared = hit.operation?.hints[hint]
      if (declared !== undefined) expect(row).toHaveTextContent(declared ? label : `not ${label}`)
    }
    // Matched paths show as a count chip; the paths themselves stay with the data.
    const paths = hit.matched.paths.length
    if (paths > 0)
      expect(row).toHaveTextContent(
        paths === 1 ? '1 declaration path' : `${paths} declaration paths`,
      )
    expect(row.textContent).not.toContain('Matched declaration paths')
    expect(row.textContent!.startsWith(String(index + 1))).toBe(false)
    // The card's own closed Details, at its foot, holds the retained signature, description
    // and guidance exactly; the summary above it shows none of them.
    const details = row.querySelector(':scope > details')!
    expect(row.querySelectorAll('details')).toHaveLength(1)
    expect(row.lastElementChild).toBe(details)
    expect(details).not.toHaveAttribute('open')
    expect(details.querySelector('summary')!.textContent).toBe('Details')
    expect(details.querySelector('pre')!.textContent).toBe(hit.signatureLine)
    expect(within(details as HTMLElement).getByText('Signature')).toBeTruthy()
    expect(details.textContent).toContain(hit.descriptionSnippet)
    if (hit.guidanceSnippet === null) expect(details.textContent).not.toContain('Guidance')
    else expect(factIn(details, 'Guidance')!.textContent).toBe(hit.guidanceSnippet)
    const summary = row.cloneNode(true) as HTMLElement
    summary.querySelector('details')!.remove()
    expect(summary.querySelector('pre')).toBeNull()
    expect(summary.textContent).not.toContain(hit.signatureLine)
    if (hit.descriptionSnippet)
      expect(summary.textContent).not.toContain(hit.descriptionSnippet.slice(0, 40))
    expect(summary.textContent).not.toContain('Guidance')
  })
})

// Catches: an empty Details toggle on a result that retained no documentation, or a Details
// that shows a part the result did not retain.
it.each<[string, Record<string, unknown>, string[] | null]>([
  ['nothing beyond its call path', { signature_line: '', description_snippet: ' ' }, null],
  [
    'only guidance',
    { signature_line: '', description_snippet: '', guidance_snippet: 'Ask first.' },
    ['Guidance', 'Ask first.'],
  ],
  [
    'only a signature',
    { signature_line: 'get(input: {}): Promise<unknown>', description_snippet: '' },
    ['Signature', 'get(input: {}): Promise<unknown>'],
  ],
])('gives a result with %s Details only for what it retained', (_, retained, shown) => {
  const container = searchPanel(
    JSON.stringify({
      hits: [{ call_path: 'coral.providers.github.pulls.get', ...retained }],
      has_more: false,
    }),
  )
  const row = container.querySelector('[aria-label="Results in rank order"] > li')!
  expect(row.querySelector('code')!.textContent).toBe('coral.providers.github.pulls.get')
  const details = row.querySelector('details')
  if (shown === null) {
    expect(details).toBeNull()
    expect(row.textContent).not.toContain('Details')
    return
  }
  expect(details).not.toBeNull()
  for (const text of shown) expect(details).toHaveTextContent(text)
  for (const absent of ['Guidance', 'Signature'].filter((part) => !shown.includes(part)))
    expect(details).not.toHaveTextContent(absent)
})

it('shows a search stored in an earlier shape as stored', () => {
  const container = searchPanel(stored.earlierShape)
  expect(container.textContent).toContain('Stored in an earlier shape')
  expect(container.querySelector('pre')!.textContent).toContain('typescript_declaration')
  expect(container.textContent).not.toContain('Technical details')
})

type Described = Extract<DiscoveryContext, { kind: 'describe' }>
const describedOperation: Described = {
  kind: 'describe',
  callPath: 'coral.providers.github.issues.create',
  types: [],
}
const describedFunction: Described = {
  kind: 'describe',
  callPath: 'coral.functions.github.openIssuesByLabel',
  types: ['Issue', 'Label'],
}
/** A retained `describe` call, selected, with `evidence` as Coral stored it. */
const describePanel = (evidence: InteractionEvidence, overrides: Partial<InteractionRow> = {}) => {
  const name = `${workspace}/tasks/one/interactions/describe`
  const container = document.createElement('div')
  container.innerHTML = markup({
    ...pendingPage,
    selected: name,
    selectedRow: null,
    detail: tool({ tool: 'describe', name, intent: 'Check the inputs', ...overrides }, evidence),
  })
  return { container, name }
}
const detailsOf = (container: HTMLElement) =>
  within(container).getByText('Details', { selector: 'summary' }).closest('details')!

// Catches: a describe panel that buries its target under generic headings, or drops
// retained documentation when it folds it away.
it.each([
  {
    case: 'an operation',
    context: describedOperation,
    response: stored.describeOperation,
    asked: ['Call path', 'Provider', 'Intent'],
    title: 'Create an issue',
    kind: 'operation',
    summary: ['issues'],
    folded: ['namespace issues', 'Any user with pull access', 'API method documentation'],
  },
  {
    case: 'a saved function with diagnostics',
    context: describedFunction,
    response: stored.describeFunction,
    asked: ['Call path', 'Types', 'Intent'],
    title: 'coral.functions.github.openIssuesByLabel',
    kind: 'saved function',
    summary: ['1 error'],
    folded: [
      'function openIssuesByLabel',
      'const limit = input.limit ?? 20',
      "Cannot find name 'coral'.",
      'error TS2304',
    ],
  },
])('shows a describe of $case like a search, with its documentation in Details', (row) => {
  const evidence = discovered(row.context, row.response)
  const { container, name } = describePanel(evidence)
  const header = container.querySelector('header')!
  expect(within(header).getByRole('heading', { level: 2 }).textContent).toBe(row.context.callPath)
  expect(within(header).getByText('describe')).toBeTruthy()
  // One list without a heading, as the subtitle names the call: what the agent asked, then
  // the call's own times, so no section gap splits them. Details holds lists of its own.
  const lists = [...container.querySelectorAll('dl')].filter((list) => !list.closest('details'))
  expect(lists).toHaveLength(1)
  const [asked] = lists
  expect(asked.closest('section')!.querySelector('h4')).toBeNull()
  expect(within(container).queryByRole('heading', { name: 'Describe' })).toBeNull()
  expect(terms(asked)).toEqual([...row.asked, 'Started', 'Completed'])
  expect(factIn(asked, 'Call path')).toHaveTextContent(row.context.callPath)
  if (row.context.types.length)
    expect(factIn(asked, 'Types')).toHaveTextContent(row.context.types.join(''))
  expect(factIn(asked, 'Intent')).toHaveTextContent('Check the inputs')
  expectInstant(factIn(asked, 'Completed'), interaction.completedAt!, '22 Sep 2026, 09:00:00 UTC')
  for (const heading of ['Context', 'Call'])
    expect(within(container).queryByRole('heading', { name: heading })).toBeNull()
  for (const gone of ['Submission outcome', 'Technical details'])
    expect(container.textContent).not.toContain(gone)
  // A compact Result names the target, its kind and its exact call path.
  const result = within(container).getByRole('heading', { name: 'Result' }).closest('section')!
  expect(result).toHaveTextContent(row.title)
  expect(result).toHaveTextContent(row.kind)
  expect(result.querySelector('code')!.textContent).toBe(row.context.callPath)
  for (const text of row.summary) expect(result).toHaveTextContent(text)
  // Every retained part of the documentation stays, folded into a closed Details at the
  // foot of the same Result card, so opening it grows that card.
  const details = detailsOf(container)
  expect(details).not.toHaveAttribute('open')
  const card = details.parentElement!
  expect(result.contains(card)).toBe(true)
  expect(card.lastElementChild).toBe(details)
  expect(card).toHaveTextContent(row.title)
  expect(card.querySelector('code')!.textContent).toBe(row.context.callPath)
  expect(result.querySelectorAll('details')).toHaveLength(1)
  for (const text of row.folded) expect(details).toHaveTextContent(text)
  const summary = card.cloneNode(true) as HTMLElement
  summary.querySelector('details')!.remove()
  for (const text of row.folded) expect(summary).not.toHaveTextContent(text)
  for (const text of row.summary) expect(summary).toHaveTextContent(text)
  expect(details).toHaveTextContent(name)
  // A describe shows its documentation, not the raw response it was read from.
  expect(container.textContent).not.toContain('Raw response')
  expect(container.querySelectorAll('details')).toHaveLength(1)
})

it('keeps every retained operation fact when it folds the documentation away', () => {
  const evidence = discovered(describedOperation, stored.describeOperation)
  if (evidence.kind !== 'discovery' || evidence.view.kind !== 'describe') throw new Error()
  const { container } = describePanel(evidence)
  const description = evidence.view.description
  const details = detailsOf(container)
  expect(details.querySelector('pre')!.textContent).toBe(description.declaration)
  expect(factIn(container.querySelector('dl')!, 'Provider')).toHaveTextContent(
    description.operation!.providerLabel,
  )
})

it.each<[string, InteractionEvidence, Partial<InteractionRow>, string[]]>([
  [
    'a rejected describe',
    { kind: 'rejection', code: 'invalid_argument', message: 'Unknown call path' },
    { outcome: 'rejected' },
    ['Rejected: invalid_argument', 'Unknown call path'],
  ],
  [
    'a describe whose completion is unknown',
    { kind: 'unknown' },
    { outcome: 'unknown', completedAt: null },
    ['Completion is unknown or incomplete.'],
  ],
  [
    'a describe whose request context is unavailable',
    {
      kind: 'discovery',
      context: { kind: 'unknown' },
      response: '{"opaque":true}',
      view: { kind: 'raw' },
    },
    {},
    ['The request context is unavailable.'],
  ],
  [
    'a describe stored in an earlier shape',
    discovered(describedOperation, '{"opaque":true}'),
    {},
    ['Stored in an earlier shape', 'Reef cannot show this response in the current layout.'],
  ],
])('shows only the notice for %s, with no Details', (_, evidence, overrides, notices) => {
  const { container, name } = describePanel(evidence, overrides)
  for (const notice of notices) expect(container.textContent).toContain(notice)
  const lists = container.querySelectorAll('dl')
  expect(lists).toHaveLength(1)
  // The call's own times close its one list.
  const times = overrides.completedAt === null ? ['Started'] : ['Started', 'Completed']
  expect(terms(lists[0]).slice(-times.length)).toEqual(times)
  // Nothing retained to describe: no Details holding only the record name, and no raw response.
  expect(container.querySelector('details')).toBeNull()
  expect(container.querySelector('hr')).toBeNull()
  expect(container.textContent).not.toContain(name)
  for (const gone of ['Details', 'Raw response', 'opaque', 'The stored response follows.'])
    expect(container.textContent).not.toContain(gone)
  expect(within(container).queryByRole('heading', { name: 'Result' })).toBeNull()
})

/** A retained `exec` submission, selected, with `evidence` as Coral stored it. */
const execPanel = (overrides: Partial<InteractionRow>, evidence: InteractionEvidence) => {
  const name = `${workspace}/tasks/one/interactions/exec`
  const container = document.createElement('div')
  container.innerHTML = markup({
    ...pendingPage,
    selected: name,
    selectedRow: null,
    detail: tool({ tool: 'exec', name, intent: 'Send invoice reminders', ...overrides }, evidence),
  })
  return { container, name }
}
/** The Wax list and card classes a search result uses. */
const resultCard = () => {
  const card = searchPanel(stored.complete).querySelector(
    '[aria-label="Results in rank order"] > li',
  )!
  return { list: card.parentElement!.classList[0], item: card.classList[0] }
}
const diagnostic =
  "program.ts(3,7): error TS2304: Cannot find name 'invoices'.\n    const total = invoices.length\n          ^"

// Catches: an exec submission that buries its error under generic headings, prose or a
// disclosure, or that claims more than the submission itself proves.
it.each<{
  case: string
  overrides: Partial<InteractionRow>
  evidence: InteractionEvidence
  error?: string
  notice?: string
}>([
  {
    case: 'an accepted submission',
    overrides: { outcome: 'succeeded', acceptedRun: run.name },
    evidence: { kind: 'run', reference: run.name },
  },
  {
    case: 'a rejected submission',
    overrides: { outcome: 'rejected' },
    evidence: { kind: 'rejection', code: 'program_admission_failed', message: diagnostic },
    error: diagnostic,
  },
  {
    case: 'a rejected submission without a message',
    overrides: { outcome: 'rejected' },
    evidence: { kind: 'rejection', code: 'program_admission_failed', message: ' ' },
    notice: 'Rejected with no message: program_admission_failed',
  },
  {
    // The badge, the icon and the outcome row already say it; no sentence repeats it.
    case: 'a submission whose completion is unknown',
    overrides: { outcome: 'unknown', completedAt: null },
    evidence: { kind: 'unknown' },
  },
  {
    case: 'a submission whose evidence is unavailable',
    overrides: { outcome: 'succeeded' },
    evidence: { kind: 'unknown' },
    notice: 'The submission evidence is unavailable.',
  },
])('shows $case with its times and intent, and its outcome only in the badge', (row) => {
  const { container, name } = execPanel(row.overrides, row.evidence)
  const header = container.querySelector('header')!
  expect(within(header).getByRole('heading', { level: 2 }).textContent).toBe(
    'Send invoice reminders',
  )
  expect(within(header).getByText('exec submission')).toBeTruthy()
  // One list, without a heading: the times together, then the intent and the outcome.
  const lists = container.querySelectorAll('dl')
  expect(lists).toHaveLength(1)
  expect(lists[0].closest('section')!.querySelector('h4')).toBeNull()
  const completed = row.overrides.completedAt !== null
  // The title is the intent, so no Intent row repeats it.
  expect(terms(lists[0])).toEqual(['Started', ...(completed ? ['Completed'] : [])])
  expectInstant(factIn(lists[0], 'Started'), interaction.startedAt, '22 Sep 2026, 09:00:00 UTC')
  // The header badge states the submission outcome, once; no row repeats it.
  const badge = row.overrides.outcome === 'unknown' ? 'result unknown' : row.overrides.outcome!
  expect(within(header).getByText(badge)).toBeTruthy()
  expect(within(container).getAllByText(badge)).toHaveLength(1)
  expect(container.textContent).not.toContain('Submission outcome')
  for (const heading of ['Context', 'Call'])
    expect(within(container).queryByRole('heading', { name: heading })).toBeNull()
  for (const gone of ['Run accepted', 'Open execution', 'Technical details', 'Rejected:', name])
    expect(container.textContent).not.toContain(gone)
  expect(container.querySelector('details')).toBeNull()
  expect(container.querySelector('a')).toBeNull()
  // The exact error, whitespace and all, in the card a search result uses.
  const card = container.querySelector('pre')?.parentElement ?? null
  if (row.error === undefined) expect(card).toBeNull()
  else {
    const { list, item } = resultCard()
    expect(card!.textContent).toBe(row.error)
    expect(card!.querySelector('pre')!.textContent).toBe(row.error)
    expect(card!.classList).toContain(item)
    expect(card!.parentElement!.classList).toContain(list)
    expect(within(container).getByRole('region', { name: 'Error' })).toBe(card!.parentElement)
  }
  if (row.notice) expect(container.textContent).toContain(row.notice)
  if (row.overrides.outcome === 'unknown') {
    expect(within(header).getByText('result unknown')).toBeTruthy()
    expect(header).not.toHaveTextContent('incomplete')
    expect(header.querySelector('svg')).not.toBeNull()
    expect(container.textContent).not.toContain('Completion is unknown')
    expect(container.textContent).not.toContain('External effects may be uncertain')
  }
  // A submission's outcome is the submission's, not the Run's.
  expect(container.textContent).not.toMatch(/Run (succeeded|completed|accepted)/)
})

/** A retained `save_function` or `delete_function` call, selected, with `evidence` as stored. */
const functionPanel = (overrides: Partial<InteractionRow>, evidence: InteractionEvidence) => {
  const name = `${workspace}/tasks/one/interactions/${overrides.tool}`
  const container = document.createElement('div')
  container.innerHTML = markup({
    ...pendingPage,
    selected: name,
    selectedRow: null,
    detail: tool({ name, ...overrides }, evidence),
  })
  return { container, name }
}
const savedFunctionId = '0199a7c2-3b4d-7e5f-8a6b-1c2d3e4f5a6b'
const savedFunction = `${workspace}/savedFunctions/${savedFunctionId}`
const functionCalls = [
  {
    tool: 'save_function',
    subtitle: 'save function',
    intent: 'Save the reminder batch',
    // A save states no target: it retained only an ID that no page opens.
    done: null,
    noun: 'save',
    notices: false,
    code: 'saved_function_admission_failed',
    message: 'reminders.ts(1,1): error TS1128: Declaration or statement expected.\n  }\n  ^',
  },
  {
    tool: 'delete_function',
    subtitle: 'delete function',
    intent: 'Remove the old reminder batch',
    done: 'Deleted',
    noun: 'delete',
    notices: true,
    code: 'not_found',
    message: "Saved function 'billing.oldReminders' does not exist.\n  path: billing.oldReminders",
  },
]
const functionCases = functionCalls.flatMap((call) =>
  (
    [
      {
        case: `a ${call.noun} that succeeded`,
        overrides: { outcome: 'succeeded' },
        evidence: { kind: 'function', reference: savedFunction },
      },
      {
        case: `a rejected ${call.noun}`,
        overrides: { outcome: 'rejected' },
        evidence: { kind: 'rejection', code: call.code, message: call.message },
        error: call.message,
      },
      {
        case: `a rejected ${call.noun} without a message`,
        overrides: { outcome: 'rejected' },
        evidence: { kind: 'rejection', code: call.code, message: '' },
        // A save without a message shows only its badge: no fallback sentence.
        notice: call.notices ? `Rejected with no message: ${call.code}` : undefined,
      },
      {
        case: `a ${call.noun} whose completion is unknown`,
        overrides: { outcome: 'unknown', completedAt: null },
        evidence: { kind: 'unknown' },
      },
      {
        case: `a ${call.noun} whose evidence is unavailable`,
        overrides: { outcome: 'succeeded' },
        evidence: { kind: 'unknown' },
        notice: call.notices ? `The ${call.noun} evidence is unavailable.` : undefined,
      },
    ] as {
      case: string
      overrides: Partial<InteractionRow>
      evidence: InteractionEvidence
      error?: string
      notice?: string
    }[]
  ).map((row) => ({ ...row, call })),
)

// Catches: a save or delete that buries what it changed or why it failed under generic
// headings, repeats its outcome, shows its record path or workspace, or shows a function's
// documentation that the call did not retain.
it.each(functionCases)('shows $case with its intent, target, then times', (row) => {
  const { call } = row
  const { container, name } = functionPanel(
    { tool: call.tool, intent: call.intent, ...row.overrides },
    row.evidence,
  )
  const header = container.querySelector('header')!
  expect(within(header).getByRole('heading', { level: 2 }).textContent).toBe(call.intent)
  expect(within(header).getByText(call.subtitle)).toBeTruthy()
  // One list without a heading: the intent, the function it changed, then the times at the
  // end. The badge states the outcome once.
  const lists = container.querySelectorAll('dl')
  expect(lists).toHaveLength(1)
  expect(lists[0].closest('section')!.querySelector('h4')).toBeNull()
  const completed = row.overrides.completedAt !== null
  const changed = row.evidence.kind === 'function' && call.done !== null
  // The title is the intent, so no Intent row repeats it.
  expect(terms(lists[0])).toEqual([
    ...(changed ? [call.done!] : []),
    'Started',
    ...(completed ? ['Completed'] : []),
  ])
  expectInstant(factIn(lists[0], 'Started'), interaction.startedAt, '22 Sep 2026, 09:00:00 UTC')
  const badge = row.overrides.outcome === 'unknown' ? 'result unknown' : row.overrides.outcome!
  expect(within(header).getByText(badge)).toBeTruthy()
  expect(within(container).getAllByText(badge)).toHaveLength(1)
  for (const heading of ['Context', 'Call', 'Result'])
    expect(within(container).queryByRole('heading', { name: heading })).toBeNull()
  for (const gone of [
    'Submission outcome',
    'Technical details',
    'Workspace',
    'Rejected:',
    'Completion is unknown',
    name,
    savedFunction,
    'workspaces/',
  ])
    expect(container.textContent).not.toContain(gone)
  // The call kept no source, declaration or diagnostics, so nothing folds into Details.
  expect(container.querySelector('details')).toBeNull()
  // It retained only the function's ID. No route opens a function by ID, so it has no link.
  expect(container.querySelector('a')).toBeNull()
  if (changed) expect(factIn(lists[0], call.done!)!.textContent).toBe(savedFunctionId)
  else {
    expect(within(container).queryByText('Saved', { selector: 'dt' })).toBeNull()
    expect(container.textContent).not.toContain(savedFunctionId)
  }
  const { list, item } = resultCard()
  const error = container.querySelector('section[aria-label="Error"]')
  if (row.error === undefined) expect(error).toBeNull()
  else {
    const card = error!.firstElementChild!
    expect(error!.classList).toContain(list)
    expect(card.classList).toContain(item)
    expect(card.textContent).toBe(row.error)
    expect(card.querySelector('pre')!.textContent).toBe(row.error)
  }
  if (row.notice) expect(container.textContent).toContain(row.notice)
  if (!call.notices)
    for (const gone of ['Rejected with no message', 'evidence is unavailable', call.code])
      expect(container.textContent).not.toContain(gone)
})

// Catches: a rejected search that repeats its outcome as a "Rejected: code" line, hides its
// exact error, or shows results it never returned.
it.each([
  ['a one-line error', 'invalid_cursor', 'Search cursor does not match this request'],
  [
    'a multi-line error',
    'invalid_argument',
    'limit must be between 1 and 50\n  got: 500\n    at search(limit)',
  ],
])('shows a search rejected with %s in the card a result uses', (_, code, message) => {
  const name = `${workspace}/tasks/one/interactions/search`
  const container = document.createElement('div')
  container.innerHTML = markup({
    ...pendingPage,
    selected: name,
    selectedRow: null,
    detail: tool(
      { tool: 'search', name, outcome: 'rejected' },
      { kind: 'rejection', code, message },
    ),
  })
  const header = container.querySelector('header')!
  expect(within(header).getByRole('heading', { level: 2 }).textContent).toBe(interaction.intent)
  expect(within(header).getByText('search')).toBeTruthy()
  expect(within(container).getAllByText('rejected')).toHaveLength(1)
  const lists = container.querySelectorAll('dl')
  expect(lists).toHaveLength(1)
  expect(terms(lists[0])).toEqual(['Started', 'Completed'])
  for (const gone of ['Rejected:', code, 'Results', 'Raw response', 'Technical details'])
    expect(container.textContent).not.toContain(gone)
  expect(container.querySelector('details')).toBeNull()
  const error = container.querySelector('section[aria-label="Error"]')!
  const card = error.firstElementChild!
  const { list, item } = resultCard()
  expect(error.classList).toContain(list)
  expect(card.classList).toContain(item)
  expect(card.textContent).toBe(message)
  expect(card.querySelector('pre')!.textContent).toBe(message)
})

const succeeded = (finalValue: string | null): RunDetail => ({
  ...runPanel({ state: 'succeeded', endedAt: '2026-09-22T09:05:00Z', requiresAction: false }),
  terminal: { kind: 'succeeded', finalValue },
})
it.each<[string, Detail, string | null]>([
  ['a Run that returned nothing', succeeded(null), null],
  ['a Run that returned JSON null', succeeded('null'), 'null'],
  [
    'a Run that returned a large integer',
    succeeded('12345678901234567890'),
    '12345678901234567890',
  ],
  [
    "an operation call's Run that returned a value",
    { ...invocationPanel({}), context: succeeded('{"total":18450.70}') },
    '18450.70',
  ],
])(
  'shows what %s returned in a Final value card, or nothing when it returned nothing',
  (_, detail, value) => {
    const container = document.createElement('div')
    container.innerHTML = markup({ ...pendingPage, selected: null, selectedRow: null, detail })
    const card = container.querySelector('[aria-label="Final value"]')
    // No value shows no card and no sentence; JSON null is a value and has its card.
    if (value === null) expect(card).toBeNull()
    else expect(card!.querySelector('pre')!.textContent).toContain(value)
    expect(container.textContent).not.toContain('returned no final value')
    expect(within(container).queryByText('Final value', { selector: 'summary' })).toBeNull()
  },
)

const owner = `${workspace}/tasks/one`
it.each<[string, Detail, ActivityRow | null]>([
  ['a Run', runPanel({ task: owner }), null],
  ['an operation call', invocationPanel({ task: owner }), null],
  [
    'a tool call',
    tool({ tool: 'exec' }, { kind: 'run', reference: run.name }),
    { ...interaction, kind: 'tool', workspace, task: owner },
  ],
  [
    'an output record',
    event({ ...at, kind: 'program.emitted', value: '{}' }),
    {
      kind: 'output',
      name: `${run.name}/events/4`,
      workspace,
      run: run.name,
      label: 'emission #4',
      startedAt: run.startedAt,
      eventKind: 'program.emitted',
      agent: '',
      task: owner,
    },
  ],
])('offers no parent navigation from %s', (_, detail, selectedRow) => {
  const container = document.createElement('div')
  container.innerHTML = markup({ ...pendingPage, selected: null, selectedRow, detail })
  expect(container.textContent).not.toContain('Open task')
  expect(container.textContent).not.toContain('Open parent execution')
})

it('states a Run end once, in its title, not again in the body', () => {
  const container = document.createElement('div')
  container.innerHTML = markup({
    ...pendingPage,
    selected: null,
    selectedRow: null,
    detail: event({ ...at, kind: 'run.ended', state: 'timed_out' }),
  })
  expect(container.textContent).not.toContain('Run state')
  expect(container.textContent!.match(/timed.out/g)).toHaveLength(1)
})

const outputOf = (value: RunEvent): ActivityRow => ({
  kind: 'output',
  name: `${run.name}/events/${value.sequence}`,
  workspace,
  run: run.name,
  label: `record #${value.sequence}`,
  startedAt: value.occurredAt,
  eventKind: value.kind,
  agent: '',
})
it.each<[string, RunEvent, boolean, string, string[], string | null]>([
  [
    'a warning log',
    { ...at, kind: 'program.logged', level: 'warn', message: 'Fetched page 2\n  then page 3' },
    true,
    'Log message',
    ['Fetched page 2\n  then page 3'],
    'warn',
  ],
  [
    'a log at a level this Reef does not know',
    { ...at, kind: 'program.logged', level: 'unknown', message: 'Retained as sent' },
    true,
    'Log message',
    ['Retained as sent'],
    'unknown',
  ],
  [
    'an object emission',
    { ...at, kind: 'program.emitted', value: '{"amount":1.50,"note":"  spaced"}' },
    true,
    'Emitted value',
    ['1.50', '"  spaced"'],
    null,
  ],
  [
    'a scalar emission reached without its row',
    { ...at, kind: 'program.emitted', value: '"done"' },
    false,
    'Emitted value',
    ['"done"'],
    null,
  ],
])(
  'shows %s under its call title with one readable time and only its exact content in a card',
  (_, value, withRow, card, content, level) => {
    const container = document.createElement('div')
    container.innerHTML = markup({
      ...pendingPage,
      selected: null,
      selectedRow: withRow ? outputOf(value) : null,
      detail: event(value),
    })
    // The title names the call, level and record; the facts list keeps only the time.
    const title = level ? `console.${level} #4` : 'coral.emit #4'
    expect(within(container).getByRole('heading', { level: 2 }).textContent).toBe(title)
    // The title and subtitle say what it is; no status badge repeats it, the icon stays.
    const header = container.querySelector('header')!
    expect(header.textContent).not.toMatch(/diagnostic|retained/)
    expect(header.querySelector('svg')).not.toBeNull()
    expect(within(container).queryByRole('heading', { name: 'Context' })).toBeNull()
    const context = container.querySelector('dl')!
    expect(
      within(context)
        .getAllByRole('term')
        .map((term) => term.textContent),
    ).toEqual(['Started'])
    const fact = (term: string) =>
      within(context).getByText(term, { selector: 'dt' }).nextElementSibling
    expectInstant(fact('Started'), value.occurredAt, '22 Sep 2026, 09:00:00 UTC')
    for (const gone of [
      'record #',
      'This log is not a user-facing result',
      'Diagnostic log',
      'Technical details',
    ])
      expect(container.textContent).not.toContain(gone)
    // The card is named for assistive technology but shows only the payload.
    const body = container.querySelector<HTMLElement>(`[aria-label="${card}"]`)!
    const code = body.querySelector('pre')!
    expect(body.textContent).toBe(code.textContent)
    for (const text of content) expect(code.textContent).toContain(text)
  },
)

// Catches: a run event that loses its time when no row is selected, or shows it raw.
it('states another run event its time once, in its list', () => {
  const container = document.createElement('div')
  container.innerHTML = markup({
    ...pendingPage,
    selected: null,
    selectedRow: null,
    detail: event({ ...at, kind: 'run.started' }),
  })
  const [list] = container.querySelectorAll('dl')
  expectInstant(factIn(list, 'Started'), at.occurredAt, '22 Sep 2026, 09:00:00 UTC')
  expect(container.textContent).not.toContain('record #')
})

it('keeps the whole log message and emitted value in the body', () => {
  const container = document.createElement('div')
  for (const detail of [
    event({
      ...at,
      kind: 'program.logged',
      level: 'info',
      message: 'Fetched 3 invoices\nNext page',
    }),
    event({ ...at, kind: 'program.emitted', value: '{"intent":"misleading"}' }),
  ]) {
    container.innerHTML = markup({ ...pendingPage, selected: null, selectedRow: null, detail })
    expect(container.textContent).toContain(
      detail.event.kind === 'program.logged' ? 'Next page' : '"misleading"',
    )
  }
})

it('keeps no record path for another run event, but its header badge', () => {
  const name = `${run.name}/events/7`
  const container = document.createElement('div')
  container.innerHTML = markup({
    ...pendingPage,
    selected: name,
    selectedRow: null,
    detail: {
      kind: 'output',
      run: run.name,
      event: { kind: 'run.started', sequence: '7', occurredAt: run.startedAt },
    },
  })
  expect(container.textContent).not.toContain('Technical details')
  expect(container.textContent).not.toContain(name)
  // A run event keeps its icon, but no badge: "retained" says nothing about it.
  expect(container.querySelector('header svg')).not.toBeNull()
  expect(container.querySelector('header')!.textContent).toBe('Program run startedrun event')
})

const runActions = () => screen.getByRole('region', { name: 'Run actions' })

// Catches: a Run's action list that loses an action's identity, its exact review, its
// deadline or the next page, or that stops using the cards a search result uses.
it('lists what the Run waits on as cards, each opening its exact review', () => {
  const approval = {
    kind: 'approval' as const,
    name: `${workspace}/approvals/send`,
    callPath: 'coral.providers.mail.send',
    providerLabel: 'Mail',
    deadline: '2030-09-22T09:15:00Z',
  }
  const connection = {
    kind: 'account_connection' as const,
    name: `${workspace}/accountConnections/linear`,
    callPath: '',
    providerLabel: 'Linear',
    deadline: '2030-09-22T09:20:00Z',
  }
  const waiting = {
    ...run,
    requiresAction: true,
    actionPage: { total: '2', next: '', actions: [approval, connection] },
  }
  renderRoute(
    <ActionReviewSession>
      <ActivityDetail
        page={{
          ...pendingPage,
          selected: waiting.name,
          selectedRow: waiting,
          detail: {
            kind: 'run',
            run: waiting,
            request: null,
            source: 'return null;',
            linkedFunctions: [],
            terminal: { kind: 'unavailable' },
          },
        }}
      />
    </ActionReviewSession>,
    { entry: '/activity' },
  )
  const nav = within(runActions()).getByRole('navigation', { name: 'Actions in this run' })
  // The count is the Run's total, and the list holds every one of them.
  expect(within(nav).getByText('2 actions need you.')).toBeInTheDocument()
  const list = within(nav).getByRole('list')
  const items = within(list).getAllByRole('listitem')
  expect(items).toHaveLength(2)
  const { list: listClass, item: itemClass } = resultCard()
  expect(list.classList).toContain(listClass)
  for (const item of items) expect(item.classList).toContain(itemClass)
  const [first, second] = items
  // An approval names its exact call path; a connection names its provider account.
  expect(first.querySelector('code')!.textContent).toBe(approval.callPath)
  expect(within(first).getByText('approval')).toBeInTheDocument()
  expect(within(second).getByText('Connect your Linear account')).toBeInTheDocument()
  expect(within(second).getByText('account connection')).toBeInTheDocument()
  for (const [item, action, label] of [
    [first, approval, 'Review coral.providers.mail.send'],
    [second, connection, 'Connect your Linear account'],
  ] as const) {
    const link = within(item).getByRole('link', { name: label })
    expect(new URL(link.getAttribute('href')!, 'http://reef').searchParams.get('request')).toBe(
      action.name,
    )
    expect(item.querySelector('time')).toHaveAttribute('dateTime', action.deadline)
  }
  // Two actions fit, so the list is whole: nothing shows more or links to another page.
  expect(within(nav).queryByRole('link', { name: /actions in this run|First actions/ })).toBeNull()
  expect(within(nav).queryByRole('button', { name: 'Show more actions' })).toBeNull()
  expect(within(nav).queryByText(/^Showing/)).toBeNull()
  expect(within(nav).queryByRole('status')).toBeNull()
})

// Catches: a list that hides the actions a failed page left out, or claims to be whole.
it('says how many actions loaded when a page of them failed, and offers to try again', () => {
  const waiting = {
    ...run,
    requiresAction: true,
    actionPage: {
      total: '150',
      next: '',
      incomplete: true,
      actions: Array.from({ length: 100 }, (_, index) => ({
        kind: 'approval' as const,
        name: `${workspace}/approvals/action-${index}`,
        callPath: `coral.providers.mail.send${index}`,
        providerLabel: 'Mail',
        deadline: '2030-09-22T09:15:00Z',
      })),
    },
  }
  renderRoute(
    <ActionReviewSession>
      <ActivityDetail
        page={{
          ...pendingPage,
          selected: waiting.name,
          selectedRow: waiting,
          detail: {
            kind: 'run',
            run: waiting,
            request: null,
            source: 'return null;',
            linkedFunctions: [],
            terminal: { kind: 'unavailable' },
          },
        }}
      />
    </ActionReviewSession>,
    { entry: '/activity' },
  )
  const nav = within(runActions()).getByRole('navigation', { name: 'Actions in this run' })
  expect(within(nav).getByText('150 actions need you.')).toBeInTheDocument()
  expect(within(within(nav).getByRole('list')).getAllByRole('listitem')).toHaveLength(5)
  expect(within(nav).getByRole('status')).toHaveTextContent(
    '100 of 150 actions loaded. The rest did not load.',
  )
  expect(within(nav).getByRole('button', { name: 'Try again' })).toBeInTheDocument()
})

// Helpers for a Run that waits on more than five actions.
const actionAt = (index: number) => ({
  kind: 'approval' as const,
  name: `${workspace}/approvals/action-${index}`,
  callPath: `coral.providers.mail.send${index}`,
  providerLabel: 'Mail',
  deadline: '2030-09-22T09:15:00Z',
})
const actions = (count: number) => Array.from({ length: count }, (_, index) => actionAt(index))
function showActions(
  actionPage: ActionPage,
  {
    request = null,
    query = 'request=stale',
  }: { request?: RequestDetail | null; query?: string } = {},
) {
  const waiting = { ...run, requiresAction: true, actionPage }
  renderRoute(
    <ActionReviewSession>
      <ActivityDetail
        page={{
          ...pendingPage,
          selected: waiting.name,
          selectedRow: waiting,
          detail: {
            kind: 'run',
            run: waiting,
            request,
            source: 'return null;',
            linkedFunctions: [],
            terminal: { kind: 'unavailable' },
          },
        }}
      />
    </ActionReviewSession>,
    { entry: `/activity?record=${encodeURIComponent(run.name)}&${query}` },
  )
  return screen.queryByRole('navigation', { name: 'Actions in this run' })
}
const listedActions = (nav: HTMLElement) =>
  within(within(nav).getByRole('list'))
    .getAllByRole('listitem')
    .map((item) => item.querySelector('code')!.textContent)
const linkQuery = (link: HTMLElement) =>
  new URL(link.getAttribute('href')!, 'http://reef').searchParams

describe('a Run that waits on more than five actions', () => {
  // Catches: a list that shows every action at once, loses Coral's order, or claims to be whole.
  it.each([
    { loaded: 0, total: '0', shown: null, more: false },
    { loaded: 1, total: '1', shown: null, more: false },
    { loaded: 5, total: '5', shown: null, more: false },
    { loaded: 6, total: '6', shown: 'Showing 5 of 6 actions.', more: true },
    { loaded: 100, total: '150', shown: 'Showing 5 of 150 actions.', more: true },
    // Coral counted an action Reef cannot open: the count stays exact and nothing offers it.
    { loaded: 5, total: '6', shown: 'Showing 5 of 6 actions.', more: false },
  ])(
    'shows the first five of $loaded loaded actions of $total',
    ({ loaded, total, shown, more }) => {
      const nav = showActions({ actions: actions(loaded), total, next: '' })
      if (total === '0') return expect(nav).toBeNull()
      expect(
        within(nav!).getByText(`${total} ${total === '1' ? 'action needs' : 'actions need'} you.`),
      ).toBeInTheDocument()
      expect(listedActions(nav!)).toEqual(actions(Math.min(loaded, 5)).map((item) => item.callPath))
      if (shown) expect(within(nav!).getByText(shown)).toBeInTheDocument()
      else expect(within(nav!).queryByText(/^Showing/)).toBeNull()
      expect(Boolean(within(nav!).queryByRole('button', { name: 'Show more actions' }))).toBe(more)
    },
  )

  // Catches: remaining actions that no control reaches, or a reveal that reads Coral again.
  it('shows the rest of the page on request, then links to the next page', async () => {
    const nav = showActions({ actions: actions(100), total: '150', next: 'cursor-1' })!
    // The next page waits until the whole first page is on screen.
    expect(within(nav).queryByRole('link', { name: 'More actions in this run' })).toBeNull()
    const more = within(nav).getByRole('button', { name: 'Show more actions' })
    expect(more).toHaveAttribute('aria-expanded', 'false')
    await userEvent.click(more)
    expect(listedActions(nav)).toEqual(actions(100).map((item) => item.callPath))
    expect(within(nav).getByRole('button', { name: 'Show fewer actions' })).toHaveAttribute(
      'aria-expanded',
      'true',
    )
    expect(within(nav).getByText('Showing 100 of 150 actions.')).toBeInTheDocument()
    const next = linkQuery(within(nav).getByRole('link', { name: 'More actions in this run' }))
    expect(next.get('actionsAfter')).toBe('cursor-1')
    expect(next.get('record')).toBe(run.name)
    // The next page lists the Run's actions; it does not reopen a selected review.
    expect(next.has('request')).toBe(false)
    expect(within(nav).queryByRole('link', { name: 'First actions' })).toBeNull()
  })

  // Catches: a later page that reads as the start of the list, or has no way back.
  it('names a later page as later, and links back to the first actions', () => {
    const nav = showActions({
      actions: actions(3),
      total: '150',
      next: '',
      after: 'cursor-1',
    })!
    expect(listedActions(nav)).toEqual(actions(3).map((item) => item.callPath))
    expect(within(nav).getByText('Showing 3 later actions of 150.')).toBeInTheDocument()
    expect(
      linkQuery(within(nav).getByRole('link', { name: 'First actions' })).has('actionsAfter'),
    ).toBe(false)
    expect(within(nav).queryByRole('link', { name: 'More actions in this run' })).toBeNull()
  })

  // A later page that holds a pending approval and a connection, as a "More actions" link opens it.
  const laterConnection: Extract<RequestDetail, { kind: 'account_connection' }> = {
    kind: 'account_connection',
    name: `${workspace}/accountConnections/later`,
    provider: `${workspace}/providers/linear`,
    state: 'pending',
    expiresAt: '2030-09-22T09:15:00Z',
  }
  const laterPage: ActionPage = {
    actions: [
      {
        kind: 'approval',
        name: approvalRequest.name,
        callPath: approvalRequest.callPath,
        providerLabel: 'Mail',
        deadline: '2030-09-22T09:15:00Z',
      },
      {
        kind: 'account_connection',
        name: laterConnection.name,
        callPath: '',
        providerLabel: 'Linear',
        deadline: '2030-09-22T09:15:00Z',
      },
    ],
    total: '102',
    next: '',
    after: 'cursor-1',
  }

  // Catches: a review link that drops the page it came from, so the review reads the first page.
  it('opens each review on a later page with that page still named', () => {
    const nav = showActions(laterPage, { query: 'actionsAfter=cursor-1' })!
    const links = within(within(nav).getByRole('list')).getAllByRole('link')
    expect(links.map((link) => linkQuery(link).get('request'))).toEqual(
      laterPage.actions.map((item) => item.name),
    )
    for (const link of links) expect(linkQuery(link).get('actionsAfter')).toBe('cursor-1')
  })

  // Catches: a review from a later page that lists the Run's actions under it, or a connection
  // that loses its provider's name.
  it.each([
    ['an approval', approvalRequest, `Review ${approvalRequest.callPath}`],
    ['a connection', laterConnection, 'Connect your Linear account'],
  ] as const)('reviews %s from a later page as its own request', (_kind, request, name) => {
    const nav = showActions(laterPage, {
      request,
      query: `actionsAfter=cursor-1&request=${encodeURIComponent(request.name)}`,
    })
    expect(nav).toBeNull()
    if (request.kind === 'approval')
      expect(screen.getByRole('region', { name })).toContainElement(
        screen.getByRole('button', { name: 'Approve this call' }),
      )
    else {
      expect(screen.getByRole('heading', { name })).toBeInTheDocument()
      expect(screen.getByRole('button', { name: 'Connect Linear' })).toBeInTheDocument()
    }
  })
})

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
    {
      connected = true,
      decline,
      ended,
    }: {
      connected?: boolean
      decline?: (form: FormData) => void
      /** How the Run ended after the request settled. */
      ended?: Extract<RunDetail['terminal'], { kind: 'succeeded' | 'failed' }>
    } = {},
  ) {
    const settled = request.state !== 'pending'
    const selected = settled
      ? {
          ...waiting,
          actionPage: { total: '0', next: '', actions: [] },
          ...(ended && { state: ended.kind, endedAt: '2026-09-22T09:05:00Z' }),
        }
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
                terminal: ended ?? { kind: 'unavailable' },
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

  it('names the shared connection, not one call, and keeps Connect and Decline in its footer', () => {
    renderConnection(connection)
    expect(
      within(runActions()).getByRole('heading', { level: 3, name: 'Connect your Linear account' }),
    ).toBeInTheDocument()
    expect(within(runActions()).getByText('account connection request')).toBeInTheDocument()
    expect(within(runActions()).queryByText(/Approve|Deny/)).not.toBeInTheDocument()
    const footer = screen.getByRole('button', { name: 'Decline' }).closest('footer')!
    expect(within(footer).getByRole('button', { name: 'Connect Linear' })).toBeInTheDocument()
  })

  // Catches: a selected connection request under the Run's own header, with Run controls or
  // prose it does not need.
  it.each(['pending', 'declined', 'expired', 'connected'] as const)(
    'leads a %s connection request with its own header, then the Run as context',
    (state) => {
      renderConnection({ ...connection, state })
      // One header, the request's: no program run header, title or badge.
      const headers = document.querySelectorAll('header')
      expect(headers).toHaveLength(1)
      const header = headers[0]
      // A settled request has left the pending list that names its provider.
      expect(within(header).getByRole('heading', { level: 3 }).textContent).toBe(
        state === 'pending' ? 'Connect your Linear account' : 'Connect your account',
      )
      expect(within(header).getByText('account connection request')).toBeInTheDocument()
      expect(within(header).getByText(state)).toBeInTheDocument()
      expect(screen.queryByRole('heading', { level: 2 })).not.toBeInTheDocument()
      expect(document.body).not.toHaveTextContent('program run')
      // A pending request's deadline reads in the header, as an approval's does.
      const deadline = header.querySelector('time')
      if (state === 'pending') expect(deadline).toHaveAttribute('dateTime', connection.expiresAt)
      else expect(deadline).toBeNull()
      // The Run follows as context, below the header, its times labelled as the Run's.
      const list = document.querySelector('dl')!
      expect(header.compareDocumentPosition(list) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
      expect(terms(list)).toEqual(['Run intent', 'Requested by', 'Run duration', 'Run started'])
      expect(factIn(list, 'Run intent')).toHaveTextContent(run.intent!)
      expect(factIn(list, 'Requested by')).toHaveTextContent('Collections agent · Finance')
      expectInstant(factIn(list, 'Run started'), run.startedAt, '22 Sep 2026, 09:00:00 UTC')
      // No Run cancel and no extra sentence about several calls.
      expect(screen.queryByRole('button', { name: 'Cancel run' })).not.toBeInTheDocument()
      expect(document.body).not.toHaveTextContent('One connection can unblock')
      if (state === 'pending') {
        expect(document.body).toHaveTextContent('Connect your Linear account to continue.')
        const footer = screen.getByRole('button', { name: 'Decline' }).closest('footer')!
        expect(within(footer).getByRole('button', { name: 'Connect Linear' })).toBeInTheDocument()
        expect(footer.querySelector('time')).toBeNull()
      } else expect(within(runActions()).getByRole('status')).toBeInTheDocument()
      expect(document.body.textContent).not.toMatch(/Approve|Deny|Arguments/)
    },
  )

  it('keeps Cancel run on the Run itself when no request is selected', () => {
    renderRoute(
      <ActionReviewSession>
        <ActivityDetail
          page={{
            ...pendingPage,
            selected: waiting.name,
            selectedRow: waiting,
            detail: {
              kind: 'run',
              run: waiting,
              request: null,
              source: 'return null;',
              linkedFunctions: [],
              terminal: { kind: 'unavailable' },
            },
          }}
        />
      </ActionReviewSession>,
      { entry: '/activity' },
    )
    expect(screen.getByRole('heading', { level: 2 })).toHaveTextContent(run.intent!)
    expect(screen.getByRole('button', { name: 'Cancel run' })).toBeInTheDocument()
    expect(
      within(runActions()).getByRole('link', { name: 'Connect your Linear account' }),
    ).toBeInTheDocument()
  })

  // Catches: a settled connection showing its Run's final value or failure, where they read as
  // the request's own outcome.
  it.each([
    [
      'succeeded',
      { kind: 'succeeded', finalValue: '{"exact-run-final-value":1}' },
      /exact-run-final-value/,
    ],
    [
      'failed',
      { kind: 'failed', code: 'program_rejected', message: 'exact-run-failure', error: null },
      /exact-run-failure|program_rejected/,
    ],
  ] as const)(
    'keeps a connection on a Run that %s to the Run state, without its outcome',
    (state, ended, outcome) => {
      renderConnection({ ...connection, state: 'connected' }, { ended })
      expect(document.body).toHaveTextContent(`program run: ${state}`)
      expect(screen.queryByRole('region', { name: 'Final value' })).toBeNull()
      expect(screen.queryByRole('region', { name: 'Error' })).toBeNull()
      expect(document.body.textContent).not.toMatch(outcome)
    },
  )

  it('shows a connection reached from one call as retained context, with nothing to act on', () => {
    const call: InvocationRow = {
      ...run,
      name: `${workspace}/invocations/waiting`,
      run: run.name,
      state: 'pending',
      outcome: 'unknown',
      dispatchedAt: null,
      waitingNanos: '0',
      provider: connection.provider,
      operation: 'list',
      callPath: 'coral.providers.linear.list',
    }
    renderRoute(
      <ActionReviewSession>
        <ActivityDetail
          page={{
            ...pendingPage,
            selected: call.name,
            selectedRow: { ...call, kind: 'invocation' },
            detail: {
              kind: 'invocation',
              invocation: call,
              arguments: '{}',
              result: { kind: 'unknown' },
              // The same connection, another approval, and a reference that no longer resolves.
              requests: [
                { kind: 'account_connection', name: connection.name },
                { kind: 'approval', name: `${workspace}/approvals/other` },
                { kind: 'unknown' },
              ],
              request: connection,
              requestUnavailable: false,
              context: {
                kind: 'run',
                run: waiting,
                source: 'return null;',
                linkedFunctions: [],
                terminal: { kind: 'unavailable' },
              },
            },
          }}
        />
      </ActionReviewSession>,
      { entry: '/activity' },
    )
    expect(screen.getByRole('heading', { level: 2 })).toHaveTextContent(call.callPath)
    // The retained connection shows in a card with its state and deadline, not its raw ID.
    const card = screen.getByRole('region', { name: 'Account connection' })
    expect(card).toHaveTextContent('connection request: pending')
    expect(card.querySelector('time')).toHaveAttribute('dateTime', connection.expiresAt)
    expect(document.body).not.toHaveTextContent(connection.name)
    expect(screen.queryByRole('button', { name: /Connect|Decline/ })).not.toBeInTheDocument()
    // The call, its Run and the retained connection each keep their own state, named apart.
    expect(screen.queryByText('State', { selector: 'dt' })).not.toBeInTheDocument()
    expect(within(document.querySelector('header')!).getByText('pending')).toBeInTheDocument()
    expect(screen.getByText('connection request: pending')).toBeInTheDocument()
    expect(screen.getByText('program run: running')).toBeInTheDocument()
    expect(document.body).not.toHaveTextContent('Run state')
    // The whole card is the one link: it opens the connection on its Run, where you act on it.
    const open = card.querySelector('a')!
    expect(open).toBe(card.firstElementChild)
    expect(open).toHaveAccessibleName('Open account connection request: pending')
    expect(open).toHaveTextContent('connection request: pending')
    expect(open.querySelector('a, button, svg')).toBeNull()
    const target = new URL(open.getAttribute('href')!, 'http://reef')
    expect(target.searchParams.get('record')).toBe(run.name)
    expect(target.searchParams.get('request')).toBe(connection.name)
    // No second link for the same request; other requests keep theirs.
    expect(screen.queryByRole('link', { name: 'Inspect account connection' })).toBeNull()
    expect(screen.getAllByRole('link', { name: /account connection/ })).toEqual([open])
    const approval = screen.getByRole('link', { name: 'Inspect approval' })
    expect(new URL(approval.getAttribute('href')!, 'http://reef').searchParams.get('request')).toBe(
      `${workspace}/approvals/other`,
    )
    expect(document.body).toHaveTextContent('This request reference is unavailable.')
  })

  // Catches: a connection link that reopens the same call instead of the request you act on.
  it("links a call's connection to its Run, where you can act on it", () => {
    const call: InvocationRow = {
      ...run,
      name: `${workspace}/invocations/waiting`,
      run: run.name,
      state: 'pending',
      outcome: 'unknown',
      dispatchedAt: null,
      waitingNanos: '0',
      provider: connection.provider,
      operation: 'list',
      callPath: 'coral.providers.linear.list',
    }
    renderRoute(
      <ActionReviewSession>
        <ActivityDetail
          page={{
            ...pendingPage,
            selected: call.name,
            selectedRow: { ...call, kind: 'invocation' },
            detail: {
              kind: 'invocation',
              invocation: call,
              arguments: '{}',
              result: { kind: 'unknown' },
              requests: [{ kind: 'account_connection', name: connection.name }],
              request: null,
              requestUnavailable: false,
            },
          }}
        />
      </ActionReviewSession>,
      { entry: '/activity' },
    )
    const link = screen.getByRole('link', { name: 'Inspect account connection' })
    const target = new URL(link.getAttribute('href')!, 'http://reef')
    expect(target.searchParams.get('record')).toBe(run.name)
    expect(target.searchParams.get('request')).toBe(connection.name)
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

/** One record's panel, as static markup in a container. */
const show = (detail: Detail, selected: string | null = null) => {
  const container = document.createElement('div')
  container.innerHTML = markup({ ...pendingPage, selected, selectedRow: null, detail })
  return container
}
// Catches: an approval review that adds its Run's state, failure or final value under its own
// decision, where they read as this call's.
const approvalStates = [
  ['pending', { state: 'pending', actionable: true }, 'Approval required'],
  [
    'approved',
    {
      state: 'approved',
      actionable: false,
      approver: 'owner',
      approverLabel: 'You',
      decidedAt: '2026-09-22T09:01:00Z',
      comment: 'Fine.',
    },
    'Approved',
  ],
  [
    'denied',
    {
      state: 'declined',
      actionable: false,
      approver: 'owner',
      approverLabel: 'You',
      decidedAt: '2026-09-22T09:01:00Z',
      comment: 'Wrong recipient.',
    },
    'Denied',
  ],
  ['expired', { state: 'expired', actionable: false }, 'Expired'],
  ['closed', { state: 'closed', actionable: false }, 'Closed'],
] as const
const runOutcomes = [
  {
    kind: 'failed',
    code: 'program_execution_failed',
    message: 'exact-run-failure',
    error: null,
  },
  { kind: 'succeeded', finalValue: '{"exact-run-final-value":1}' },
] as const
it.each(
  approvalStates.flatMap(([state, overrides, label]) =>
    runOutcomes.map((terminal) => [state, terminal.kind, overrides, label, terminal] as const),
  ),
)(
  'shows a %s approval on a Run that %s with its own status and none of the Run outcome',
  (state, _, overrides, label, terminal) => {
    const pending = state === 'pending'
    const container = show(
      {
        ...runPanel(
          pending
            ? { ...run, state: 'running', endedAt: null, requiresAction: true }
            : {
                ...run,
                state: terminal.kind,
                endedAt: '2026-09-22T09:05:00Z',
                requiresAction: false,
              },
        ),
        request: { ...approvalRequest, ...overrides },
        terminal: pending ? { kind: 'unavailable' } : terminal,
      } as RunDetail,
      run.name,
    )
    const review = within(container).getByRole('region', { name: /^Review / })
    expect(within(review.querySelector('header')!).getByText(label)).toBeTruthy()
    expect(container.textContent).not.toContain('program run:')
    if ('comment' in overrides) {
      const decision = within(container).getByText('Reviewed by', { selector: 'dt' }).closest('dl')!
      expect(terms(decision)).toEqual(['Reviewed by', 'Decided', 'Comment'])
      expect(factIn(decision, 'Comment')).toHaveTextContent(overrides.comment)
    }
    // The Run's outcome belongs to the Run's own panel.
    expect(within(container).queryByRole('region', { name: 'Error' })).toBeNull()
    expect(within(container).queryByRole('region', { name: 'Final value' })).toBeNull()
    expect(container.textContent).not.toMatch(/exact-run-failure|exact-run-final-value/)
  },
)

// Catches: a cancellation that Coral holds offered again, or its status outliving the Run.
it('says the run is canceling until the live read shows it ended', async () => {
  const running = { ...run, requiresAction: false, actionPage: undefined }
  const pageOf = (current: typeof running, terminal: RunDetail['terminal']): ActivityPage => ({
    ...pendingPage,
    selected: current.name,
    selectedRow: current,
    detail: {
      kind: 'run',
      run: current,
      request: null,
      source: 'return null;',
      linkedFunctions: [],
      terminal,
    },
  })
  const { update } = renderLivePage(
    (page) => (
      <ActionReviewSession>
        <ActivityDetail page={page} />
      </ActionReviewSession>
    ),
    pageOf(running, { kind: 'unavailable' }),
    {
      entry: '/activity',
      routes: [{ path: '/activity/resolve', action: () => ({ recording: true }) }],
    },
  )
  const user = userEvent.setup()
  await user.click(screen.getByRole('button', { name: 'Cancel run' }))
  await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Cancel run' }))

  expect(await screen.findByText('Canceling the run…')).toHaveAttribute('role', 'status')
  expect(screen.getByRole('button', { name: 'Cancel run' })).toBeDisabled()

  update(
    pageOf(
      { ...running, state: 'canceled', endedAt: '2026-09-22T09:05:00Z' },
      { kind: 'canceled' },
    ),
  )
  expect(screen.queryByText('Canceling the run…')).toBeNull()
})

// Catches: a pinned decision that leaves the named review, so navigating by region skips it.
it('keeps the pinned Deny and Approve inside the one named review region', () => {
  const pending = { ...run, state: 'running' as const, endedAt: null, requiresAction: true }
  renderRoute(
    <ActionReviewSession>
      <ActivityDetail
        page={{
          ...pendingPage,
          selected: run.name,
          selectedRow: pending,
          detail: { ...runPanel(pending), request: approvalRequest },
        }}
      />
    </ActionReviewSession>,
    { entry: '/activity' },
  )
  const regions = screen.getAllByRole('region', { name: `Review ${approvalRequest.callPath}` })
  expect(regions).toHaveLength(1)
  const [review] = regions
  const approve = within(review).getByRole('button', { name: 'Approve this call' })
  const deny = within(review).getByRole('button', { name: 'Deny this call' })
  // Still the drawer's one pinned footer, at the review's end, after the call's outcome.
  const footer = approve.closest('footer')!
  expect(footer).toContainElement(deny)
  expect(footer).toBe(review.lastElementChild)
  // The buttons submit the review's own form.
  const form = approve.getAttribute('form')!
  expect(review.querySelector(`form[id="${form}"]`)).not.toBeNull()
})

describe('each record states only what helps, once', () => {
  const scaffolding = ['Technical details', 'Context', 'Call', 'Run evidence', 'Retained request']
  const expectNoScaffolding = (container: HTMLElement) => {
    for (const heading of scaffolding)
      expect(within(container).queryByRole('heading', { name: heading })).toBeNull()
    expect(container.textContent).not.toContain('Technical details')
  }
  const ended = { state: 'failed' as const, endedAt: '2026-09-22T09:05:00Z' }

  // Catches: a Run that pads its panel with sentences about evidence it does not have, repeats
  // its badge, or folds a failure away; and a no-value that reads like a JSON null.
  it.each<[string, RunDetail['terminal'], Partial<typeof run>, string[], string[]]>([
    [
      'returned no value',
      { kind: 'succeeded', finalValue: null },
      { state: 'succeeded' },
      [],
      ['returned no final value'],
    ],
    [
      'returned JSON null',
      { kind: 'succeeded', finalValue: 'null' },
      { state: 'succeeded' },
      ['null'],
      [],
    ],
    ['was canceled', { kind: 'canceled' }, { state: 'canceled' }, [], ['Terminal outcome']],
    ['timed out', { kind: 'timed_out' }, { state: 'timed_out' }, [], ['Terminal outcome']],
    [
      'ended without terminal evidence',
      { kind: 'unavailable' },
      { state: 'interrupted' },
      [],
      ['Terminal evidence is unavailable'],
    ],
    [
      'failed',
      {
        kind: 'failed',
        code: 'program_execution_failed',
        message: 'TypeError: invoices is not iterable\n    at main (program.ts:3:7)',
        error: {
          name: 'TypeError',
          message: 'invoices is not iterable',
          frames: [{ source: 'program.ts', line: 3, column: 7 }],
          operationCode: '',
          operationTraceId: '',
          operationStatus: 0,
          operationBody: '',
          operationBodyTruncated: false,
          truncated: false,
          circular: false,
          causeOmitted: false,
          omittedErrors: 0,
          cause: null,
          errors: [],
        },
      },
      {},
      [
        'program_execution_failed',
        'TypeError: invoices is not iterable\n    at main (program.ts:3:7)',
        'program.ts:3:7',
      ],
      ['Failure details'],
    ],
  ])('shows a Run that %s with only its own evidence', (_, terminal, overrides, shown, gone) => {
    const container = show({
      ...runPanel({ ...ended, ...overrides }),
      terminal,
    } as RunDetail)
    const lists = container.querySelectorAll('dl')
    expect(lists).toHaveLength(1)
    // Its times close the list.
    expect(terms(lists[0])).toEqual(['Duration', 'Started', 'Ended'])
    expectNoScaffolding(container)
    for (const text of gone) expect(container.textContent).not.toContain(text)
    // A failure is never folded away.
    expect(container.querySelector('details')).toBeNull()
    const card = container.querySelector<HTMLElement>(
      '[aria-label="Final value"], [aria-label="Error"]',
    )
    if (shown.length === 0) expect(card).toBeNull()
    for (const text of shown) expect(card!.textContent).toContain(text)
  })

  // Catches: an operation call that buries its controls, shows raw IDs, or hides its error.
  it.each<[string, InvocationDetail['result'], string[], string[]]>([
    [
      'a Coral failure',
      { kind: 'coral_failure', code: 'operation_disabled', message: 'The operation is disabled.' },
      ['Coral failure: operation_disabled', 'The operation is disabled.'],
      [],
    ],
    [
      'an upstream result',
      { kind: 'upstream_outcome', value: '{"status":200}' },
      ['"status": 200'],
      ['HTTP status and MCP isError'],
    ],
    ['no retained result', { kind: 'unknown' }, [], ['No retained outcome']],
    [
      'an incomplete dispatch',
      { kind: 'indeterminate' },
      ['External effects may be uncertain.'],
      [],
    ],
  ])(
    'shows an operation call with %s, its controls in view and no raw IDs',
    (_, result, shown, gone) => {
      const live = { ...run, state: 'running' as const, endedAt: null }
      const detail: InvocationDetail = {
        ...invocationPanel({}),
        result,
        context: {
          ...runPanel(live, {
            ...interaction,
            tool: 'exec',
            name: `${workspace}/tasks/one/interactions/exec`,
          }),
          source: 'exact-accepted-source',
        },
      }
      const container = show(detail)
      expectNoScaffolding(container)
      for (const id of [detail.invocation.name, run.name])
        expect(container.textContent).not.toContain(id)
      for (const text of [...gone, 'Run ID', 'Invocation ID', 'Whole-run controls'])
        expect(container.textContent).not.toContain(text)
      // One list, its times last.
      const lists = [...container.querySelectorAll('dl')].filter((list) => !list.closest('details'))
      expect(lists).toHaveLength(1)
      const times = ['Duration', 'Started', ...(detail.invocation.endedAt ? ['Ended'] : [])]
      expect(terms(lists[0]).slice(-times.length)).toEqual(times)
      // The whole-run controls stay in view; the accepted source folds into Details.
      expect(
        within(container).getByRole('button', { name: 'Cancel run' }).closest('details'),
      ).toBeNull()
      expect(
        within(container).getByRole('link', { name: 'View in history' }).closest('details'),
      ).toBeNull()
      const details = within(container)
        .getByText('Details', { selector: 'summary' })
        .closest('details')!
      expect(details).toHaveTextContent('exact-accepted-source')
      for (const text of shown) {
        expect(container.textContent).toContain(text)
        expect(details.textContent).not.toContain(text)
      }
    },
  )

  // Catches: an approval review that keeps raw IDs, the program body or the Run's controls, or
  // that dates the request by its Run or its deadline.
  it.each([
    ['from its Run', false],
    ['from its call', true],
  ])('reviews an approval reached %s with only the call to decide on', (_, fromCall) => {
    const live = { ...run, state: 'running' as const, endedAt: null, requiresAction: true }
    const call = {
      ...invocationPanel({ state: 'pending', outcome: 'unknown', endedAt: null }),
      request: approvalRequest,
      context: { ...runPanel(live), source: 'exact-program-source' },
    } as InvocationDetail
    const container = show(
      fromCall
        ? call
        : ({
            ...runPanel(live),
            source: 'exact-program-source',
            request: approvalRequest,
          } as RunDetail),
      run.name,
    )
    expect(container.textContent).not.toContain('Technical details')
    for (const id of [approvalRequest.name, approvalRequest.provider, workspace])
      expect(container.textContent).not.toContain(id)
    for (const heading of ['Context', 'Decision'])
      expect(within(container).queryByRole('heading', { name: heading })).toBeNull()
    // No program body, history link or Run cancel: the review decides this one call.
    for (const gone of ['Program body', 'exact-program-source', 'View in history', 'Cancel run'])
      expect(container.textContent).not.toContain(gone)
    expect(container.textContent).toContain('exact-recipient')
    expect(container.textContent).toContain('Approve this call')
    // The call's own start, when the call is loaded; never the Run's start or the deadline.
    const list = within(container).getByText('Requested by', { selector: 'dt' }).closest('dl')!
    if (fromCall) {
      expect(terms(list).slice(-2)).toEqual(['Requested by', 'Call started'])
      expectInstant(
        factIn(list, 'Call started'),
        call.invocation.startedAt,
        '22 Sep 2026, 09:00:00 UTC',
      )
    } else expect(terms(list).at(-1)).toBe('Requested by')
    expect(container.textContent).not.toMatch(/^Started$/m)
  })

  // Catches: a duration with fractions of a second, ended or still counting.
  it("states a Run's duration in whole seconds", () => {
    const container = show(
      runPanel({
        state: 'succeeded',
        durationNanos: '263001000000',
        endedAt: '2026-09-22T09:05:00Z',
        requiresAction: false,
      }),
    )
    expect(factIn(container.querySelector('dl')!, 'Duration')).toHaveTextContent('4m 23s')
  })
  it('counts a running Run up in whole seconds', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval', 'performance'] })
    try {
      renderRoute(
        <ActivityConnectedContext value>
          <ActionReviewSession>
            <ActivityDetail
              page={{
                ...pendingPage,
                selected: null,
                selectedRow: null,
                detail: runPanel({
                  state: 'running',
                  durationNanos: '999000000',
                  endedAt: null,
                  requiresAction: false,
                }),
              }}
            />
          </ActionReviewSession>
        </ActivityConnectedContext>,
        { entry: '/activity' },
      )
      const term = screen.getByText('Duration', { selector: 'dt' })
      expect(term.nextElementSibling!.textContent).toBe('0s')
      await act(async () => {
        vi.advanceTimersByTime(1000)
      })
      expect(term.nextElementSibling!.textContent).toBe('1s')
    } finally {
      vi.useRealTimers()
    }
  })

  // Catches: a tool call whose list repeats the title, or keeps a record path.
  it.each<[string, Partial<InteractionRow>, InteractionEvidence]>([
    [
      'an exec submission',
      { tool: 'exec', outcome: 'succeeded' },
      { kind: 'run', reference: run.name },
    ],
    [
      'a save',
      { tool: 'save_function' },
      { kind: 'function', reference: `${workspace}/savedFunctions/id` },
    ],
    [
      'a delete',
      { tool: 'delete_function' },
      { kind: 'function', reference: `${workspace}/savedFunctions/id` },
    ],
    ['another tool', { tool: 'inspect' }, { kind: 'unknown' }],
  ])('does not repeat the title of %s as its Intent', (_, overrides, evidence) => {
    const name = `${workspace}/tasks/one/interactions/call`
    const container = show(tool({ intent: 'Send reminders', name, ...overrides }, evidence), name)
    expect(within(container).getByRole('heading', { level: 2 }).textContent).toBe('Send reminders')
    expect(within(container).queryByText('Intent', { selector: 'dt' })).toBeNull()
    expectNoScaffolding(container)
    expect(container.textContent).not.toContain(name)
    expect(container.querySelectorAll('dl')).toHaveLength(1)
  })

  // Catches: a run event that repeats its time as raw text or keeps its record path.
  it.each<[string, RunEvent, string[]]>([
    [
      'a cancellation request',
      { ...at, kind: 'run.cancellation_requested', actorKind: 'user', actor: 'Avery' },
      ['Avery'],
    ],
    ['a run start', { ...at, kind: 'run.started' }, []],
  ])('shows %s without its record path or raw time', (_, value, shown) => {
    const name = `${run.name}/events/4`
    const container = show(event(value), name)
    expectNoScaffolding(container)
    for (const text of [name, 'record #', run.startedAt])
      expect(container.textContent).not.toContain(text)
    for (const text of shown) expect(container.textContent).toContain(text)
  })
})

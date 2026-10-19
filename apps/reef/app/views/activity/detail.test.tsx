/** @vitest-environment happy-dom */
import { act, screen, within } from '@testing-library/react'
import { useEffect, useState } from 'react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import { renderToStaticMarkup } from 'react-dom/server'
import { markupRoute, renderRoute } from '@/test-support/route'
import { ActivityDetail } from './detail'
import { ActionReviewSession } from '../actions/review'
import { approvalRequest, pendingPage, run, workspace } from './fixtures'
import { stored } from './discovery/fixtures'
import { discoveryView } from './discovery/response'
import { ActivityConnectedContext } from './live'
import { ContextImpact } from './context-impact'
import type {
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
  // The review's own decision leads; the Run's state is a badge named for the Run.
  expect(html).not.toContain('Run state')
  expect(html).toContain('program run: running')
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
  // The approval review names the caller once, beside the workspace, in place of a Run heading.
  expect(within(container).getByText(/\bYou\b/)).toHaveTextContent('You · Finance')
  expect(within(container).queryByRole('heading', { level: 2 })).not.toBeInTheDocument()
  expect(html).not.toContain('Requested purpose:')
  expect(html).not.toContain('1 action needs you.')
  expect(html).not.toContain('Approval needed')
  // The review keeps its own technical details; the Run's are not repeated below it.
  const technical = [...container.querySelectorAll('details')].filter(
    (element) => element.querySelector('summary')?.textContent === 'Technical details',
  )
  expect(technical).toHaveLength(1)
  expect(technical[0].textContent).toContain(approvalRequest.name)
  expect(technical[0].hasAttribute('open')).toBe(false)
  // The Run's history link and cancel control move into the review's technical details.
  expect(technical[0].textContent).toContain('View in history')
  expect(technical[0].textContent).toContain('Cancel run')
  expect(html.match(/View in history/g)).toHaveLength(1)
  expect(html).not.toContain('Accepted program source')
  const history = [...technical[0].querySelectorAll('a')].find(
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
    expect(within(container).getByText(`Provider: ${call.provider}`)).toBeTruthy()
    const label = within(container).getByText('recipient', { selector: 'dt' })
    expect(label.nextElementSibling).toHaveTextContent('"exact"')
    // The call's own timing reads the same with or without its Run.
    const fact = (term: string) =>
      within(container).getByText(term, { selector: 'dt' }).nextElementSibling
    expectInstant(fact('Started'), call.startedAt, '22 Sep 2026, 09:00:05 UTC')
    expect(fact('Duration')).toHaveTextContent('2.000 s')
    expectInstant(fact('Ended'), call.endedAt!, '22 Sep 2026, 09:00:07 UTC')
    expect(within(container).queryByText('Initiated by')).toBeNull()
    expect(fact('Waiting time')).toHaveTextContent('0.001 s')
    expect(fact('Dispatch')).toHaveTextContent(call.dispatchedAt!)
    // The header badge states the call's outcome; no State row repeats it.
    expect(within(container).queryByText('State', { selector: 'dt' })).toBeNull()
    expect(within(container.querySelector('header')!).getByText('upstream outcome')).toBeTruthy()
    const technical = within(container)
      .getByText('Technical details', { selector: 'summary' })
      .closest('details')!
    expect(technical).not.toHaveAttribute('open')
    if (withRun) {
      expect(technical).toHaveTextContent('Cancel run')
      expect(technical).toHaveTextContent('Whole-run controls')
      expect(container.textContent).toContain('Send invoice reminders')
    } else expect(container.textContent).not.toContain('Send invoice reminders')
  },
)

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
  // The call's outcome and the Run's terminal evidence are visible, each under its own label.
  expect(disclosure('Operation outcome')).toHaveAttribute('open')
  expect(disclosure('Operation outcome')).toHaveTextContent('call-outcome')
  expect(disclosure('Operation outcome')).not.toHaveTextContent('exact-run-failure')
  const evidence = within(container)
    .getByRole('heading', { level: 3, name: 'Run evidence' })
    .closest('section')!
  expect(evidence).toHaveTextContent('program run: failed')
  expect(evidence).not.toHaveTextContent('Run state')
  expect(within(evidence).getByText('Failure details', { selector: 'summary' })).toBeTruthy()
  expect(evidence).toHaveTextContent('exact-run-failure')
  expect(evidence).not.toHaveTextContent('call-outcome')
  // Source, snapshots, and the original submission stay retained, behind technical details.
  const technical = disclosure('Technical details')
  expect(technical).not.toHaveAttribute('open')
  expect(disclosure('Accepted program source')).toHaveTextContent('exact-accepted-source')
  expect(disclosure('Saved-function snapshot: billing.remind')).toHaveTextContent('exact-snapshot')
  for (const title of ['Accepted program source', 'Saved-function snapshot: billing.remind'])
    expect(technical.contains(disclosure(title))).toBe(true)
  const original = within(technical).getByText('Inspect original exec submission')
  expect(new URL(original.getAttribute('href')!, 'http://reef').searchParams.get('record')).toBe(
    submission,
  )
  // Run timing is labeled as the Run's, apart from the call's own.
  expectInstant(
    within(technical).getByText('Run ended', { selector: 'dt' }).nextElementSibling,
    failed.endedAt,
    '22 Sep 2026, 09:01:00 UTC',
  )
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
  let update!: (next: ActivityPage) => void
  function LivePage() {
    const [current, setCurrent] = useState(taskDetailPage(task))
    useEffect(() => {
      update = setCurrent
    }, [])
    return <ActivityDetail page={current} />
  }
  renderRoute(
    <ActionReviewSession>
      <LivePage />
    </ActionReviewSession>,
    { entry: '/activity' },
  )
  const before = screen.getByLabelText('55.5 kB saved so far')
  expect(before).toHaveTextContent('−55.5 kB')
  act(() =>
    update(
      taskDetailPage({
        ...task,
        outcome: 'success',
        endedAt: '2026-09-22T09:05:00Z',
        contextImpactBytes: '-2200000',
        contextImpactPartial: false,
      }),
    ),
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
  ['a Run without an intent', runPanel({ intent: '' }), 'No intent recorded', 'program run'],
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
    'a saved function without an intent by its target',
    tool(
      { tool: 'save_function', intent: '' },
      { kind: 'function', reference: `${workspace}/savedFunctions/reminders` },
    ),
    `${workspace}/savedFunctions/reminders`,
    'save function',
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
    'an operation call end by its outcome',
    event({
      ...at,
      kind: 'invocation.ended',
      invocation: `${workspace}/invocations/one`,
      outcome: 'coral_failure',
    }),
    'Operation call ended: coral failure',
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
    ).toEqual(['Started', ...(ended ? ['Ended'] : []), 'Duration'])
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
  // What the agent asked for comes first, under one Search heading, and only once.
  expect(asked.closest('section')!.querySelector('h4')).toHaveTextContent('Search')
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
  // Results and the truth about coverage stay. A trimmed page states no per-kind
  // coverage, so it says once that more results remain.
  const results = container.querySelector('[aria-label="Results in rank order"]')!
  expect(results.querySelectorAll(':scope > li')).toHaveLength(5)
  const coverage = container.querySelector('[aria-label="Search coverage"]')!
  expect(coverage.textContent).toBe('More results were available after this page.')
  // One Result heading leads the coverage and the ranked results.
  const result = within(container).getByRole('heading', { name: 'Result' })
  expect(result.compareDocumentPosition(coverage) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
  expect(result.closest('section')!.contains(results)).toBe(true)
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
    expect(row.querySelector('details')).toBeNull()
    // The catalog keeps the full details: no rank, signature, description or guidance here.
    expect(row.textContent!.startsWith(String(index + 1))).toBe(false)
    expect(row.querySelector('pre')).toBeNull()
    expect(row.textContent).not.toContain(hit.signatureLine)
    expect(row.textContent).not.toContain(hit.descriptionSnippet.slice(0, 40))
    expect(row.textContent).not.toContain('Guidance:')
  })
})

it('shows a search stored in an earlier shape as stored', () => {
  const container = searchPanel(stored.earlierShape)
  expect(container.textContent).toContain('Stored in an earlier shape')
  expect(container.querySelector('pre')!.textContent).toContain('typescript_declaration')
  expect(container.textContent).not.toContain('Technical details')
})

it('keeps a describe call, apart from search, with its raw response and technical details', () => {
  const container = document.createElement('div')
  container.innerHTML = markup({
    ...pendingPage,
    selected: null,
    selectedRow: null,
    detail: tool(
      { tool: 'describe' },
      discovered(
        { kind: 'describe', callPath: 'coral.providers.github.issues.create', types: [] },
        stored.describeOperation,
      ),
    ),
  })
  expect(within(container).getByRole('heading', { name: 'Call' })).toBeTruthy()
  for (const kept of ['Submission outcome', 'Raw response', 'Technical details'])
    expect(container.textContent).toContain(kept)
  const completed = within(container).getByText('Completed', { selector: 'dt' }).nextElementSibling
  expectInstant(completed, interaction.completedAt!, '22 Sep 2026, 09:00:00 UTC')
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
  'shows what %s returned in a Final value card, or says it returned nothing',
  (_, detail, value) => {
    const container = document.createElement('div')
    container.innerHTML = markup({ ...pendingPage, selected: null, selectedRow: null, detail })
    const card = container.querySelector('[aria-label="Final value"]')
    if (value === null) {
      expect(card).toBeNull()
      expect(container.textContent).toContain('The program returned no final value.')
    } else {
      expect(card!.querySelector('pre')!.textContent).toContain(value)
      expect(container.textContent).not.toContain('The program returned no final value.')
    }
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

it('keeps the time and record line for other run events', () => {
  const container = document.createElement('div')
  container.innerHTML = markup({
    ...pendingPage,
    selected: null,
    selectedRow: null,
    detail: event({ ...at, kind: 'run.started' }),
  })
  expect(container.textContent).toContain(`${at.occurredAt} · record #4`)
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

it('keeps another run event record identity behind technical details', () => {
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
  const technical = within(container)
    .getByText('Technical details', { selector: 'summary' })
    .closest('details')!
  expect(technical).not.toHaveAttribute('open')
  expect(technical).toHaveTextContent(name)
  // Other record kinds retain their header status icon and badge.
  expect(container.querySelector('header svg')).not.toBeNull()
  expect(within(container.querySelector('header')!).getByText('retained')).toBeTruthy()
})

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
              requests: [],
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
    expect(screen.getByText('Request ID', { selector: 'dt' }).nextElementSibling).toHaveTextContent(
      connection.name,
    )
    expect(screen.queryByRole('button', { name: /Connect|Decline/ })).not.toBeInTheDocument()
    // The call, its Run and the retained connection each keep their own state, named apart.
    expect(screen.queryByText('State', { selector: 'dt' })).not.toBeInTheDocument()
    expect(within(document.querySelector('header')!).getByText('pending')).toBeInTheDocument()
    expect(screen.getByText('connection request: pending')).toBeInTheDocument()
    expect(screen.getByText('program run: running')).toBeInTheDocument()
    expect(document.body).not.toHaveTextContent('Run state')
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

/** @vitest-environment happy-dom */
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, expect, it, vi } from 'vitest'
import { ExecutionSourceSplit } from './execution-source-split'
import { PanelHeader } from './panel'
import type { ActivityRow, ProgramError, RunDetail } from './types'

afterEach(() => {
  vi.restoreAllMocks()
})

const run: RunDetail['run'] = {
  agent: '',
  name: 'workspaces/finance/runs/one',
  workspace: 'workspaces/finance',
  state: 'failed',
  startedAt: '2026-09-22T09:00:00Z',
  endedAt: '2026-09-22T09:00:01Z',
  durationNanos: '1',
  requiresAction: false,
  task: '',
  actor: 'you',
}
function context(overrides: Partial<RunDetail> = {}): RunDetail {
  return {
    kind: 'run',
    run,
    source: 'export default async function program() {\n  return 1\n}',
    linkedFunctions: [],
    terminal: { kind: 'unavailable' },
    ...overrides,
  }
}
const frameError: ProgramError = {
  name: 'TypeError',
  message: 'x',
  frames: [{ source: 'Program', line: 2, column: 3 }],
  toolCode: '',
  toolTraceId: '',
  toolStatus: 0,
  toolBody: '',
  toolBodyTruncated: false,
  cause: null,
  errors: [],
  truncated: false,
  causeOmitted: false,
  omittedErrors: 0,
  circular: false,
}
const failedContext = context({
  terminal: {
    kind: 'failed',
    code: 'program_execution_failed',
    message: 'x',
    error: frameError,
  },
})
const matchableContext = (overrides: Partial<RunDetail> = {}) =>
  context({
    source: [
      'export default async function program() {',
      '  await coral.providers.mail.send({})',
      '',
      '  coral.emit({ sent: 1 })',
      '}',
    ].join('\n'),
    ...overrides,
  })
const invocationRow: Extract<ActivityRow, { kind: 'invocation' }> = {
  kind: 'invocation',
  name: 'workspaces/finance/invocations/one',
  workspace: 'workspaces/finance',
  actor: 'you',
  agent: '',
  task: '',
  run: run.name,
  provider: 'workspaces/finance/providers/mail',
  tool: 'send',
  callPath: 'mail.send',
  state: 'ended',
  outcome: 'upstream_outcome',
  startedAt: run.startedAt,
  endedAt: run.startedAt,
  durationNanos: '1',
  requiresAction: false,
  dispatchedAt: run.startedAt,
  waitingNanos: '0',
}

function markup(props: Partial<React.ComponentProps<typeof ExecutionSourceSplit>> = {}) {
  return render(
    <ExecutionSourceSplit context={context()} locatable selectedRow={null} {...props}>
      <p>Selected record detail</p>
    </ExecutionSourceSplit>,
  )
}

const outputRow = (eventKind: string): Extract<ActivityRow, { kind: 'output' }> => ({
  kind: 'output',
  name: `${run.name}/events/1`,
  workspace: 'workspaces/finance',
  run: run.name,
  label: 'output #1',
  startedAt: run.startedAt,
  eventKind,
  agent: '',
})

// Catches: the source pane heading the accepted program, marking it read-only, or offering
// a Follow selection toggle, whose off state left a new record's line out of view.
it('shows the accepted program under no heading, read-only label or Follow toggle', () => {
  markup()
  expect(screen.getByText('Selected record detail')).toBeInTheDocument()
  expect(document.querySelector('[data-line="1"]')).toHaveTextContent(
    'export default async function program() {',
  )
  for (const gone of ['Accepted program source', 'Read-only snapshot', 'Follow selection'])
    expect(screen.queryByText(gone)).not.toBeInTheDocument()
  expect(screen.queryByRole('switch')).not.toBeInTheDocument()
})

// Catches: a missing-location banner for a record that has no call of its own to locate.
it.each([
  ['the Run itself, which did not fail', {}],
  [
    'a failed Run whose frame is outside its source',
    {
      context: context({
        terminal: {
          kind: 'failed',
          code: 'program_execution_failed',
          message: 'x',
          error: { ...frameError, frames: [{ source: 'Program', line: 9, column: 1 }] },
        },
      }),
    },
  ],
  [
    'a Run event that is not a log or an emission',
    {
      locatable: false,
      selectedRow: outputRow('invocation.ended'),
    },
  ],
] as const)('says nothing of a location for %s', (_, props) => {
  markup(props)
  expect(screen.queryByText('Source location unavailable')).not.toBeInTheDocument()
  expect(document.querySelectorAll('[data-highlighted]')).toHaveLength(0)
})

// Catches: the split adding a Run header of its own above the selected record's header,
// or the record's header losing the drawer's close control.
it('adds no header of its own and gives the close control to the selected record', () => {
  render(
    <ExecutionSourceSplit
      context={context({
        run: { ...run, intent: 'Send reminders', actorLabel: 'Billing bot' },
      })}
      locatable
      selectedRow={null}
      close={<button type="button">Close details</button>}
    >
      <PanelHeader icon="Circle" tone="gray" title="Selected record title" status={null} />
    </ExecutionSourceSplit>,
  )
  expect(screen.queryByText('Send reminders')).not.toBeInTheDocument()
  expect(screen.queryByText(/Billing bot/)).not.toBeInTheDocument()
  const [heading, ...others] = screen.getAllByRole('heading')
  expect(others).toHaveLength(0)
  expect(heading).toHaveTextContent('Selected record title')
  expect(screen.getAllByRole('button', { name: 'Close details' })).toEqual([
    within(heading!.closest('header')!).getByRole('button', {
      name: 'Close details',
    }),
  ])
})

it('highlights the exact line a failed Run threw from, when the Run itself is selected', () => {
  markup({ context: failedContext, locatable: true })
  expect(screen.queryByText('Source location unavailable')).not.toBeInTheDocument()
  const line = document.querySelector('[data-line="2"]')!
  expect(line).toHaveAttribute('data-highlighted', 'true')
})

// Catches: a child selection inheriting the Run's failure line instead of its own location.
it('never attributes the parent Run’s failure location to a tool selection', () => {
  const failedWithCall = context({
    source: [
      'export default async function program() {',
      '  return null', // line 2: where `frameError` says the Run itself threw
      '  await coral.providers.mail.send({})', // line 3: where Coral stored this call
      '}',
    ].join('\n'),
    terminal: { kind: 'failed', code: 'program_execution_failed', message: 'x', error: frameError },
  })
  markup({
    context: failedWithCall,
    locatable: false,
    selectedRow: {
      ...invocationRow,
      sourceLocation: { document: { kind: 'program' }, line: 3, column: 31 },
    },
  })
  expect(screen.queryByText('Source location unavailable')).not.toBeInTheDocument()
  expect(document.querySelector('[data-line="3"]')).toHaveAttribute('data-highlighted', 'true')
  expect(document.querySelector('[data-line="2"]')).not.toHaveAttribute('data-highlighted')
})

it('highlights the line Coral stored for the selected record, in the snapshot it names', () => {
  const savedFunction = 'workspaces/finance/savedFunctions/notify'
  markup({
    context: matchableContext({
      linkedFunctions: [
        {
          savedFunction,
          path: 'billing.notify',
          source: 'export default async () => {\n  console.log("sent")\n}',
        },
      ],
    }),
    locatable: false,
    selectedRow: {
      kind: 'output',
      name: `${run.name}/events/1`,
      workspace: 'workspaces/finance',
      run: run.name,
      label: 'log #1',
      startedAt: run.startedAt,
      eventKind: 'program.logged',
      agent: '',
      sourceLocation: { document: { kind: 'saved_function', savedFunction }, line: 2, column: 11 },
    },
  })
  expect(screen.getByText('Saved-function snapshot: billing.notify')).toBeInTheDocument()
  expect(document.querySelector('[data-line="2"]')).toHaveAttribute('data-highlighted', 'true')
  // Catches: the highlight told only by color, with no text a screen reader announces.
  expect(screen.getByRole('status')).toHaveTextContent('Line 2')
})

// Catches: a tool, log or emission with no stored location shown without saying so,
// or a text match of its call standing in for a location Coral never stored.
it.each([
  ['a tool', invocationRow],
  ['a log', outputRow('program.logged')],
  ['an emission', outputRow('program.emitted')],
])('says the location is unavailable for %s without a stored one', (_, selectedRow) => {
  markup({ context: matchableContext(), locatable: false, selectedRow })
  expect(screen.getByText('Source location unavailable')).toBeInTheDocument()
  expect(document.querySelectorAll('[data-highlighted]')).toHaveLength(0)
})

it('starts the split balanced 50/50 of the actual available space', () => {
  markup()
  expect(screen.getByRole('separator', { name: /resize/i })).toHaveAttribute('aria-valuenow', '50')
})

it('moves the split on a resize, clamped so neither pane shrinks past its minimum', async () => {
  // A 400px split: the minimum 120px pane is 30% of it, so the maximum ratio is 70%.
  vi.spyOn(Element.prototype, 'getBoundingClientRect').mockReturnValue({
    top: 0,
    bottom: 400,
    left: 0,
    right: 0,
    height: 400,
    width: 0,
  } as DOMRect)
  const user = userEvent.setup()
  markup()
  const separator = screen.getByRole('separator', { name: /resize/i })
  separator.focus()
  await user.keyboard('{ArrowDown}') // 16px of 400px = 4 percentage points
  expect(separator).toHaveAttribute('aria-valuenow', '54')
  for (let i = 0; i < 30; i++) await user.keyboard('{ArrowDown}')
  expect(separator).toHaveAttribute('aria-valuenow', '70')
})

it('keeps the split size and the source when the children remount for a new record', async () => {
  vi.spyOn(Element.prototype, 'getBoundingClientRect').mockReturnValue({
    top: 0,
    bottom: 400,
    left: 0,
    right: 0,
    height: 400,
    width: 0,
  } as DOMRect)
  const user = userEvent.setup()
  const { rerender } = render(
    <ExecutionSourceSplit context={context()} locatable selectedRow={null}>
      <p key="a">First selected record</p>
    </ExecutionSourceSplit>,
  )
  const separator = screen.getByRole('separator', { name: /resize/i })
  separator.focus()
  await user.keyboard('{ArrowDown}')
  expect(separator).toHaveAttribute('aria-valuenow', '54')
  const code = document.querySelector('[data-line="1"]')!.closest('pre')
  rerender(
    <ExecutionSourceSplit context={context()} locatable selectedRow={null}>
      <p key="b">Second selected record</p>
    </ExecutionSourceSplit>,
  )
  expect(screen.getByText('Second selected record')).toBeInTheDocument()
  expect(screen.getByRole('separator', { name: /resize/i })).toHaveAttribute('aria-valuenow', '54')
  expect(document.querySelector('[data-line="1"]')!.closest('pre')).toBe(code)
})

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
  operationCode: '',
  operationTraceId: '',
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
  operation: 'send',
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

it('names the accepted program and says explicitly when no location is recorded', () => {
  markup()
  expect(screen.getByText('Accepted program source')).toBeInTheDocument()
  expect(screen.getByText('Source location unavailable')).toBeInTheDocument()
  expect(screen.getByText('Selected record detail')).toBeInTheDocument()
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
it('never attributes the parent Run’s failure location to an operation selection', () => {
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

// Catches: a text match of the call standing in for a location Coral never stored.
it('says the location is unavailable for a record without a stored one, though its call matches', () => {
  markup({ context: matchableContext(), locatable: false, selectedRow: invocationRow })
  expect(screen.getByText('Source location unavailable')).toBeInTheDocument()
  expect(document.querySelectorAll('[data-highlighted]')).toHaveLength(0)
})

it('keeps the resize divider and the Follow selection toggle on', () => {
  markup()
  expect(screen.getByRole('separator', { name: /resize/i })).toBeInTheDocument()
  const toggle = screen.getByRole('switch', { name: 'Follow selection' })
  expect(toggle).toHaveAttribute('aria-checked', 'true')
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

it('does not reset Follow selection or the split size when the children remount for a new record', async () => {
  const user = userEvent.setup()
  const { rerender } = render(
    <ExecutionSourceSplit context={context()} locatable selectedRow={null}>
      <p key="a">First selected record</p>
    </ExecutionSourceSplit>,
  )
  const toggle = screen.getByRole('switch', { name: 'Follow selection' })
  await user.click(toggle)
  expect(toggle).toHaveAttribute('aria-checked', 'false')
  rerender(
    <ExecutionSourceSplit context={context()} locatable selectedRow={null}>
      <p key="b">Second selected record</p>
    </ExecutionSourceSplit>,
  )
  expect(screen.getByText('Second selected record')).toBeInTheDocument()
  expect(screen.getByRole('switch', { name: 'Follow selection' })).toHaveAttribute(
    'aria-checked',
    'false',
  )
})

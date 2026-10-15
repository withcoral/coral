import type { Meta, StoryObj } from '@storybook/react'
import { createRoutesStub } from 'react-router'
import { expect, waitFor, within } from 'storybook/test'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import type { ConnectionState } from '../accounts/types'
import { ActionReviewSession } from '../actions/review'
import { Activity } from './activity'
import { stored } from './discovery/fixtures'
import {
  approval,
  approvalAction,
  approvalDetail,
  at,
  connection,
  connectionAction,
  connectionDetail,
  discovery,
  endedInvocation,
  endedRun,
  failureMessages,
  groupedError,
  interaction,
  invocation,
  invocationDetail,
  invocationResults,
  later,
  outputRow,
  panelPage,
  providerFilteredSearch,
  run,
  runDetail,
  searched,
  settle,
  task,
  toolRow,
  workspace,
  type ApprovalState,
  type CancellationActor,
  type InteractionOutcome,
  type LogLevel,
  type TaskOutcome,
} from './side-panel-fixtures'
import type {
  ActivityPage,
  InteractionEvidence,
  InteractionRow,
  InvocationDetail,
  InvocationOutcome,
  InvocationState,
  RunDetail,
  RunEvent,
  RunFailureCode,
  RunState,
} from './types'

/**
 * Every Activity side panel, opened on load. A story names the record kind and
 * the state Coral reports for it. `evidence` is exact retained text the open
 * panel must show, whatever its layout.
 */
const meta = {
  title: 'Activity/Side Panels',
  component: Activity,
  parameters: { layout: 'fullscreen', chromatic: { viewports: [1440] } },
  render: (args) => (
    <ActionReviewSession
      request={
        args.page.detail?.kind === 'run' || args.page.detail?.kind === 'invocation'
          ? args.page.detail.request
          : null
      }
    >
      <Activity {...args} />
    </ActionReviewSession>
  ),
  decorators: [
    (Story, context) => {
      const { page } = context.args
      const query = new URLSearchParams()
      if (page.selected) query.set('record', page.selected)
      const detail = page.detail
      const request =
        detail?.kind === 'run' || detail?.kind === 'invocation' ? detail.request?.name : undefined
      if (request) query.set('request', request)
      const Stub = createRoutesStub([{ path: '*', Component: Story }])
      return (
        <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
          <PageLayout variant="split" breadcrumbs={trail([], 'Activity')}>
            <Stub initialEntries={[`${page.listPath}?${query}`]} />
          </PageLayout>
        </div>
      )
    },
  ],
  play: async ({ canvasElement, parameters }) => {
    const opened = await within(canvasElement.ownerDocument.body).findByRole('dialog')
    await waitFor(() => expect(opened).toBeVisible())
    for (const text of [parameters.evidence ?? []].flat())
      await expect(opened).toHaveTextContent(text)
  },
} satisfies Meta<typeof Activity>
export default meta
type Story = StoryObj<typeof meta>

const reconnecting = { connected: false, followRoot: false, atStart() {}, viewport() {} }
/** A short, narrow phone screen. Chromatic reads `viewports`; Vitest reads the global. */
const narrow = {
  globals: { viewport: { value: 'mobile1', isRotated: false } },
  parameters: { chromatic: { viewports: [320] } },
}

/** One panel story: `page` opens on load and shows each `evidence` text. */
function panel(
  name: string,
  page: ActivityPage,
  options: { evidence?: string | string[]; live?: typeof reconnecting; narrow?: boolean } = {},
): Story {
  return {
    name,
    args: { page, ...(options.live && { live: options.live }) },
    ...(options.narrow && { globals: narrow.globals }),
    parameters: {
      evidence: options.evidence,
      ...(options.narrow && narrow.parameters),
    },
  }
}
const runPage = (detail: RunDetail, overrides?: Partial<ActivityPage>) =>
  panelPage(
    {
      ...detail.run,
      kind: 'run',
      submission: detail.submission,
      hasFinalValue: detail.terminal.kind === 'succeeded' && detail.terminal.finalValue !== null,
    },
    detail,
    overrides,
  )
const invocationPage = (detail: InvocationDetail) =>
  panelPage({ ...detail.invocation, kind: 'invocation' }, detail)
const toolPage = (value: InteractionRow, evidence: InteractionEvidence) =>
  panelPage(toolRow(value), {
    kind: 'tool',
    task: toolRow(value).task,
    interaction: value,
    evidence,
  })
const eventPage = (event: RunEvent, label: string) =>
  panelPage(outputRow(event, label), { kind: 'output', run: run().name, event })

// Tasks ---------------------------------------------------------------------

const longGoal =
  'Find every overdue invoice in the Finance workspace, group them by account owner, and send each owner one reminder that lists the invoice numbers, amounts, and due dates.'
const tasks = {
  open: panel('Task · open goal', taskPage(task()), {
    evidence: 'Find overdue invoices and notify their owners',
  }),
  success: panel(
    'Task · goal succeeded, with completion feedback',
    taskPage(task({ outcome: 'success', endedAt: later }), 'Sent 24 reminders to 9 owners.'),
    { evidence: 'Sent 24 reminders to 9 owners.' },
  ),
  failure: panel(
    'Task · goal failed, with completion feedback',
    taskPage(
      task({ outcome: 'failure', endedAt: later }),
      'I could not find an operation to export invoices.\nSearch returned no matching operations.',
    ),
    { evidence: 'I could not find an operation to export invoices.' },
  ),
} satisfies Record<TaskOutcome, Story>
function taskPage(row: ReturnType<typeof task>, feedback?: string) {
  return panelPage(row, { kind: 'task', task: row, ...(feedback !== undefined && { feedback }) })
}
export const TaskOpen = tasks.open
export const TaskSucceeded = tasks.success
export const TaskFailed = tasks.failure
export const TaskLongGoalNarrow = panel(
  'Task · long goal, narrow screen',
  taskPage(task({ intent: longGoal })),
  { evidence: longGoal, narrow: true },
)

// Tool calls: search and describe evidence -------------------------------------

const searchCall = interaction('search', 'succeeded')
const describeCall = (callPath: string) =>
  interaction('describe', 'succeeded', {
    intent: 'Inspect the inputs before calling it',
    name: `${searchCall.name}-${callPath}`,
  })
export const SearchResults = panel(
  'Search · results, complete coverage',
  toolPage(searchCall, discovery(searched, stored.complete)),
  { evidence: ['github pulls', 'coral.providers.github.pulls.merge'] },
)
/** Compact results on a phone: long call paths wrap inside their row, beside their link. */
export const SearchResultsNarrow: Story = {
  ...panel(
    'Search · results, narrow screen',
    toolPage(searchCall, discovery(searched, stored.complete)),
    { evidence: 'coral.providers.github.pulls.getMergeAsyncResult', narrow: true },
  ),
  play: async (context) => {
    await meta.play(context)
    const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
    await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
    const rows = within(
      within(dialog).getByRole('list', { name: 'Results in rank order' }),
    ).getAllByRole('listitem')
    await expect(rows).toHaveLength(5)
    for (const row of rows) {
      const edge = row.getBoundingClientRect()
      for (const part of [row.querySelector('code')!, within(row).getByRole('link')]) {
        const box = part.getBoundingClientRect()
        await expect(box.left >= edge.left && box.right <= edge.right).toBe(true)
      }
    }
  },
}
export const SearchProviderFiltered = panel(
  'Search · one provider, saved functions skipped',
  toolPage(searchCall, discovery({ ...searched, provider: 'github' }, providerFilteredSearch)),
  { evidence: 'github pulls' },
)
export const SearchPartialCoverage = panel(
  'Search · partial coverage',
  toolPage(searchCall, discovery(searched, stored.partial)),
  { evidence: 'github pulls' },
)
export const SearchFailedCoverage = panel(
  'Search · a kind could not be searched',
  toolPage(searchCall, discovery(searched, stored.error)),
  { evidence: 'github pulls' },
)
export const SearchMixedResults = panel(
  'Search · operations and saved functions',
  toolPage(searchCall, discovery({ ...searched, query: 'github issues by label' }, stored.mixed)),
  { evidence: 'github issues by label' },
)
export const SearchTargetsGone = panel(
  'Search · results no longer in the catalog',
  toolPage(searchCall, discovery(searched, stored.complete, { gone: true })),
  { evidence: 'coral.providers.github.pulls.merge' },
)
export const SearchEarlierShape = panel(
  'Search · response stored in an earlier shape',
  toolPage(searchCall, discovery(searched, stored.earlierShape)),
  { evidence: 'typescript_declaration' },
)
export const DescribeOperation = panel(
  'Describe · operation',
  toolPage(
    describeCall('issues.create'),
    discovery(
      { kind: 'describe', callPath: 'coral.providers.github.issues.create', types: [] },
      stored.describeOperation,
    ),
  ),
  { evidence: 'Create an issue' },
)
export const DescribeSavedFunction = panel(
  'Describe · saved function, with diagnostics',
  toolPage(
    describeCall('openIssuesByLabel'),
    discovery(
      { kind: 'describe', callPath: 'coral.functions.github.openIssuesByLabel', types: [] },
      stored.describeFunction,
    ),
  ),
  { evidence: ["Cannot find name 'coral'."] },
)

// Tool calls: other evidence -------------------------------------------------------

const execCall = (outcome: InteractionOutcome, overrides: Partial<InteractionRow> = {}) =>
  interaction('exec', outcome, { intent: 'Send invoice reminders', ...overrides })
const toolEvidence = {
  discovery: SearchResults,
  run: panel(
    'Exec submission · accepted, run started',
    toolPage(execCall('succeeded', { acceptedRun: run().name }), {
      kind: 'run',
      reference: run().name,
    }),
    { evidence: 'Send invoice reminders' },
  ),
  function: panel(
    'Save function · saved',
    toolPage(interaction('save_function', 'succeeded', { intent: 'Save the reminder batch' }), {
      kind: 'function',
      reference: `${workspace}/savedFunctions/reminders`,
    }),
    { evidence: `${workspace}/savedFunctions/reminders` },
  ),
  rejection: panel(
    'Exec submission · rejected',
    toolPage(execCall('rejected'), {
      kind: 'rejection',
      code: 'program_admission_failed',
      message: "program.ts(3,7): error TS2304: Cannot find name 'invoices'.",
    }),
    { evidence: ['program_admission_failed', "Cannot find name 'invoices'."] },
  ),
  unknown: panel(
    'Exec submission · completion unknown',
    toolPage(execCall('unknown'), { kind: 'unknown' }),
    { evidence: 'Send invoice reminders' },
  ),
} satisfies Record<InteractionEvidence['kind'], Story>
export const ExecAccepted = toolEvidence.run
export const ExecRejected = toolEvidence.rejection
export const ExecCompletionUnknown = toolEvidence.unknown
export const SaveFunctionSaved = toolEvidence.function
export const DeleteFunctionDeleted = panel(
  'Delete function · deleted',
  toolPage(
    interaction('delete_function', 'succeeded', { intent: 'Remove the old reminder batch' }),
    {
      kind: 'function',
      reference: `${workspace}/savedFunctions/old-reminders`,
    },
  ),
  { evidence: `${workspace}/savedFunctions/old-reminders` },
)
export const SaveFunctionRejected = panel(
  'Save function · rejected',
  toolPage(interaction('save_function', 'rejected', { intent: 'Save the reminder batch' }), {
    kind: 'rejection',
    code: 'saved_function_admission_failed',
    message: 'reminders.ts(1,1): error TS1128: Declaration or statement expected.',
  }),
  { evidence: 'saved_function_admission_failed' },
)
export const SearchRejected = panel(
  'Search · rejected',
  toolPage(interaction('search', 'rejected'), {
    kind: 'rejection',
    code: 'invalid_cursor',
    message: 'Search cursor does not match this request',
  }),
  { evidence: 'invalid_cursor' },
)

// Program Runs ---------------------------------------------------------------

const finalValue = '{"reminded":24,"owners":9}'
export const RunRunning: Story = {
  ...panel('Program run · running', runPage(runDetail(run())), {
    evidence: 'Send invoice reminders',
  }),
  play: async (context) => {
    await meta.play(context)
    const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
    const cancel = within(dialog).getByRole('button', { name: 'Cancel run' })
    // A destructive button: filled red, not a bare text control.
    const [red, green, blue] = getComputedStyle(cancel).backgroundColor.match(/\d+/g)!.map(Number)
    await expect(red).toBeGreaterThan(green + 60)
    await expect(red).toBeGreaterThan(blue + 60)
    // It ends at the panel's end edge, as the timing list does.
    const list = within(dialog).getByText('Started', { selector: 'dt' }).closest('dl')!
    const end = list.getBoundingClientRect().right
    await expect(Math.abs(cancel.getBoundingClientRect().right - end)).toBeLessThanOrEqual(1)
  },
}
export const RunRunningReconnecting = panel(
  'Program run · running, reconnecting',
  runPage(runDetail(run())),
  { live: reconnecting, evidence: 'Send invoice reminders' },
)
export const RunWaitingOnActions = panel(
  'Program run · waiting on several actions',
  runPage(
    runDetail(
      run({
        requiresAction: true,
        actionPage: {
          total: '3',
          next: 'next-action',
          actions: [
            approvalAction,
            {
              ...approvalAction,
              name: `${workspace}/approvals/update`,
              callPath: 'coral.providers.billing.updateInvoice',
              providerLabel: 'Billing',
            },
          ],
        },
      }),
    ),
  ),
  { evidence: 'coral.providers.mail.send' },
)
export const RunRequestUnavailable = panel(
  'Program run · selected request unavailable',
  runPage(runDetail(run(), { requestUnavailable: true })),
  { evidence: 'Send invoice reminders' },
)
export const RunPersonalOutsideTask = panel(
  'Program run · personal, outside a task',
  runPage(
    runDetail(
      endedRun('succeeded', {
        name: `${workspace}/runs/personal`,
        agent: '',
        actor: 'you',
        actorLabel: 'You',
        task: '',
        intent: '',
        submission: undefined,
      }),
      { terminal: { kind: 'succeeded', finalValue: '[]' }, source: 'return []' },
    ),
  ),
)
export const RunLabelsUnavailable = panel(
  'Program run · agent and workspace labels unavailable',
  runPage(
    runDetail(endedRun('succeeded', { actorLabel: '', workspaceLabel: '' }), {
      terminal: { kind: 'succeeded', finalValue },
    }),
  ),
  { evidence: '"reminded"' },
)
export const RunSucceededWithFinalValue = panel(
  'Program run · succeeded, final value',
  runPage(runDetail(endedRun('succeeded'), { terminal: { kind: 'succeeded', finalValue } })),
  { evidence: '"owners"' },
)
/** What the program returned, with a precise amount and a trailing zero Coral keeps as stored. */
const returned = '{"reminded":24,"owners":9,"totalOverdue":18450.70,"skipped":["INV-4180"]}'
/**
 * The panel a "final value" row opens. That row names `<run>/final`; the loader answers
 * with its succeeded Run, so the Run panel shows the value the program returned. That is
 * the program's result, not an intermediate `coral.emit` output.
 */
export const FinalValue: Story = {
  ...panel(
    'Final value',
    runPage(
      runDetail(endedRun('succeeded'), { terminal: { kind: 'succeeded', finalValue: returned } }),
    ),
    { evidence: '"totalOverdue"' },
  ),
  // Literal, so Storybook's static index shows this name too.
  name: 'Final value',
  play: async (context) => {
    await meta.play(context)
    const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
    const header = dialog.querySelector('header')!
    await expect(
      within(header).getByRole('heading', { level: 2, name: 'Send invoice reminders' }),
    ).toBeVisible()
    await expect(header).toHaveTextContent('program run')
    await expect(within(header).getByText('succeeded', { exact: true })).toBeVisible()
    // The returned value shows directly in its own card, not behind a disclosure.
    const value = within(dialog).getByRole('region', { name: 'Final value' })
    await expect(value).toBeVisible()
    await expect(value.textContent).toBe(value.querySelector('pre')!.textContent)
    await expect(within(dialog).queryByText('Final value', { selector: 'summary' })).toBeNull()
    for (const token of ['"reminded"', '24', '"owners"', '9', '18450.70', '"INV-4180"'])
      await expect(value).toHaveTextContent(token)
    // A returned value is not an emission: no emitted-output card or title. The Run's
    // accepted source below may call coral.emit; the selected record's own pane does not.
    await expect(within(dialog).queryByRole('region', { name: 'Emitted value' })).toBeNull()
    await expect(
      within(dialog).getByRole('region', { name: 'Selected record' }),
    ).not.toHaveTextContent('coral.emit')
  },
}
export const RunSucceededWithoutFinalValue = panel(
  'Program run · succeeded, no final value',
  runPage(runDetail(endedRun('succeeded'), { terminal: { kind: 'succeeded', finalValue: null } })),
)
const failed = (
  code: RunFailureCode,
  error: Extract<RunDetail['terminal'], { kind: 'failed' }>['error'] = null,
) =>
  panel(
    `Program run · failed, ${code.replaceAll('_', ' ')}`,
    runPage(
      runDetail(endedRun('failed'), {
        terminal: { kind: 'failed', code, message: failureMessages[code], error },
      }),
    ),
    { evidence: failureMessages[code] },
  )
const failures = {
  program_rejected: failed('program_rejected', {
    ...groupedError.errors[1],
    cause: null,
  }),
  invalid_program_value: failed('invalid_program_value'),
  resource_limit_exceeded: failed('resource_limit_exceeded'),
  memory_limit_exceeded: failed('memory_limit_exceeded'),
  program_execution_failed: failed('program_execution_failed', groupedError),
  run_authority_lost: failed('run_authority_lost'),
  unknown: failed('unknown'),
} satisfies Record<RunFailureCode, Story>
export const RunFailedProgramRejected = failures.program_rejected
export const RunFailedInvalidProgramValue = failures.invalid_program_value
export const RunFailedResourceLimit = failures.resource_limit_exceeded
export const RunFailedMemoryLimit = failures.memory_limit_exceeded
export const RunFailedGroupedProgramError = failures.program_execution_failed
export const RunFailedAuthorityLost = failures.run_authority_lost
export const RunFailedUnknownCode = failures.unknown
const runStates = {
  running: RunRunning,
  succeeded: RunSucceededWithFinalValue,
  failed: RunFailedGroupedProgramError,
  canceled: panel(
    'Program run · canceled',
    runPage(runDetail(endedRun('canceled'), { terminal: { kind: 'canceled' } })),
  ),
  timed_out: panel(
    'Program run · timed out',
    runPage(runDetail(endedRun('timed_out'), { terminal: { kind: 'timed_out' } })),
  ),
  interrupted: panel(
    'Program run · interrupted',
    runPage(runDetail(endedRun('interrupted'), { terminal: { kind: 'interrupted' } })),
  ),
  unknown: panel(
    'Program run · state unknown, terminal evidence unavailable',
    runPage(runDetail(endedRun('unknown'))),
  ),
} satisfies Record<RunState, Story>
export const RunCanceled = runStates.canceled
export const RunTimedOut = runStates.timed_out
export const RunInterrupted = runStates.interrupted
export const RunStateUnknown = runStates.unknown

// Operation Invocations --------------------------------------------------------

export const InvocationPendingOnApproval = panel(
  'Operation call · pending, waits on an approval (review not opened)',
  invocationPage(
    invocationDetail(invocation({ requiresAction: true }), {
      requests: [{ kind: 'approval', name: approvalAction.name }],
    }),
  ),
  { evidence: ['coral.providers.mail.send', 'owner@example.com'] },
)
export const InvocationPendingOnConnection = panel(
  'Operation call · pending, waits on an account connection',
  invocationPage(
    invocationDetail(
      invocation({
        requiresAction: true,
        provider: `${workspace}/providers/linear`,
        callPath: 'coral.providers.linear.listIssues',
        operation: 'listIssues',
      }),
      {
        arguments: '{"assignee":"me","limit":50}',
        requests: [{ kind: 'account_connection', name: connectionAction.name }],
        request: connection('pending'),
      },
    ),
  ),
  { evidence: 'coral.providers.linear.listIssues' },
)
export const InvocationPendingQueued = panel(
  'Operation call · pending, not waiting on you',
  invocationPage(
    invocationDetail(invocation({ waitingNanos: '4000000000', durationNanos: '4000000000' })),
  ),
  { evidence: 'coral.providers.mail.send' },
)
const invocationStates = {
  pending: InvocationPendingOnApproval,
  dispatching: panel(
    'Operation call · dispatching',
    invocationPage(
      invocationDetail(
        invocation({
          state: 'dispatching',
          dispatchedAt: '2026-09-18T14:02:01.000Z',
          waitingNanos: '1000000000',
        }),
      ),
    ),
    { evidence: 'coral.providers.mail.send' },
  ),
  ended: panel(
    'Operation call · ended, upstream outcome',
    invocationPage(
      invocationDetail(endedInvocation('upstream_outcome'), {
        result: invocationResults.upstream_outcome,
      }),
    ),
    { evidence: 'msg_01J8' },
  ),
  unknown: panel(
    'Operation call · state unknown',
    invocationPage(invocationDetail(invocation({ state: 'unknown' }))),
    { evidence: 'coral.providers.mail.send' },
  ),
} satisfies Record<InvocationState, Story>
export const InvocationDispatching = invocationStates.dispatching
export const InvocationStateUnknown = invocationStates.unknown
const ended = (outcome: InvocationOutcome, name: string, evidence?: string) =>
  panel(
    name,
    invocationPage(
      invocationDetail(endedInvocation(outcome), { result: invocationResults[outcome] }),
    ),
    { evidence },
  )
const invocationOutcomes = {
  upstream_outcome: invocationStates.ended,
  coral_failure: ended(
    'coral_failure',
    'Operation call · ended, Coral failure',
    invocationResults.coral_failure.message,
  ),
  run_terminated: ended(
    'run_terminated',
    'Operation call · ended, run terminated it before dispatch',
  ),
  indeterminate: ended('indeterminate', 'Operation call · ended, result indeterminate'),
  unknown: ended('unknown', 'Operation call · ended, outcome unknown'),
} satisfies Record<InvocationOutcome, Story>
export const InvocationUpstreamOutcome = invocationOutcomes.upstream_outcome
export const InvocationCoralFailure = invocationOutcomes.coral_failure
export const InvocationRunTerminated = invocationOutcomes.run_terminated
export const InvocationIndeterminate = invocationOutcomes.indeterminate
export const InvocationOutcomeUnknown = invocationOutcomes.unknown
export const InvocationUpstreamHttpError = panel(
  'Operation call · upstream HTTP 404',
  invocationPage(
    invocationDetail(endedInvocation('upstream_outcome'), {
      result: {
        kind: 'upstream_outcome',
        value: '{"data":{"message":"Recipient not found"},"metadata":{"status":404}}',
      },
    }),
  ),
  { evidence: 'Recipient not found' },
)
export const InvocationUpstreamMcpError = panel(
  'Operation call · upstream MCP result with isError',
  invocationPage(
    invocationDetail(
      endedInvocation('upstream_outcome', {
        provider: `${workspace}/providers/linear`,
        callPath: 'coral.providers.linear.createIssue',
        operation: 'createIssue',
      }),
      {
        arguments: '{"title":"Follow up on INV-4171","teamId":"FIN"}',
        result: {
          kind: 'upstream_outcome',
          value: '{"content":[{"type":"text","text":"Team FIN is archived."}],"isError":true}',
        },
      },
    ),
  ),
  { evidence: 'Team FIN is archived.' },
)
export const InvocationConnectionDeclined = panel(
  'Operation call · failed after its account connection was declined',
  invocationPage(
    invocationDetail(
      endedInvocation('coral_failure', {
        dispatchedAt: null,
        provider: `${workspace}/providers/linear`,
        callPath: 'coral.providers.linear.listIssues',
        operation: 'listIssues',
      }),
      {
        arguments: '{"assignee":"me","limit":50}',
        result: {
          kind: 'coral_failure',
          code: 'account_connection_declined',
          message: 'You declined the shared account connection request.',
        },
        requests: [{ kind: 'account_connection', name: connectionAction.name }],
        request: connection('declined'),
      },
    ),
  ),
  { evidence: 'account_connection_declined' },
)
export const InvocationRequestUnavailable = panel(
  'Operation call · selected request unavailable',
  invocationPage(
    invocationDetail(invocation({ requiresAction: true }), {
      requests: [{ kind: 'approval', name: approvalAction.name }],
      requestUnavailable: true,
    }),
  ),
  { evidence: 'coral.providers.mail.send' },
)
export const InvocationUnknownRequestReference = panel(
  'Operation call · request reference unavailable',
  invocationPage(
    invocationDetail(endedInvocation('upstream_outcome'), {
      result: invocationResults.upstream_outcome,
      requests: [{ kind: 'unknown' }],
    }),
  ),
  { evidence: 'msg_01J8' },
)
export const InvocationExactLargeInteger = panel(
  'Operation call · exact integers beyond JavaScript precision',
  invocationPage(
    invocationDetail(endedInvocation('upstream_outcome'), {
      arguments: '{"invoice":9007199254740993,"ledger":-9223372036854775808}',
      result: {
        kind: 'upstream_outcome',
        value: '{"data":{"id":18446744073709551615},"metadata":{"status":200}}',
      },
    }),
  ),
  { evidence: ['9007199254740993', '-9223372036854775808', '18446744073709551615'] },
)

// Approvals -----------------------------------------------------------------

const approvalPage = (
  request: ReturnType<typeof approval>,
  context?: Parameters<typeof approvalDetail>[1],
) => invocationPage(approvalDetail(request, context))
export const ApprovalPending = panel(
  'Approval · pending, you can decide',
  approvalPage(approval()),
  { evidence: ['coral.providers.mail.send', 'owner@example.com'] },
)
const approvals = {
  pending: ApprovalPending,
  approved: panel('Approval · approved, with comment', approvalPage(approval(settle('approved'))), {
    evidence: 'Checked the exact invoice recipient.',
  }),
  declined: panel('Approval · denied, with comment', approvalPage(approval(settle('declined'))), {
    evidence: 'This owner left the company.',
  }),
  expired: panel('Approval · expired', approvalPage(approval(settle('expired'))), {
    evidence: 'owner@example.com',
  }),
  closed: panel('Approval · closed when the run ended', approvalPage(approval(settle('closed'))), {
    evidence: 'owner@example.com',
  }),
} satisfies Record<ApprovalState, Story>
export const ApprovalApproved = approvals.approved
export const ApprovalDenied = approvals.declined
export const ApprovalExpired = approvals.expired
export const ApprovalClosed = approvals.closed
export const ApprovalPendingReconnecting = panel(
  'Approval · pending, reconnecting',
  approvalPage(approval()),
  { live: reconnecting, evidence: 'owner@example.com' },
)
export const ApprovalPendingNotActionable = panel(
  'Approval · pending, you cannot decide it',
  approvalPage(approval({ actionable: false })),
  { evidence: 'owner@example.com' },
)
export const ApprovalProviderUnavailable = panel(
  'Approval · pending, provider label unavailable',
  approvalPage(approval({ providerLabel: undefined })),
  { evidence: 'coral.providers.mail.send' },
)
export const ApprovalReviewerUnavailable = panel(
  'Approval · approved by a reviewer you cannot see, no comment',
  approvalPage(approval({ ...settle('approved'), approverLabel: undefined, comment: null })),
  { evidence: 'owner@example.com' },
)
export const ApprovalApprovedRunWaitsOnOthers = panel(
  'Approval · approved, the run still waits on other actions',
  approvalPage(approval(settle('approved')), {
    context: run({
      requiresAction: true,
      actionPage: {
        total: '2',
        next: '',
        actions: [
          {
            ...approvalAction,
            name: `${workspace}/approvals/update`,
            callPath: 'coral.providers.billing.updateInvoice',
            providerLabel: 'Billing',
          },
          connectionAction,
        ],
      },
    }),
  }),
  { evidence: 'Checked the exact invoice recipient.' },
)
const longArguments = JSON.stringify({
  to: 'owner@example.com',
  lines: Array.from({ length: 80 }, (_, i) => `Invoice INV-${4100 + i} · overdue 14 days`),
})
export const ApprovalLongArgumentsNarrow = panel(
  'Approval · pending, long arguments, narrow screen',
  approvalPage(approval({ arguments: longArguments })),
  { narrow: true, evidence: 'INV-4179' },
)
const longCallPath =
  'coral.providers.enterpriseResourcePlanning.accountsReceivable.invoices.sendOverdueReminderToPrimaryBillingContact'
export const ApprovalLongCallPathNarrow = panel(
  'Approval · pending, long call path and values, narrow screen',
  approvalPage(
    approval({
      callPath: longCallPath,
      arguments: JSON.stringify({
        contact: 'accounts-payable-escalations-for-the-northern-europe-region@example.com',
        reference: 'a'.repeat(160),
      }),
    }),
  ),
  { narrow: true, evidence: longCallPath },
)

/** Exact logical arguments as Coral's JSON serializer can write them. */
const argumentCases = {
  'nested objects': [
    '{"invoice":{"id":"INV-4171","amount":{"currency":"EUR","value":"1250.00"}},"notify":true,"cc":null}',
    'INV-4171',
  ],
  'array at the top level': ['["owner@example.com","finance@example.com"]', 'finance@example.com'],
  'string at the top level': ['"owner@example.com"', 'owner@example.com'],
  'empty object': ['{}', 'coral.providers.mail.send'],
  'integers beyond JavaScript precision': [
    '{"invoice":9007199254740993,"amount":0.30000000000000004}',
    '9007199254740993',
  ],
  'unicode and escapes': [
    '{"subject":"Rechnung fällig · 請求書 🧾","body":"Line one\\nLine \\"two\\""}',
    'Rechnung fällig',
  ],
} as const
const argumentStory = (label: keyof typeof argumentCases) =>
  panel(
    `Approval · arguments: ${label}`,
    approvalPage(approval({ arguments: argumentCases[label][0] })),
    { evidence: argumentCases[label][1] },
  )
export const ApprovalArgumentsNested = argumentStory('nested objects')
export const ApprovalArgumentsArray = argumentStory('array at the top level')
export const ApprovalArgumentsString = argumentStory('string at the top level')
export const ApprovalArgumentsEmpty = argumentStory('empty object')
export const ApprovalArgumentsLargeIntegers = argumentStory('integers beyond JavaScript precision')
export const ApprovalArgumentsUnicode = argumentStory('unicode and escapes')

// Account connections --------------------------------------------------------

const connectionPage = (state: ConnectionState) => runPage(connectionDetail(connection(state)))
const connections = {
  pending: panel('Account connection · pending', connectionPage('pending'), {
    evidence: 'Read my open Linear issues',
  }),
  connected: panel('Account connection · connected', connectionPage('connected')),
  declined: panel('Account connection · declined', connectionPage('declined')),
  expired: panel('Account connection · expired', connectionPage('expired')),
  route_changed: panel(
    'Account connection · account setting changed',
    connectionPage('route_changed'),
  ),
  authorization_changed: panel(
    'Account connection · account authorization changed',
    connectionPage('authorization_changed'),
  ),
} satisfies Record<ConnectionState, Story>
export const ConnectionPending = connections.pending
export const ConnectionConnected = connections.connected
export const ConnectionDeclined = connections.declined
export const ConnectionExpired = connections.expired
export const ConnectionRouteChanged = connections.route_changed
export const ConnectionAuthorizationChanged = connections.authorization_changed
export const ConnectionPendingReconnecting = panel(
  'Account connection · pending, reconnecting',
  connectionPage('pending'),
  { live: reconnecting },
)
export const ConnectionPendingNarrow = panel(
  'Account connection · pending, narrow screen',
  connectionPage('pending'),
  { narrow: true },
)

// Run output and events --------------------------------------------------------

/** A run event's own fields, without the position every event shares. */
type EventBody = RunEvent extends infer E
  ? E extends unknown
    ? Omit<E, 'sequence' | 'occurredAt'>
    : never
  : never
const event = (sequence: number, value: EventBody) =>
  ({ ...value, sequence: String(sequence), occurredAt: at }) as RunEvent
/**
 * A log or an emission: a title naming its call and record, its time as the one fact, and
 * only its exact content in a card that stays inside the drawer, however long or narrow.
 */
function payloadCard(story: Story, card: string, title: string): Story {
  return {
    ...story,
    play: async (context) => {
      await meta.play(context)
      const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
      const region = within(dialog).getByRole('region', { name: card })
      await expect(region).toBeVisible()
      await expect(region.getBoundingClientRect().right).toBeLessThanOrEqual(
        dialog.getBoundingClientRect().right,
      )
      await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
      // The title names the call and the record; the facts list keeps only the time.
      await expect(within(dialog).getByRole('heading', { level: 2, name: title })).toBeVisible()
      // The icon and close control stay; no badge repeats what the title says.
      const header = dialog.querySelector('header')!
      await expect(header.querySelector('svg')).toBeVisible()
      await expect(within(header).getByRole('button', { name: 'Close details' })).toBeVisible()
      await expect(within(header).queryByText(/^(diagnostic|retained)$/)).toBeNull()
      const terms = within(dialog.querySelector('dl')!).getAllByRole('term')
      await expect(terms.map((term) => term.textContent)).toEqual(['Started'])
      // The card shows only the payload; the facts above carry no heading.
      await expect(region.textContent).toBe(region.querySelector('pre')!.textContent)
      await expect(within(dialog).queryByRole('heading', { name: 'Context' })).toBeNull()
      await expect(dialog).not.toHaveTextContent('Technical details')
      await expect(dialog).not.toHaveTextContent('This log is not a user-facing result')
    },
  }
}
const emission = (value: string) => event(7, { kind: 'program.emitted', value })
export const EmittedObject = payloadCard(
  panel(
    'program.emitted · object, separate from the final value',
    eventPage(emission('{"invoice":"INV-4171","owner":"owner@example.com"}'), 'emission #7'),
    { evidence: 'INV-4171' },
  ),
  'Emitted value',
  'coral.emit #7',
)
export const EmittedString = payloadCard(
  panel(
    'program.emitted · string',
    eventPage(emission('"Halfway through the reminders"'), 'emission #7'),
    { evidence: 'Halfway through the reminders' },
  ),
  'Emitted value',
  'coral.emit #7',
)
export const EmittedLongNarrow = panel(
  'program.emitted · long value, narrow screen',
  eventPage(
    emission(
      JSON.stringify({
        owners: Array.from({ length: 40 }, (_, i) => ({
          owner: `owner-${i + 1}@example.com`,
          invoices: [`INV-${4100 + i}`],
        })),
      }),
    ),
    'emission #7',
  ),
  { narrow: true, evidence: 'owner-40@example.com' },
)
EmittedLongNarrow.play = payloadCard(EmittedLongNarrow, 'Emitted value', 'coral.emit #7').play
const logged = (level: LogLevel, message: string) =>
  payloadCard(
    panel(
      `program.logged · ${level}`,
      eventPage(event(8, { kind: 'program.logged', level, message }), 'log #8'),
      { evidence: message },
    ),
    'Log message',
    `console.${level} #8`,
  )
const logLevels = {
  debug: logged('debug', 'Fetched page 2 of overdue invoices.'),
  info: logged('info', 'Sending 24 reminders.'),
  warn: logged('warn', 'Owner of INV-4180 has no email address. Skipping.'),
  error: logged('error', 'Mail rejected owner@example.com: mailbox full.'),
  unknown: logged('unknown', 'Retained at a level this version of Reef does not know.'),
} satisfies Record<LogLevel, Story>
export const LoggedDebug = logLevels.debug
export const LoggedInfo = logLevels.info
export const LoggedWarn = logLevels.warn
export const LoggedError = logLevels.error
export const LoggedUnknownLevel = logLevels.unknown
export const LoggedLongNarrow = panel(
  'program.logged · long message, narrow screen',
  eventPage(
    event(8, {
      kind: 'program.logged',
      level: 'warn',
      message: `Skipped owners without an address: ${Array.from({ length: 30 }, (_, i) => `owner-${i + 1}`).join(', ')}`,
    }),
    'log #8',
  ),
  { narrow: true, evidence: 'owner-30' },
)
LoggedLongNarrow.play = payloadCard(LoggedLongNarrow, 'Log message', 'console.warn #8').play
const invocationEnded = (outcome: InvocationOutcome) =>
  panel(
    `invocation.ended · ${outcome.replaceAll('_', ' ')}`,
    eventPage(
      event(5, { kind: 'invocation.ended', invocation: invocation().name, outcome }),
      'run event',
    ),
  )
const endedCalls = {
  upstream_outcome: invocationEnded('upstream_outcome'),
  coral_failure: invocationEnded('coral_failure'),
  run_terminated: invocationEnded('run_terminated'),
  indeterminate: invocationEnded('indeterminate'),
  unknown: invocationEnded('unknown'),
} satisfies Record<InvocationOutcome, Story>
export const CallEndedUpstreamOutcome = endedCalls.upstream_outcome
export const CallEndedCoralFailure = endedCalls.coral_failure
export const CallEndedRunTerminated = endedCalls.run_terminated
export const CallEndedIndeterminate = endedCalls.indeterminate
export const CallEndedOutcomeUnknown = endedCalls.unknown
const cancellation = (actorKind: CancellationActor, actor: string) =>
  panel(
    `run.cancellation_requested · by ${{ user: 'a user', agent: 'an agent', unknown: 'an unknown principal' }[actorKind]}`,
    eventPage(
      event(9, { kind: 'run.cancellation_requested', actor, actorKind }),
      'cancellation requested',
    ),
  )
const cancellations = {
  user: cancellation('user', 'users/owner'),
  agent: cancellation('agent', `${workspace}/agents/collections`),
  unknown: cancellation('unknown', ''),
} satisfies Record<CancellationActor, Story>
export const CancellationByUser = cancellations.user
export const CancellationByAgent = cancellations.agent
export const CancellationByUnknown = cancellations.unknown
const runEnded = (state: Exclude<RunState, 'running'>) =>
  panel(
    `run.ended · ${state.replaceAll('_', ' ')}`,
    eventPage(event(10, { kind: 'run.ended', state }), 'execution ended'),
  )
const endings = {
  succeeded: runEnded('succeeded'),
  failed: runEnded('failed'),
  canceled: runEnded('canceled'),
  timed_out: runEnded('timed_out'),
  interrupted: runEnded('interrupted'),
  unknown: runEnded('unknown'),
} satisfies Record<Exclude<RunState, 'running'>, Story>
export const RunEndedSucceeded = endings.succeeded
export const RunEndedFailed = endings.failed
export const RunEndedCanceled = endings.canceled
export const RunEndedTimedOut = endings.timed_out
export const RunEndedInterrupted = endings.interrupted
export const RunEndedUnknown = endings.unknown
const events = {
  'run.started': panel(
    'run.started',
    eventPage(event(1, { kind: 'run.started' }), 'execution accepted'),
  ),
  'invocation.started': panel(
    'invocation.started',
    eventPage(event(4, { kind: 'invocation.started', invocation: invocation().name }), 'run event'),
  ),
  'invocation.ended': CallEndedUpstreamOutcome,
  'program.emitted': EmittedObject,
  'program.logged': LoggedInfo,
  'run.cancellation_requested': CancellationByUser,
  'run.ended': RunEndedFailed,
  unknown: panel(
    'Run event · kind unknown to this version',
    eventPage(event(11, { kind: 'unknown' }), 'run event'),
  ),
} satisfies Record<RunEvent['kind'], Story>
export const RunStarted = events['run.started']
export const CallStarted = events['invocation.started']
export const UnknownEvent = events.unknown

// Unavailable detail ------------------------------------------------------------

export const DetailUnavailable = panel(
  'Unavailable · record removed or concealed',
  panelPage(invocation(), null, { unavailable: true, selectedRow: null }),
)

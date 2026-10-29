// Activity detail panels for the Activity/Side Panels gallery. Each builder returns
// one record in a state that Coral's loader can produce, with only the evidence
// that state carries. Values beside a model type use `satisfies` over that type,
// so a new state fails typechecking until the gallery covers it.

import type { ConnectionState } from '../accounts/types'
import { stored } from './discovery/fixtures'
import { discoveryView, functionTarget } from './discovery/response'
import type {
  ActivityPage,
  ActivityRow,
  DiscoveryContext,
  InteractionEvidence,
  InteractionRow,
  InvocationDetail,
  InvocationOutcome,
  InvocationResult,
  ProgramError,
  RecordSourceLocation,
  RequestDetail,
  RunDetail,
  RunEvent,
  RunFailureCode,
  RunState,
} from './types'
import { minutesAfterStoryNow } from './story-clock'

export const workspace = 'workspaces/finance'
export const at = '2026-09-18T14:02:00.000Z'
export const later = '2026-09-18T14:06:02.000Z'
/** Far enough ahead that a pending deadline stays open in every snapshot. */
export const deadline = '2030-09-18T14:17:00.000Z'
/**
 * A pending action's deadline 15 minutes after the stories' fixed clock, so a list of actions
 * reads "Expires in 15 minutes" in every run and snapshot rather than a count of years in minutes.
 */
export const soon = minutesAfterStoryNow(15)
export const taskName = `${workspace}/tasks/invoices`
export const runName = `${workspace}/runs/8f3c`
export const invocationName = `${workspace}/invocations/mail`

type TaskRow = Extract<ActivityRow, { kind: 'task' }>
type RunRow = Extract<ActivityRow, { kind: 'run' }>
type InvocationRow = Extract<ActivityRow, { kind: 'invocation' }>
type OutputRow = Extract<ActivityRow, { kind: 'output' }>
type ToolRow = Extract<ActivityRow, { kind: 'tool' }>
export type Approval = Extract<RequestDetail, { kind: 'approval' }>
export type Connection = Extract<RequestDetail, { kind: 'account_connection' }>

/** Coral's Task goal outcomes. Only an ended Task has `success` or `failure`. */
export type TaskOutcome = 'open' | 'success' | 'failure'
/** Coral's attempt outcomes for one tool call. */
export type InteractionOutcome = 'succeeded' | 'rejected' | 'unknown'
/** Coral's approval states. Reef reads them as strings. */
export type ApprovalState = 'pending' | 'approved' | 'declined' | 'expired' | 'closed'
export type LogLevel = Extract<RunEvent, { kind: 'program.logged' }>['level']
export type CancellationActor = Extract<
  RunEvent,
  { kind: 'run.cancellation_requested' }
>['actorKind']

export function task(overrides: Partial<TaskRow> = {}): TaskRow {
  return {
    kind: 'task',
    name: taskName,
    workspace,
    actor: 'Collections agent',
    intent: 'Find overdue invoices and notify their owners',
    outcome: 'open',
    startedAt: at,
    endedAt: null,
    agent: '',
    ...overrides,
  }
}

/** The accepted `exec` submission that started `runName`. */
export const submission: InteractionRow = {
  name: `${taskName}/interactions/accepted`,
  agent: '',
  tool: 'exec',
  intent: 'Send invoice reminders',
  outcome: 'succeeded',
  startedAt: at,
  completedAt: at,
  acceptedRun: runName,
}

export function run(overrides: Partial<RunRow> = {}): RunRow {
  return {
    kind: 'run',
    name: runName,
    workspace,
    agent: `${workspace}/agents/collections`,
    actor: 'agent-principal',
    actorLabel: 'Collections agent',
    workspaceLabel: 'Finance',
    intent: 'Send invoice reminders',
    task: taskName,
    state: 'running',
    startedAt: at,
    endedAt: null,
    durationNanos: '242000000000',
    requiresAction: false,
    submission,
    ...overrides,
  }
}

/** A Run that ended in `state` at `later`. */
export const endedRun = (state: Exclude<RunState, 'running'>, overrides: Partial<RunRow> = {}) =>
  run({ state, endedAt: later, durationNanos: '242000000000', ...overrides })

/**
 * The program the gallery's Run accepted. Each tool, log and emission the gallery
 * shows under that Run sits at its own call here (see `programAt`).
 */
export const source = `const invoices = await coral.providers.billing.listInvoices({ status: 'overdue' })
for (const invoice of invoices.data) {
  coral.emit({ invoice: invoice.id, owner: invoice.owner })
  await coral.providers.mail.send({ to: invoice.owner, subject: \`Invoice \${invoice.id} is overdue\` })
}
console.debug('overdue invoices', invoices.data.length)
const issues = await coral.providers.linear.listIssues({ assignee: 'me', limit: 50 })
if (!issues.data.some((issue) => issue.title.includes('INV-4171')))
  await coral.providers.linear.createIssue({ title: 'Follow up on INV-4171', teamId: 'FIN' })
console.info('reminded', invoices.data.length)
console.warn('owners without an address', 0)
console.error('reminders that failed', 0)
console.log('done')
return { reminded: invoices.data.length }`

/**
 * Where Coral stores the call `call` in `code`, the gallery's accepted program unless
 * given: its line, and the column of its last name, as the runtime reports them (1-based,
 * in UTF-16 units). A call the program does not make fails, so no fixture points nowhere.
 */
export function programAt(call: string, code = source): RecordSourceLocation {
  const lines = code.split('\n')
  const index = lines.findIndex((line) => line.includes(`${call}(`))
  if (index < 0) throw new Error(`The program makes no ${call} call.`)
  const start = lines[index]!.indexOf(`${call}(`)
  return {
    document: { kind: 'program' },
    line: index + 1,
    column: start + call.lastIndexOf('.') + 2,
  }
}

export function runDetail(row: RunRow, overrides: Partial<RunDetail> = {}): RunDetail {
  return {
    kind: 'run',
    request: null,
    requestUnavailable: false,
    run: row,
    submission: row.submission,
    source,
    linkedFunctions: [],
    terminal: { kind: 'unavailable' },
    ...overrides,
  }
}

/** A tool call of the Run, stored at its call path's call in the accepted program. */
export function invocation(overrides: Partial<InvocationRow> = {}): InvocationRow {
  const callPath = overrides.callPath ?? 'coral.providers.mail.send'
  return {
    kind: 'invocation',
    name: invocationName,
    workspace,
    actor: 'agent-principal',
    agent: `${workspace}/agents/collections`,
    task: taskName,
    run: runName,
    provider: `${workspace}/providers/mail`,
    tool: 'send',
    callPath,
    state: 'pending',
    outcome: 'unknown',
    startedAt: at,
    endedAt: null,
    durationNanos: '221000000000',
    requiresAction: false,
    dispatchedAt: null,
    waitingNanos: '221000000000',
    sourceLocation: 'sourceLocation' in overrides ? overrides.sourceLocation : programAt(callPath),
    ...overrides,
  }
}

/** A call that dispatched one second after it started and ended with `outcome`. */
export const endedInvocation = (
  outcome: InvocationOutcome,
  overrides: Partial<InvocationRow> = {},
) =>
  invocation({
    state: 'ended',
    outcome,
    endedAt: later,
    dispatchedAt: outcome === 'run_terminated' ? null : '2026-09-18T14:02:01.000Z',
    waitingNanos: outcome === 'run_terminated' ? '221000000000' : '1000000000',
    ...overrides,
  })

export const mailArguments = JSON.stringify({
  to: 'owner@example.com',
  subject: 'Invoice INV-4171 is overdue',
  body: 'Your invoice is ready for review.',
})

/** The tool's detail, with the Run it ran in as context, as Coral's loader reads it. */
export function invocationDetail(
  row: InvocationRow,
  overrides: Partial<InvocationDetail> = {},
): InvocationDetail {
  return {
    kind: 'invocation',
    invocation: row,
    arguments: mailArguments,
    result: { kind: 'unknown' },
    requests: [],
    request: null,
    requestUnavailable: false,
    context: runDetail(run()),
    ...overrides,
  }
}

/** The retained result that each invocation outcome carries. */
export const invocationResults = {
  upstream_outcome: {
    kind: 'upstream_outcome',
    // Coral's HTTP envelope: the status, the provider's data, then Coral's metadata.
    value: '{"status":202,"data":{"id":"msg_01J8","status":"queued"},"metadata":{"kind":"http"}}',
  },
  coral_failure: {
    kind: 'coral_failure',
    code: 'tool_disabled',
    message: 'The tool coral.providers.mail.send is disabled in this workspace.',
  },
  run_terminated: { kind: 'run_terminated' },
  indeterminate: { kind: 'indeterminate' },
  unknown: { kind: 'unknown' },
} satisfies Record<InvocationOutcome, InvocationResult>

export const approvalName = `${workspace}/approvals/mail`
export const connectionName = `${workspace}/accountConnections/linear`

export function approval(overrides: Partial<Approval> = {}): Approval {
  return {
    kind: 'approval',
    actionable: true,
    name: approvalName,
    run: runName,
    invocation: invocationName,
    provider: `${workspace}/providers/mail`,
    providerLabel: 'Mail',
    callPath: 'coral.providers.mail.send',
    arguments: mailArguments,
    state: 'pending',
    deadline,
    approver: '',
    decidedAt: null,
    comment: null,
    ...overrides,
  }
}

/** How each approval settles at `later`. Only a person's decision has an approver. */
export const settledApprovals = {
  approved: {
    state: 'approved',
    approver: 'users/owner',
    approverLabel: 'Avery Owner',
    comment: 'Checked the exact invoice recipient.',
  },
  declined: {
    state: 'declined',
    approver: 'users/owner',
    approverLabel: 'Avery Owner',
    comment: 'This owner left the company. Send it to finance instead.',
  },
  expired: { state: 'expired', deadline: later },
  closed: { state: 'closed' },
} satisfies Record<Exclude<ApprovalState, 'pending'>, Partial<Approval>>

export const settle = (state: Exclude<ApprovalState, 'pending'>): Partial<Approval> => ({
  actionable: false,
  // Every settlement records when; only a person's decision records who.
  decidedAt: later,
  ...settledApprovals[state],
})

/** One pending approval of `invocationName` in the Run's action page. */
export const approvalAction = {
  kind: 'approval' as const,
  name: approvalName,
  callPath: 'coral.providers.mail.send',
  providerLabel: 'Mail',
  deadline: soon,
}
export const connectionAction = {
  kind: 'account_connection' as const,
  name: connectionName,
  callPath: '',
  providerLabel: 'Linear',
  deadline: soon,
}

/** What Coral records on the call and its Run once an approval settles. */
const afterApproval = {
  approved: {
    row: () => endedInvocation('upstream_outcome', { requiresAction: false }),
    result: invocationResults.upstream_outcome,
    run: () => run(),
    terminal: { kind: 'unavailable' },
  },
  declined: {
    row: () =>
      endedInvocation('coral_failure', { dispatchedAt: null, waitingNanos: '221000000000' }),
    result: {
      kind: 'coral_failure',
      code: 'approval_declined',
      message: 'An authorized user declined this call.',
    },
    run: () => run(),
    terminal: { kind: 'unavailable' },
  },
  expired: {
    row: () =>
      endedInvocation('coral_failure', { dispatchedAt: null, waitingNanos: '221000000000' }),
    result: {
      kind: 'coral_failure',
      code: 'approval_expired',
      message: 'The approval deadline passed before anyone decided.',
    },
    run: () => run(),
    terminal: { kind: 'unavailable' },
  },
  // A Run that ends closes its open approvals and terminates their calls.
  closed: {
    row: () => endedInvocation('run_terminated'),
    result: invocationResults.run_terminated,
    run: () => endedRun('canceled'),
    terminal: { kind: 'canceled' },
  },
} satisfies Record<
  Exclude<ApprovalState, 'pending'>,
  {
    row: () => InvocationRow
    result: InvocationResult
    run: () => RunRow
    terminal: RunDetail['terminal']
  }
>

/**
 * The approval review: the exact call, inside the Run that waits on it, stored at that
 * call in the Run's program: the gallery's, or `source`.
 */
export function approvalDetail(
  request: Approval,
  options: { context?: RunRow; source?: string } = {},
): InvocationDetail {
  const settled =
    request.state === 'pending' ? null : afterApproval[request.state as keyof typeof afterApproval]
  const code = options.source ?? source
  const sourceLocation = programAt(request.callPath, code)
  const row = settled
    ? settled.row()
    : invocation({
        name: request.invocation,
        callPath: request.callPath,
        requiresAction: true,
        sourceLocation,
      })
  const context =
    options.context ??
    (settled
      ? settled.run()
      : run({
          requiresAction: true,
          actionPage: {
            total: '1',
            next: '',
            actions: [{ ...approvalAction, name: request.name, callPath: request.callPath }],
          },
        }))
  return invocationDetail(
    { ...row, name: request.invocation, callPath: request.callPath, sourceLocation },
    {
      arguments: request.arguments,
      result: settled?.result ?? { kind: 'unknown' },
      requests: [{ kind: 'approval', name: request.name }],
      request,
      context: runDetail(context, {
        source: code,
        ...(settled && { terminal: settled.terminal }),
      }),
    },
  )
}

export function connection(
  state: ConnectionState,
  overrides: Partial<Connection> = {},
): Connection {
  return {
    kind: 'account_connection',
    name: connectionName,
    provider: `${workspace}/providers/linear`,
    state,
    expiresAt: state === 'pending' ? soon : null,
    ...overrides,
  }
}

/** The account connection review, on the Run that waits on it. */
export function connectionDetail(request: Connection): RunDetail {
  const pending = request.state === 'pending'
  return runDetail(
    run({
      name: `${workspace}/runs/issues`,
      intent: 'Read my open Linear issues',
      agent: '',
      actor: 'you',
      actorLabel: 'You',
      task: '',
      submission: undefined,
      requiresAction: pending,
      actionPage: pending
        ? { total: '1', next: '', actions: [connectionAction] }
        : { total: '0', next: '', actions: [] },
    }),
    {
      request,
      source: "return await coral.providers.linear.listIssues({ assignee: 'me' })",
    },
  )
}

/** Exact connection outcomes; no other value reaches Reef. */
export const connectionStates = [
  'pending',
  'connected',
  'declined',
  'expired',
  'route_changed',
  'authorization_changed',
] as const satisfies readonly ConnectionState[]

export function interaction(
  tool: string,
  outcome: InteractionOutcome,
  overrides: Partial<InteractionRow> = {},
): InteractionRow {
  return {
    name: `${taskName}/interactions/${tool}-${outcome}`,
    agent: '',
    tool,
    intent: 'Find the tool that merges a pull request',
    outcome,
    startedAt: at,
    completedAt: outcome === 'unknown' ? null : at,
    acceptedRun: '',
    ...overrides,
  }
}

export const toolRow = (value: InteractionRow): ToolRow => ({
  ...value,
  kind: 'tool',
  workspace,
  task: taskName,
})

/** A retained `search` or `describe` answer, with links to today's catalog unless `gone`. */
export function discovery(
  context: DiscoveryContext,
  response: string,
  options: { gone?: boolean } = {},
): Extract<InteractionEvidence, { kind: 'discovery' }> {
  const view = discoveryView(context, response)
  const targets =
    view.kind === 'search' ? view.page.hits : view.kind === 'describe' ? [view.description] : []
  for (const target of targets) {
    const tool = providerTool(target.callPath)
    // The loader names an untitled tool by its path inside its provider.
    if (tool && target.title === target.callPath) target.title = tool.relativePath
    if (options.gone) continue
    const path = functionTarget(target.callPath)
    target.currentHref = tool
      ? `/${workspace}/providers/${tool.namespace}/tools/${tool.relativePath}`
      : `/${workspace}/functions/${path}`
  }
  return { kind: 'discovery', context, response, view }
}
/** The gallery's providers all sit at `coral.providers.<namespace>`, as the loader finds them. */
function providerTool(callPath: string) {
  const match = /^coral\.providers\.([^.]+)\.(.+)$/.exec(callPath)
  return match ? { namespace: match[1], relativePath: match[2] } : null
}

export const searched: Extract<DiscoveryContext, { kind: 'search' }> = {
  kind: 'search',
  query: 'github pulls',
  provider: null,
  limit: 5,
  cursor: null,
}

/**
 * A retained untrimmed page, searched within one provider, so Coral skips Saved
 * Functions. Trimmed pages state only partial or failed coverage.
 */
export const providerFilteredSearch = JSON.stringify({
  ...JSON.parse(stored.untrimmed),
  retriever_statuses: JSON.parse(stored.untrimmed).retriever_statuses.map(
    (status: { retriever: string }) =>
      status.retriever === 'saved_function'
        ? {
            retriever: 'saved_function',
            state: 'skipped',
            coverage: {
              eligible_units: 0,
              searched_units: 0,
              failed_units: 0,
              returned_count: 0,
              has_more: false,
            },
          }
        : status,
  ),
})

/** The console call that writes a log of each level; Reef reads an unknown level as `log`. */
const consoleCall = {
  debug: 'console.debug',
  info: 'console.info',
  warn: 'console.warn',
  error: 'console.error',
  unknown: 'console.log',
} satisfies Record<LogLevel, string>

/**
 * A Run event's row. A log or an emission is stored at its own call in the accepted
 * program; the Run's other events have no call of their own.
 */
export function outputRow(event: RunEvent, label: string): OutputRow {
  const call =
    event.kind === 'program.logged'
      ? consoleCall[event.level]
      : event.kind === 'program.emitted'
        ? 'coral.emit'
        : null
  return {
    kind: 'output',
    name: `${runName}/events/${event.sequence}`,
    workspace,
    run: runName,
    label,
    startedAt: event.occurredAt,
    eventKind: event.kind,
    agent: `${workspace}/agents/collections`,
    task: taskName,
    ...(call && { sourceLocation: programAt(call) }),
  }
}

const programError = (overrides: Partial<ProgramError>): ProgramError => ({
  name: 'Error',
  message: '',
  frames: [],
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
  ...overrides,
})

/** A grouped program error with a cause, tool identity, and bounded content. */
export const groupedError = programError({
  name: 'AggregateError',
  message: '2 of 24 reminders failed',
  frames: [
    { source: 'program.ts', line: 4, column: 11 },
    { source: 'program.ts', line: 2, column: 1 },
  ],
  errors: [
    programError({
      name: 'CoralToolError',
      message: 'Mail rejected the recipient address.',
      toolCode: 'upstream_rejected',
      toolTraceId: '4bf92f3577b34da6a3ce929d0e0e4736',
      frames: [{ source: 'program.ts', line: 4, column: 11 }],
    }),
    programError({
      name: 'TypeError',
      message: "Cannot read properties of undefined (reading 'owner')",
      frames: [{ source: 'program.ts', line: 3, column: 48 }],
      cause: programError({ name: 'Error', message: 'See the grouped error.', circular: true }),
    }),
  ],
  cause: programError({
    name: 'Error',
    message: 'The batch stopped after the first failures.',
    causeOmitted: true,
  }),
  truncated: true,
  omittedErrors: 22,
})

/** The message Coral retains for each Run failure code. */
export const failureMessages = {
  program_rejected: 'The program threw before it returned a value.',
  invalid_program_value: 'The program returned a value that is not JSON: undefined.',
  resource_limit_exceeded: 'The program made more than 1000 tool calls.',
  memory_limit_exceeded: 'The program used more than 64 MiB of memory.',
  program_execution_failed: 'The program failed while it ran.',
  run_authority_lost: 'The agent that started this run lost access to the workspace.',
  unknown: 'The run failed for a reason this version of Reef does not know.',
} satisfies Record<RunFailureCode, string>

/** Opens `selected` in the drawer, over a history that lists it. */
export function panelPage(
  selectedRow: ActivityRow,
  detail: ActivityPage['detail'],
  overrides: Partial<ActivityPage> = {},
): ActivityPage {
  return {
    rows: [selectedRow],
    branches: {},
    expanded: [],
    hasBefore: false,
    hasAfter: false,
    anchorUnavailable: false,
    listUnavailable: false,
    agent: null,
    detail,
    unavailable: false,
    selected: selectedRow.name,
    selectedRow,
    listPath: `/${workspace}/activity`,
    ...overrides,
  }
}

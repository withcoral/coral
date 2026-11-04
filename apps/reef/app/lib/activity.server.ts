import { Code, ConnectError } from '@connectrpc/connect'
import { redirect } from 'react-router'
import {
  ActivityApprovalOutcome,
  ActivityRequiredActionKind,
  ActivityRunState,
  ActivityInvocationState,
  ActivityInvocationOutcome,
  ActivityLogLevel,
  ActivityPrincipalKind,
  ActivityRunFailureCode,
} from '@/generated/coral/management/v1/activity_pb'
import type {
  ActivityTask,
  ActivityActionSummary,
  ActivityInteraction,
  InspectTaskInteractionResponse,
  ActivityRun,
  ActivityInvocation,
  ActivityProgramError,
  ActivityDiscovery,
  InspectApprovalResponse,
  InspectInvocationResponse,
  ActivityRequestReference,
  ActivityRunEvent,
  ActivitySourceLocation,
  InspectRunResponse,
  BrowseRunEntriesResponse,
} from '@/generated/coral/management/v1/activity_pb'
import type {
  TaskRow,
  ActionSummary,
  InteractionRow,
  InteractionEvidence,
  DiscoveryView,
  ActivityPage,
  ActivityRow,
  ActivityTrace,
  SettledApproval,
  SettledPage,
  ExecutionRow,
  InvocationRow,
  ProgramError,
  InvocationResult,
  DiscoveryContext,
  RunState,
  InvocationState,
  InvocationOutcome,
  RequestReference,
  ApprovalDetail,
  ApprovalReading,
  RunEvent,
  RunTerminal,
  RunFailureCode,
  ActionPage,
  RunDetail,
  RecordSourceLocation,
  PendingAction,
} from '@/views/activity/types'
import {
  privateHeaders,
  resourcePath,
  workspaceName,
  type ManagementApi,
} from './management.server'
import { savedFunctionHref } from './management-views.server'
import { listAllTools, listAllProviders, providerPath } from './providers.server'
import { toolOwner } from './call-root'
import { connectionState } from './accounts.server'
import type { RunRow } from '@/views/activity/types'
import { rootOf } from './activity-root'
import { discoveryView, functionTarget } from '@/views/activity/discovery/response'
import { retainRows, type ActivityReadingWindow } from './activity-live-types'
import { ACTIVITY_PAGE_SIZE } from './activity-list'
import { activityPageFetchKey } from './activity-page-fetch'

/** Activity and its requests, and the current catalog that discovery links open. */
export type ActivityApi = Pick<
  ManagementApi,
  'activity' | 'accounts' | 'providers' | 'savedFunctions'
> &
  // Page live mode reads whether the Workspace is archived. Without it, it counts as active.
  Partial<Pick<ManagementApi, 'workspaces'>>
/** One Workspace's list: the caller's own activity, or one Agent's. */
type ActivityScope = { workspace: string; agent?: string }
const runStates = {
  running: ActivityRunState.RUNNING,
  succeeded: ActivityRunState.SUCCEEDED,
  failed: ActivityRunState.FAILED,
  canceled: ActivityRunState.CANCELED,
  timed_out: ActivityRunState.TIMED_OUT,
  interrupted: ActivityRunState.INTERRUPTED,
} satisfies Record<Exclude<RunState, 'unknown'>, ActivityRunState>
const invocationStates = {
  pending: ActivityInvocationState.PENDING,
  dispatching: ActivityInvocationState.DISPATCHING,
  ended: ActivityInvocationState.ENDED,
} satisfies Record<Exclude<InvocationState, 'unknown'>, ActivityInvocationState>
const invocationOutcomes = {
  upstream_outcome: ActivityInvocationOutcome.UPSTREAM_OUTCOME,
  coral_failure: ActivityInvocationOutcome.CORAL_FAILURE,
  run_terminated: ActivityInvocationOutcome.RUN_TERMINATED,
  indeterminate: ActivityInvocationOutcome.INDETERMINATE,
} satisfies Record<Exclude<InvocationOutcome, 'unknown'>, ActivityInvocationOutcome>
function enumLabel<T extends string>(values: Record<T, number>, value: number): T | 'unknown' {
  return (Object.keys(values) as T[]).find((key) => values[key] === value) ?? 'unknown'
}
const timestamp = (value: bigint) => new Date(Number(value / 1_000_000n)).toISOString()
function taskView(task: ActivityTask): TaskRow {
  return {
    name: task.name,
    contextImpactBytes: task.contextImpactBytes?.toString(),
    contextImpactPartial: task.contextImpactPartial,
    workspace: task.workspace,
    intent: task.intent,
    outcome: task.outcome,
    startedAt: timestamp(task.startedAtUnixNanos),
    endedAt: task.endedAtUnixNanos === undefined ? null : timestamp(task.endedAtUnixNanos),
    actor: task.actor,
    agent: task.agent,
  }
}
function interactionView(value: ActivityInteraction): InteractionRow {
  return {
    position: value.position === undefined ? undefined : Number(value.position),
    name: value.name,
    contextImpactBytes: value.contextImpactBytes?.toString(),
    agent: value.agent,
    tool:
      (
        {
          submit: 'exec',
          save: 'save_function',
          delete: 'delete_function',
        } as Record<string, string>
      )[value.kind] ?? value.kind,
    acceptedRun: value.acceptedRun,
    intent: value.intent,
    outcome: value.outcome,
    startedAt: timestamp(value.startedAtUnixNanos),
    completedAt:
      value.completedAtUnixNanos === undefined ? null : timestamp(value.completedAtUnixNanos),
  }
}
function discoveryContextView({ context }: ActivityDiscovery): DiscoveryContext {
  switch (context.case) {
    case 'search':
      return {
        kind: 'search',
        query: context.value.query ?? null,
        provider: context.value.provider ?? null,
        limit: context.value.limit,
        offset: context.value.offset,
      }
    case 'describe':
      return { kind: 'describe', callPath: context.value.callPath, types: [...context.value.types] }
    default:
      return { kind: 'unknown' }
  }
}
/** A catalog list the caller cannot read links nothing, as an empty one would. */
async function listedOrNone<T>(read: () => Promise<T[]>) {
  try {
    return await read()
  } catch (error) {
    if (unavailable(error)) return []
    throw error
  }
}
/**
 * Links each discovery target to today's catalog. A target keeps no link when
 * its tool, provider or function is gone, or when the caller cannot list them.
 */
async function linkCurrentTargets(
  view: DiscoveryView,
  interaction: ActivityInteraction,
  api: ActivityApi,
) {
  const targets =
    view.kind === 'search' ? view.page.hits : view.kind === 'describe' ? [view.description] : []
  const workspace = workspaceOf(interaction.name)
  if (targets.some((target) => target.kind === 'provider_tool')) {
    const providers = await listedOrNone(() => listAllProviders(api, workspace))
    const linked = targets.flatMap((target) => {
      const owner = target.kind === 'provider_tool' ? toolOwner(target.callPath, providers) : null
      if (!owner) return []
      // An untitled tool shows its path inside the provider, as the provider page does.
      if (target.title === target.callPath) target.title = owner.relativePath
      return [{ target, provider: owner.provider.name, path: owner.relativePath }]
    })
    // An update can remove a tool and keep its provider, so read the
    // current tools of each provider that a target names.
    const tools = new Map(
      await Promise.all(
        [...new Set(linked.map(({ provider }) => provider))].map(
          async (provider) =>
            [
              provider,
              new Set(
                (await listedOrNone(() => listAllTools(api, provider))).map(
                  (tool) => tool.relativePath,
                ),
              ),
            ] as const,
        ),
      ),
    )
    for (const { target, provider, path } of linked)
      if (tools.get(provider)?.has(path))
        target.currentHref = `${providerPath(resourcePath(workspace), provider)}/tools/${encodeURIComponent(path)}`
  }
  // Reef opens only the caller's own functions. A function that an Agent's
  // search found belongs to the Agent, so the same path can name another function.
  if (!interaction.agent && targets.some((target) => target.kind === 'saved_function')) {
    const paths = new Set(
      await listedOrNone(async () => {
        const all: string[] = []
        let pageToken = ''
        do {
          const page = await api.savedFunctions.listSavedFunctions({
            parent: workspace,
            pageSize: 100,
            pageToken,
          })
          all.push(...page.savedFunctions.map((savedFunction) => savedFunction.path))
          pageToken = page.nextPageToken
        } while (pageToken)
        return all
      }),
    )
    for (const target of targets) {
      const path = target.kind === 'saved_function' ? functionTarget(target.callPath) : null
      if (path && paths.has(path))
        target.currentHref = savedFunctionHref(resourcePath(workspace), path)
    }
  }
}
async function evidenceView(
  value: InspectTaskInteractionResponse,
  interaction: ActivityInteraction,
  api: ActivityApi,
): Promise<InteractionEvidence> {
  const evidence = value.evidence
  switch (evidence.case) {
    case 'discovery': {
      const context = discoveryContextView(evidence.value)
      const view = discoveryView(context, evidence.value.responseJson)
      await linkCurrentTargets(view, interaction, api)
      return { kind: 'discovery', context, response: evidence.value.responseJson, view }
    }
    case 'rejection':
      return {
        kind: 'rejection',
        code: evidence.value.code,
        message: evidence.value.message,
      }
    case 'run':
      return { kind: 'run', reference: evidence.value }
    case 'savedFunction':
      return { kind: 'function', reference: evidence.value }
    default:
      return { kind: 'unknown' }
  }
}
function executionFacts(
  run: Pick<
    ActivityRun,
    | 'name'
    | 'workspace'
    | 'startedAtUnixNanos'
    | 'endedAtUnixNanos'
    | 'durationNanos'
    | 'requiresAction'
    | 'task'
    | 'actor'
    | 'agent'
    | 'contextImpactBytes'
  >,
): Omit<ExecutionRow, 'state'> {
  return {
    name: run.name,
    contextImpactBytes: run.contextImpactBytes?.toString(),
    workspace: run.workspace,
    agent: run.agent,
    startedAt: timestamp(run.startedAtUnixNanos),
    endedAt: run.endedAtUnixNanos === undefined ? null : timestamp(run.endedAtUnixNanos),
    durationNanos: run.durationNanos.toString(),
    requiresAction: run.requiresAction,
    task: run.task,
    actor: run.actor,
  }
}
function runView(value: ActivityRun): ExecutionRow {
  return {
    ...executionFacts(value),
    state: enumLabel(runStates, value.state),
    intent: value.intent,
    actorLabel: value.actorLabel,
    workspaceLabel: value.workspaceLabel,
    actionPage: value.actionPage
      ? {
          actions: value.actionPage.actions.flatMap(actionSummaryView),
          total: value.actionPage.total.toString(),
          next: value.actionPage.nextAction,
        }
      : undefined,
  }
}
/**
 * The later page of a Run's actions that a "More actions" link asks for, after `cursor`. Coral
 * returns up to 100 actions a page. A page that fails leaves the first page, marked incomplete.
 */
async function laterRunActions(
  api: ActivityApi,
  run: string,
  first: ActionPage,
  cursor: string,
  signal: AbortSignal | undefined,
): Promise<ActionPage> {
  try {
    const value = await api.activity.inspectRun(
      { name: run, pageSize: 1, actionAfter: cursor },
      { signal },
    )
    const page = value.run?.actionPage
    if (page)
      return {
        actions: page.actions.flatMap(actionSummaryView),
        total: page.total.toString(),
        next: page.nextAction,
        after: cursor,
      }
  } catch (error) {
    if (signal?.aborted) throw error
  }
  return { ...first, incomplete: true }
}
function actionSummaryView(value: ActivityActionSummary): ActionSummary[] {
  if (!value.request) return []
  const reference = requestReferenceView(value.request)
  return reference.kind === 'unknown'
    ? []
    : [
        {
          ...reference,
          callPath: value.callPath,
          providerLabel: value.providerLabel,
          deadline: timestamp(value.deadlineAtUnixNanos),
        },
      ]
}
function invocationView(value: ActivityInvocation): InvocationRow {
  return {
    ...executionFacts(value),
    dispatchedAt:
      value.dispatchedAtUnixNanos === undefined ? null : timestamp(value.dispatchedAtUnixNanos),
    waitingNanos:
      value.dispatchedAtUnixNanos === undefined
        ? value.durationNanos.toString()
        : (value.dispatchedAtUnixNanos - value.startedAtUnixNanos).toString(),
    state: enumLabel(invocationStates, value.state),
    run: value.run,
    provider: value.provider,
    tool: value.tool,
    callPath: value.callPath,
    outcome: enumLabel(invocationOutcomes, value.outcome),
    sourceLocation: sourceLocationView(value.sourceLocation),
  }
}
/** A stored location in a document this client knows; anything else is unavailable. */
function sourceLocationView(
  value: ActivitySourceLocation | undefined,
): RecordSourceLocation | undefined {
  if (!value) return undefined
  const { document, line, column } = value
  switch (document.case) {
    case 'program':
      return { document: { kind: 'program' }, line, column }
    case 'savedFunction':
      return { document: { kind: 'saved_function', savedFunction: document.value }, line, column }
    default:
      return undefined
  }
}
function errorView(value: ActivityProgramError): ProgramError {
  return {
    name: value.name,
    message: value.message,
    frames: value.frames.map(({ source, line, column }) => ({
      source,
      line,
      column,
    })),
    toolCode: value.toolCode,
    toolTraceId: value.toolTraceId,
    toolStatus: value.toolStatus,
    toolBody: value.toolBody,
    toolBodyTruncated: value.toolBodyTruncated,
    cause: value.cause ? errorView(value.cause) : null,
    errors: value.errors.map(errorView),
    truncated: value.truncated,
    causeOmitted: value.causeOmitted,
    omittedErrors: value.omittedErrors,
    circular: value.circular,
  }
}
function runEventView(event: ActivityRunEvent): RunEvent {
  const facts = {
    sequence: event.sequence.toString(),
    occurredAt: timestamp(event.occurredAtUnixNanos),
  }
  const { payload } = event
  switch (payload.case) {
    case 'started':
      return { ...facts, kind: 'run.started' }
    case 'invocationStarted':
      return {
        ...facts,
        kind: 'invocation.started',
        invocation: payload.value.invocation,
      }
    case 'invocationEnded':
      return {
        ...facts,
        kind: 'invocation.ended',
        invocation: payload.value.invocation,
        outcome: enumLabel(invocationOutcomes, payload.value.outcome),
      }
    case 'programEmitted':
      return {
        ...facts,
        kind: 'program.emitted',
        value: payload.value.valueJson,
      }
    case 'programLogged':
      return {
        ...facts,
        kind: 'program.logged',
        level: enumLabel(
          {
            debug: ActivityLogLevel.DEBUG,
            info: ActivityLogLevel.INFO,
            warn: ActivityLogLevel.WARN,
            error: ActivityLogLevel.ERROR,
          },
          payload.value.level,
        ),
        message: payload.value.message,
      }
    case 'cancellationRequested':
      return {
        ...facts,
        kind: 'run.cancellation_requested',
        actor: payload.value.actor,
        actorKind: enumLabel(
          {
            user: ActivityPrincipalKind.USER,
            agent: ActivityPrincipalKind.AGENT,
          },
          payload.value.actorKind,
        ),
      }
    case 'ended': {
      const state = enumLabel(runStates, payload.value.state)
      return {
        ...facts,
        kind: 'run.ended',
        state: state === 'running' ? 'unknown' : state,
      }
    }
    default:
      return { ...facts, kind: 'unknown' }
  }
}
function runTerminalView({ terminal }: InspectRunResponse): RunTerminal {
  switch (terminal.case) {
    case 'succeeded':
      return {
        kind: 'succeeded',
        finalValue: terminal.value.finalValueJson ?? null,
      }
    case 'failed':
      return {
        kind: 'failed',
        code: enumLabel(
          {
            program_rejected: ActivityRunFailureCode.PROGRAM_REJECTED,
            invalid_program_value: ActivityRunFailureCode.INVALID_PROGRAM_VALUE,
            resource_limit_exceeded: ActivityRunFailureCode.RESOURCE_LIMIT_EXCEEDED,
            memory_limit_exceeded: ActivityRunFailureCode.MEMORY_LIMIT_EXCEEDED,
            program_execution_failed: ActivityRunFailureCode.PROGRAM_EXECUTION_FAILED,
            run_authority_lost: ActivityRunFailureCode.RUN_AUTHORITY_LOST,
          } satisfies Record<Exclude<RunFailureCode, 'unknown'>, ActivityRunFailureCode>,
          terminal.value.code,
        ),
        message: terminal.value.message,
        error: terminal.value.error ? errorView(terminal.value.error) : null,
      }
    case 'canceled':
      return { kind: 'canceled' }
    case 'timedOut':
      return { kind: 'timed_out' }
    case 'interrupted':
      return { kind: 'interrupted' }
    default:
      return { kind: 'unavailable' }
  }
}
function invocationResultView({ outcome }: InspectInvocationResponse): InvocationResult {
  switch (outcome.case) {
    case 'upstreamOutcomeJson':
      return { kind: 'upstream_outcome', value: outcome.value }
    case 'coralFailure':
      return {
        kind: 'coral_failure',
        code: outcome.value.code,
        message: outcome.value.message,
      }
    case 'runTerminated':
      return { kind: 'run_terminated' }
    case 'indeterminate':
      return { kind: 'indeterminate' }
    default:
      return { kind: 'unknown' }
  }
}
function requestReferenceView({ request }: ActivityRequestReference): RequestReference {
  switch (request.case) {
    case 'approval':
      return { kind: 'approval', name: request.value }
    case 'accountConnection':
      return { kind: 'account_connection', name: request.value }
    default:
      return { kind: 'unknown' }
  }
}
function eventCursor(value: string | null) {
  if (!value) return 0n
  if (!/^\d{1,20}$/.test(value) || BigInt(value) > 2n ** 64n - 1n)
    throw new Response('Invalid event page', {
      status: 400,
      headers: privateHeaders,
    })
  return BigInt(value)
}

function unavailable(error: unknown) {
  return (
    error instanceof ConnectError &&
    (error.code === Code.NotFound || error.code === Code.PermissionDenied)
  )
}
/** A passing Coral fault. Only optional context may leave it out; sign-in, validation and a
 * cancelled load still fail the read. */
export function transient(error: unknown) {
  return (
    error instanceof ConnectError &&
    (error.code === Code.Unavailable || error.code === Code.Internal)
  )
}
function invalid(message: string): never {
  throw new Response(message, { status: 400, headers: privateHeaders })
}
function workspaceOf(name: string) {
  return name.split('/').slice(0, 2).join('/')
}
function recordKind(name: string) {
  if (/^workspaces\/[^/]+\/tasks\/[^/]+\/interactions\/[^/]+$/.test(name)) return 'tool'
  if (/^workspaces\/[^/]+\/runs\/[^/]+\/events\/\d+$/.test(name)) return 'output'
  if (/^workspaces\/[^/]+\/runs\/[^/]+\/final$/.test(name)) return 'final'
  if (/^workspaces\/[^/]+\/tasks\/[^/]+$/.test(name)) return 'task'
  if (/^workspaces\/[^/]+\/runs\/[^/]+$/.test(name)) return 'run'
  if (/^workspaces\/[^/]+\/invocations\/[^/]+$/.test(name)) return 'invocation'
  return invalid('Invalid activity record')
}
const toolRow = (row: ActivityInteraction): ActivityRow => ({
  ...interactionView(row),
  kind: 'tool',
  workspace: workspaceOf(row.name),
  task: row.name.split('/interactions/')[0],
})
function eventLabel(kind: string, sequence: string) {
  switch (kind) {
    case 'program.emitted':
      return `emission #${sequence}`
    case 'program.logged':
      return `log #${sequence}`
    case 'run.started':
      return 'execution accepted'
    case 'run.ended':
      return 'execution ended'
    case 'run.cancellation_requested':
      return 'cancellation requested'
    default:
      return 'run event'
  }
}
function outputRow(
  run: string,
  event: {
    sequence: string
    occurredAt: string
    kind: string
    contextImpactBytes?: bigint
    sourceLocation?: ActivitySourceLocation
  },
  task = '',
  agent = '',
): Extract<ActivityRow, { kind: 'output' }> {
  return {
    kind: 'output',
    contextImpactBytes: event.contextImpactBytes?.toString(),
    name: `${run}/events/${event.sequence}`,
    workspace: workspaceOf(run),
    run,
    label: eventLabel(event.kind, event.sequence),
    startedAt: event.occurredAt,
    eventKind: event.kind,
    task,
    agent,
    sourceLocation: sourceLocationView(event.sourceLocation),
  }
}

/** A Run with its accepted source and outcome, and no request selected. */
function runDetailView(value: InspectRunResponse, run: ExecutionRow): RunDetail {
  return {
    kind: 'run',
    request: null,
    requestUnavailable: false,
    run,
    source: value.submittedSource,
    submission: value.submission ? interactionView(value.submission) : undefined,
    linkedFunctions: value.linkedFunctions.map(({ savedFunction, path, source }) => ({
      savedFunction,
      path,
      source,
    })),
    terminal: runTerminalView(value),
    contextImpact: value.contextImpact && {
      finalValueBytes: value.contextImpact.finalValueBytes.toString(),
      outputBytes: value.contextImpact.outputBytes.toString(),
      toolResultBytes: value.contextImpact.toolResultBytes.toString(),
    },
  }
}

const APPROVAL_NAME = /^workspaces\/[^/]+\/approvals\/[^/]+$/

function approvalView(request: InspectApprovalResponse): ApprovalDetail {
  return {
    kind: 'approval',
    actionable: request.actionable,
    name: request.name,
    run: request.run,
    invocation: request.invocation,
    provider: request.provider,
    providerLabel: request.providerLabel,
    approverLabel: request.approverLabel,
    callPath: request.callPath,
    arguments: request.argumentsJson,
    state: request.state,
    deadline: timestamp(request.deadlineAtUnixNanos),
    approver: request.approver,
    decidedAt:
      request.decidedAtUnixNanos === undefined ? null : timestamp(request.decidedAtUnixNanos),
    comment: request.comment ?? null,
  }
}

/** Adds why the Task started. The Run names its Task only when the caller can see it; an
 * unavailable Task, or a passing fault on this optional read, leaves the intent out, not the
 * review. */
async function addTaskIntent(
  review: ApprovalDetail,
  api: Pick<ActivityApi, 'activity'>,
  task: string,
  signal: AbortSignal,
) {
  try {
    const value = await api.activity.inspectTask({ name: task, pageSize: 1 }, { signal })
    if (value.task?.intent) review.taskIntent = value.task.intent
  } catch (error) {
    if (!unavailable(error) && !transient(error)) throw error
  }
}

async function selectedDetail(
  page: ActivityPage,
  query: URLSearchParams,
  api: ActivityApi,
  signal: AbortSignal,
  selectInitialRequest: boolean,
) {
  const name = page.selected
  if (!name) return
  let retainedConnection: string | undefined
  let selectedRequest = query.get('request') ?? ''
  try {
    switch (recordKind(name)) {
      case 'task': {
        const value = await api.activity.inspectTask({ name, pageSize: 1 }, { signal })
        if (!value.task) throw new ConnectError('unavailable', Code.NotFound)
        const task = taskView(value.task)
        page.detail = { kind: 'task', task, feedback: value.feedback }
        page.selectedRow = { ...task, kind: 'task' }
        break
      }
      case 'tool': {
        const value = await api.activity.inspectTaskInteraction({ name }, { signal })
        if (!value.interaction) throw new ConnectError('unavailable', Code.NotFound)
        page.detail = {
          kind: 'tool',
          task: name.split('/interactions/')[0],
          interaction: interactionView(value.interaction),
          evidence: await evidenceView(value, value.interaction, api),
        }
        page.selectedRow = toolRow(value.interaction)
        break
      }
      case 'output': {
        const [run, sequence] = name.split('/events/')
        const value = await api.activity.inspectRunEvent(
          {
            run,
            sequence: eventCursor(sequence),
          },
          { signal },
        )
        if (!value.event) throw new ConnectError('unavailable', Code.NotFound)
        const event = runEventView(value.event)
        const detail: NonNullable<ActivityPage['detail']> & { kind: 'output' } = {
          kind: 'output',
          run,
          event,
        }
        page.detail = detail
        page.selectedRow = outputRow(
          run,
          {
            ...event,
            contextImpactBytes: value.contextImpactBytes,
            sourceLocation: value.event.sourceLocation,
          },
          value.run?.task,
          value.run?.agent,
        )
        const context = await runSourceContext(run, page, query, api, signal)
        if (context) detail.context = context
        break
      }
      case 'run':
      // A link from before a Run's return value left its rows opens the Run, which shows it.
      case 'final': {
        const run = name.replace(/\/final$/, '')
        const value = await api.activity.inspectRun(
          {
            name: run,
            pageSize: 1,
            // The first page of actions. A "More actions" link reads its later page on its own.
            actionAfter: '',
            accountConnection: /^workspaces\/[^/]+\/accountConnections\/[^/]+$/.test(
              selectedRequest,
            )
              ? selectedRequest
              : '',
          },
          { signal },
        )
        retainedConnection = value.accountConnection || undefined
        if (!value.run) throw new ConnectError('unavailable', Code.NotFound)
        const row = runView(value.run)
        // The Run's list, and a review opened from it, keep the page a "More actions" link named.
        // A separate read, so a page that fails leaves the Run and its exact review readable.
        const cursor = name === run ? query.get('actionsAfter') : null
        if (row.actionPage && cursor)
          row.actionPage = await laterRunActions(api, run, row.actionPage, cursor, signal)
        if (selectInitialRequest && !query.has('request') && name === run && !cursor) {
          const first = row.actionPage?.actions[0]
          if (first?.kind === 'account_connection') {
            selectedRequest = first.name
            page.initialRequest = first.name
          }
        }
        page.detail = runDetailView(value, row)
        page.selected = run
        page.selectedRow = {
          ...row,
          kind: 'run',
          submission: page.detail.submission,
        }
        break
      }
      case 'invocation': {
        const value = await api.activity.inspectInvocation({ name }, { signal })
        if (!value.invocation) throw new ConnectError('unavailable', Code.NotFound)
        const row = invocationView(value.invocation)
        const detail: NonNullable<ActivityPage['detail']> & {
          kind: 'invocation'
        } = {
          kind: 'invocation',
          invocation: row,
          arguments: value.argumentsJson,
          result: invocationResultView(value),
          requests: value.requests.map(requestReferenceView),
          request: null,
          requestUnavailable: false,
        }
        if (selectInitialRequest && !query.has('request') && row.requiresAction) {
          const first = detail.requests.find((reference) => reference.kind === 'approval')
          if (first?.kind === 'approval') {
            selectedRequest = first.name
            page.initialRequest = first.name
          }
        }
        page.detail = detail
        page.selectedRow = { ...row, kind: 'invocation' }
        // An approval review reads this Run itself below; reading it here too would repeat it.
        if (!selectedRequest) {
          const context = await runSourceContext(row.run, page, query, api, signal)
          if (context) detail.context = context
        }
      }
    }
    const detail = page.detail
    if (detail?.kind === 'run' || detail?.kind === 'invocation') {
      if (!selectedRequest) return
      const reference = (
        detail.kind === 'run' ? (detail.run.actionPage?.actions ?? []) : detail.requests
      ).find((ref) => ref.kind !== 'unknown' && ref.name === selectedRequest)
      const run = detail.kind === 'run' ? detail.run.name : detail.invocation.run
      try {
        if (
          reference?.kind === 'approval' ||
          (detail.kind === 'run' && APPROVAL_NAME.test(selectedRequest))
        ) {
          const request = await api.activity.inspectApproval(
            {
              name: selectedRequest,
            },
            { signal },
          )
          if (
            request.run !== run ||
            (detail.kind === 'invocation' && request.invocation !== detail.invocation.name)
          )
            throw new ConnectError('unavailable', Code.NotFound)
          const review = approvalView(request)
          if (detail.kind === 'run') {
            // Approval belongs to this exact tool, not its containing Run.
            // Resolve it in this response rather than redirecting and rereading history.
            const value = await api.activity.inspectInvocation(
              { name: request.invocation },
              { signal },
            )
            if (
              !value.invocation ||
              value.invocation.run !== run ||
              value.invocation.name !== request.invocation
            )
              throw new ConnectError('unavailable', Code.NotFound)
            const invocation = invocationView(value.invocation)
            page.detail = {
              kind: 'invocation',
              invocation,
              arguments: value.argumentsJson,
              result: invocationResultView(value),
              requests: value.requests.map(requestReferenceView),
              request: review,
              requestUnavailable: false,
              context: detail,
            }
            page.selected = invocation.name
            page.selectedRow = { ...invocation, kind: 'invocation' }
            page.initialRecord = invocation.name
          } else {
            // Use the same authorized Run composition without fetching its request twice.
            const context: ActivityPage = {
              ...page,
              selected: run,
              detail: null,
              selectedRow: null,
            }
            const contextQuery = new URLSearchParams(query)
            contextQuery.set('request', '')
            await selectedDetail(context, contextQuery, api, signal, false)
            // Approval review needs its Run. An unavailable Run conceals the approval too.
            if (context.detail?.kind !== 'run') throw new ConnectError('unavailable', Code.NotFound)
            detail.context = context.detail
            detail.request = review
          }
          // Approval review shows why the Task started.
          const owner =
            page.detail?.kind === 'invocation' ? page.detail.context?.run.task : undefined
          if (owner) await addTaskIntent(review, api, owner, signal)
        } else if (
          reference?.kind === 'account_connection' ||
          selectedRequest === retainedConnection
        ) {
          const request = await api.accounts.getAccountConnectionRequest(
            {
              name: selectedRequest,
            },
            { signal },
          )
          detail.request = {
            kind: 'account_connection',
            name: request.name,
            provider: request.provider,
            state: connectionState(request.state),
            expiresAt: request.expireTime
              ? timestamp(
                  request.expireTime.seconds * 1_000_000_000n + BigInt(request.expireTime.nanos),
                )
              : null,
          }
        } else detail.requestUnavailable = true
      } catch (error) {
        if (unavailable(error)) detail.requestUnavailable = true
        else throw error
      }
      // Only a successful approval review reads the Run above. Any other request, or one
      // that is unavailable, leaves the tool with the Run's own authorized source.
      if (detail.kind === 'invocation' && !detail.context) {
        const context = await runSourceContext(run, page, query, api, signal)
        if (context) detail.context = context
      }
    }
  } catch (error) {
    if (!unavailable(error)) throw error
    page.detail = null
    page.selectedRow = null
    page.unavailable = true
  }
}

/**
 * The containing Run's accepted source and terminal evidence, for Activity's source pane.
 * It reads through the same `inspectRun` the Run's own detail uses, so the caller's access
 * to the Run authorizes it independently of the selected record. An unavailable Run, or a
 * passing fault on this optional read, resolves to no context and leaves the record's own
 * detail in place; any other failure propagates like every other read here.
 */
async function runSourceContext(
  run: string,
  page: ActivityPage,
  query: URLSearchParams,
  api: ActivityApi,
  signal: AbortSignal,
): Promise<RunDetail | undefined> {
  const context: ActivityPage = { ...page, selected: run, detail: null, selectedRow: null }
  const contextQuery = new URLSearchParams(query)
  contextQuery.set('request', '')
  try {
    await selectedDetail(context, contextQuery, api, signal, false)
  } catch (error) {
    if (!transient(error)) throw error
    return undefined
  }
  return context.detail?.kind === 'run' ? context.detail : undefined
}

/** Every pending request in `workspace` that waits on the caller, most urgent first, each
 * with a Run that waits on it. A `kind` keeps only that kind. */
export async function pendingActions(
  api: Pick<ManagementApi, 'activity'>,
  workspace: string,
  signal: AbortSignal,
  kind = ActivityRequiredActionKind.UNSPECIFIED,
): Promise<PendingAction[]> {
  const actions: PendingAction[] = []
  let after = ''
  do {
    const value = await api.activity.browseRequiredActions(
      { workspace, after, pageSize: 100, kind },
      { signal },
    )
    for (const { action, run } of value.actions)
      if (action && run)
        for (const summary of actionSummaryView(action))
          actions.push({ action: summary, run: { ...runView(run), kind: 'run' } })
    after = value.nextAfter
  } while (after)
  return actions
}

const settledOutcomes = {
  [ActivityApprovalOutcome.APPROVED]: 'approved',
  [ActivityApprovalOutcome.DECLINED]: 'declined',
  [ActivityApprovalOutcome.EXPIRED]: 'expired',
} as const satisfies Partial<Record<ActivityApprovalOutcome, SettledApproval['outcome']>>

/** One page of `workspace`'s settled approvals that you can inspect, newest first, after
 * `after` or from the newest. A row whose outcome this build does not know is left out. */
export async function settledApprovals(
  api: Pick<ManagementApi, 'activity'>,
  workspace: string,
  after: string,
  signal: AbortSignal,
): Promise<SettledPage> {
  const value = await api.activity.browseSettledApprovals({ workspace, after }, { signal })
  return {
    approvals: value.approvals.flatMap((row): SettledApproval[] => {
      const outcome = settledOutcomes[row.outcome as keyof typeof settledOutcomes]
      if (!outcome || !row.run) return []
      return [
        {
          name: row.name,
          outcome,
          settledAt: timestamp(row.settledAtUnixNanos),
          approverLabel: row.approverLabel,
          callPath: row.callPath,
          providerLabel: row.providerLabel,
          run: { ...runView(row.run), kind: 'run' },
        },
      ]
    }),
    next: value.nextAfter,
  }
}

/**
 * The review of `name`, read as the caller. Null when `name` is no approval of `workspace` or
 * Coral conceals it or its Run: an address grants no authority. Coral keeps every state of a
 * retained request readable, so a settled one still has its review.
 */
export async function approvalReading(
  api: Pick<ManagementApi, 'activity'>,
  workspace: string,
  name: string,
  signal: AbortSignal,
): Promise<ApprovalReading | null> {
  if (!APPROVAL_NAME.test(name) || !name.startsWith(`${workspace}/`)) return null
  try {
    const request = await api.activity.inspectApproval({ name }, { signal })
    const [call, run] = await Promise.all([
      api.activity.inspectInvocation({ name: request.invocation }, { signal }),
      api.activity.inspectRun({ name: request.run, pageSize: 1, actionAfter: '' }, { signal }),
    ])
    if (!run.run || call.invocation?.run !== request.run) return null
    const context = runDetailView(run, runView(run.run))
    const detail = approvalView(request)
    if (context.run.task) await addTaskIntent(detail, api, context.run.task, signal)
    return { detail, context, call: { ...invocationView(call.invocation), kind: 'invocation' } }
  } catch (error) {
    if (unavailable(error)) return null
    throw error
  }
}

/**
 * Before continuous loading, a page link named the record next to it. The reader now
 * opens at an anchor, so the link redirects to the first record of the page it named.
 */
async function redirectToAnchor(request: Request, api: ActivityApi, scope: ActivityScope) {
  const url = new URL(request.url)
  const before = url.searchParams.get('before') ?? ''
  const after = url.searchParams.get('after') ?? ''
  for (const name of [before, after])
    if (name && !['task', 'run'].includes(recordKind(name))) invalid('Invalid activity page')
  url.searchParams.delete('before')
  url.searchParams.delete('after')
  url.searchParams.delete('anchor')
  try {
    const value = await api.activity.browseActivityRoots(
      { scope, before, after, anchor: '', pageSize: ACTIVITY_PAGE_SIZE },
      { signal: request.signal },
    )
    const first = value.roots[0]?.record.value?.name
    if (first) url.searchParams.set('anchor', first)
  } catch (error) {
    // The destination reports an unavailable list itself.
    if (!unavailable(error)) throw error
  }
  throw redirect(`${url.pathname}${url.search}`, { headers: privateHeaders })
}

/** The most records a Coral page holds. */
const TRACE_PAGE = 100
/** The most records one trace reads, so a very long Task cannot hold a page load. */
export const TRACE_LIMIT = 2000

/**
 * One top-level record, a Task or a standalone Run, and the records under it: a Task's tool
 * calls and the Runs they started, and each Run's tool calls and Events, in Coral's order.
 * It reads at most `limit` records; when more remain, the trace is not `complete`.
 */
export async function loadActivityTrace(
  api: ActivityApi,
  root: string,
  {
    scope,
    signal,
    limit = TRACE_LIMIT,
  }: { scope: ActivityScope; signal?: AbortSignal; limit?: number },
): Promise<ActivityTrace> {
  const children: ActivityTrace['children'] = {}
  let left = limit
  // Coral reads a page size of 0 as its default, so a read never asks for fewer than one.
  const pageSize = () => Math.min(TRACE_PAGE, left)
  let complete = true
  const missing: string[] = []
  // A Run's pages. Its own row comes from the first page when the caller has none.
  const readRun = async (name: string, row?: RunRow) => {
    children[name] = []
    let cursor = 0n
    let more = true
    while (more && left) {
      const entries = await api.activity.browseRunEntries(
        { name, after: cursor, pageSize: pageSize(), scope },
        { signal },
      )
      row ??= {
        ...runView(entries.run!),
        kind: 'run',
        submission: entries.submission ? interactionView(entries.submission) : undefined,
      }
      children[name].push(...runChildren(row, entries.entries))
      left -= entries.entries.length
      cursor = entries.nextAfter
      more = entries.hasMore && entries.entries.length > 0
    }
    if (more) complete = false
    return row
  }
  if (recordKind(root) === 'run') {
    const run = await readRun(root)
    return { root: run!, children, complete, unavailable: [] }
  }
  let after = ''
  let task: Extract<ActivityRow, { kind: 'task' }> | undefined
  let feedback: string | undefined
  children[root] = []
  do {
    const value = await api.activity.inspectTask(
      { name: root, after, pageSize: pageSize(), scope },
      { signal },
    )
    task ??= { ...taskView(value.task!), kind: 'task' }
    feedback ??= value.feedback
    for (const interaction of value.interactions)
      children[root].push(
        interaction.run
          ? { ...runView(interaction.run), kind: 'run', submission: interactionView(interaction) }
          : toolRow(interaction),
      )
    left -= value.interactions.length
    // A page with no records ends the read, so a cursor that never moves cannot hold it.
    after = value.interactions.length ? value.nextAfter : ''
    if (after && !left) complete = false
  } while (after && left)
  for (const row of children[root]) {
    if (row.kind !== 'run') continue
    if (!left) {
      complete = false
      break
    }
    try {
      await readRun(row.name, row)
    } catch (error) {
      if (!unavailable(error)) throw error
      delete children[row.name]
      missing.push(row.name)
    }
  }
  return {
    root: task!,
    children,
    complete,
    unavailable: missing,
    ...(feedback && { feedback }),
  }
}

/** A Run's tool calls and Events as rows, in Coral's order. */
function runChildren(
  row: Extract<ActivityRow, { kind: 'run' }>,
  entries: BrowseRunEntriesResponse['entries'],
) {
  return entries.flatMap(({ record }): ActivityRow[] => {
    if (record.case === 'invocation')
      return [
        { ...invocationView(record.value), task: row.task, actor: row.actor, kind: 'invocation' },
      ]
    if (record.case !== 'event') return []
    const event = outputRow(
      row.name,
      {
        sequence: record.value.sequence.toString(),
        kind: record.value.kind,
        contextImpactBytes: record.value.contextImpactBytes,
        occurredAt: timestamp(record.value.occurredAtUnixNanos),
        sourceLocation: record.value.sourceLocation,
      },
      row.task,
      row.agent,
    )
    // A Run end that returned bytes is the Run's return value, which its detail reads.
    if (record.value.kind === 'run.ended' && (record.value.contextImpactBytes ?? 0n) > 0n)
      return [{ ...event, name: `${row.name}/final`, label: 'return value' }]
    return [event]
  })
}

/** The most calls one page reads on their own, to say why they waited or how they failed. */
const CALL_READS = 20
/** A wait this long is worth a reason; shorter ones are the call's admission. */
const LONG_WAIT_MS = 1000

type CallRow = Extract<ActivityRow, { kind: 'invocation' }>
type CallFacts = {
  waitedFor?: 'approval' | 'account connection'
  error?: { code: string; message: string }
}

/** A read the page can do without: a refusal or a passing fault leaves its fact out. */
async function optional<T>(read: () => Promise<T>) {
  try {
    return await read()
  } catch (error) {
    if (unavailable(error) || transient(error)) return undefined
    throw error
  }
}
const byStart = (a: ActivityRow, b: ActivityRow) =>
  Date.parse(a.startedAt) - Date.parse(b.startedAt)

/** A call still waiting on a person, or one that waited long before it ran. */
const waitedLong = (row: CallRow) => {
  const until = row.dispatchedAt ?? row.endedAt
  if (until === null) return row.requiresAction
  return Date.parse(until) - Date.parse(row.startedAt) >= LONG_WAIT_MS
}

/**
 * The function each of the last `CALL_READS` saves in `rows` saved, its path where one of the
 * next `CALL_READS` Runs after the first save called it, and whether such a Run succeeded after
 * the save. A Run records the path of each function it called; a save records only the function.
 */
async function savedFunctions(api: ActivityApi, rows: ActivityRow[], signal: AbortSignal) {
  const saves = rows
    .filter(
      (row) => row.kind === 'tool' && row.tool === 'save_function' && row.outcome === 'succeeded',
    )
    .slice(-CALL_READS)
  const saved = new Map<string, string>()
  await Promise.all(
    saves.map(async (row) => {
      const value = await optional(() =>
        api.activity.inspectTaskInteraction({ name: row.name }, { signal }),
      )
      if (value?.evidence.case === 'savedFunction') saved.set(row.name, value.evidence.value)
    }),
  )
  if (!saved.size) return undefined
  const first = Math.min(
    ...saves.filter((row) => saved.has(row.name)).map((row) => Date.parse(row.startedAt)),
  )
  const paths = new Map<string, string>()
  // When each function was last called by a Run that succeeded.
  const tested = new Map<string, number>()
  await Promise.all(
    rows
      .filter((row) => row.kind === 'run' && Date.parse(row.startedAt) >= first)
      .slice(0, CALL_READS)
      .map(async (row) => {
        const value = await optional(() =>
          api.activity.inspectRun({ name: row.name, pageSize: 1 }, { signal }),
        )
        for (const linked of value?.linkedFunctions ?? []) {
          paths.set(linked.savedFunction, linked.path)
          if (row.kind === 'run' && row.state === 'succeeded')
            tested.set(
              linked.savedFunction,
              Math.max(tested.get(linked.savedFunction) ?? 0, Date.parse(row.startedAt)),
            )
        }
      }),
  )
  const startOf = new Map(saves.map((row) => [row.name, Date.parse(row.startedAt)]))
  return Object.fromEntries(
    [...saved].map(([name, savedFunction]) => [
      name,
      {
        savedFunction,
        ...(paths.has(savedFunction) && { path: paths.get(savedFunction)! }),
        // A Run that succeeded after this save called the function it saved.
        ...((tested.get(savedFunction) ?? 0) >= startOf.get(name)! && { tested: true }),
      },
    ]),
  )
}

/**
 * Reads the calls of `trace` that waited long or failed in Coral, newest first and at most
 * `CALL_READS` of them: the request each waited on, and each failure's error. Then names the
 * last call or Run that failed, with its error and how often that tool failed the same way
 * before it. Last, names the function each save saved: by its path, where a later Run of the
 * trace called it. A read Coral cannot give now leaves its fact out; the timeline still shows
 * the record.
 */
async function traceFacts(
  api: ActivityApi,
  trace: ActivityTrace,
  signal: AbortSignal,
): Promise<Pick<ActivityTrace, 'failure' | 'waitedFor' | 'saves'>> {
  const rows = Object.values(trace.children).flat().toSorted(byStart)
  const calls = rows.filter((row): row is CallRow => row.kind === 'invocation')
  const facts = new Map<string, CallFacts>()
  await Promise.all(
    calls
      .filter((row) => row.outcome === 'coral_failure' || waitedLong(row))
      .toReversed()
      .slice(0, CALL_READS)
      .map(async (row) => {
        const value = await optional(() =>
          api.activity.inspectInvocation({ name: row.name }, { signal }),
        )
        if (!value) return
        const kinds = value.requests.map((request) => requestReferenceView(request).kind)
        const result = invocationResultView(value)
        facts.set(row.name, {
          ...(kinds.includes('approval')
            ? { waitedFor: 'approval' as const }
            : kinds.includes('account_connection') && { waitedFor: 'account connection' as const }),
          ...(result.kind === 'coral_failure' && {
            error: { code: result.code, message: result.message },
          }),
        })
      }),
  )
  const waitedFor = Object.fromEntries(
    [...facts].flatMap(([name, fact]) => (fact.waitedFor ? [[name, fact.waitedFor]] : [])),
  )
  const result: Pick<ActivityTrace, 'failure' | 'waitedFor' | 'saves'> = Object.keys(waitedFor)
    .length
    ? { waitedFor }
    : {}
  const saves = await savedFunctions(api, rows, signal)
  if (saves) result.saves = saves
  const last = rows
    .filter(
      (row) =>
        (row.kind === 'invocation' && row.outcome === 'coral_failure') ||
        (row.kind === 'run' && row.state === 'failed'),
    )
    .at(-1)
  if (last?.kind === 'invocation') {
    const fact = facts.get(last.name)
    if (!fact?.error) return result
    const same = calls.filter((row) => {
      const earlier = facts.get(row.name)?.error
      return (
        row !== last &&
        Date.parse(row.startedAt) <= Date.parse(last.startedAt) &&
        row.callPath === last.callPath &&
        earlier?.code === fact.error!.code &&
        earlier.message === fact.error!.message
      )
    }).length
    result.failure = {
      record: last.name,
      label: last.callPath || last.tool,
      ...fact.error,
      ...(fact.waitedFor && { waitedFor: fact.waitedFor }),
      ...(same > 0 && { sameBefore: same }),
    }
  } else if (last?.kind === 'run') {
    const value = await optional(() =>
      api.activity.inspectRun({ name: last.name, pageSize: 1 }, { signal }),
    )
    const terminal = value && runTerminalView(value)
    if (terminal?.kind === 'failed')
      result.failure = {
        record: last.name,
        label: last.submission?.intent || last.intent || last.name,
        code: terminal.code,
        message: terminal.message,
      }
  }
  return result
}

export async function loadActivity(
  request: Request,
  api: ActivityApi,
  options: {
    scope: ActivityScope
    run?: string
    reading?: ActivityReadingWindow
    /** Where the reader navigates when it is embedded outside the Activity page. */
    listPath?: string
    /** False skips the open root's records, for a list that shows no timeline. */
    trace?: boolean
  },
): Promise<ActivityPage> {
  const query = new URL(request.url).searchParams
  const { signal } = request
  // Observation names its reading window directly; navigation names only an anchor.
  const reading = options.reading
  if (
    !reading &&
    (query.has('before') || query.has('after')) &&
    query.get(activityPageFetchKey) !== 'page'
  )
    await redirectToAnchor(request, api, options.scope)
  const namedRoot = query.get('root')
  if (namedRoot && !['task', 'run'].includes(recordKind(namedRoot)))
    invalid('Invalid activity root')
  const rootAnchor = query.get('anchor')
  if (rootAnchor && !['task', 'run'].includes(recordKind(rootAnchor)))
    invalid('Invalid activity anchor')
  const selectedRecord = query.get('record')
  const selected =
    options.run && selectedRecord && recordKind(selectedRecord) === 'invocation'
      ? selectedRecord
      : (options.run ?? selectedRecord)
  if (selected) recordKind(selected)
  const { scope } = options
  const page: ActivityPage = {
    rows: [],
    hasBefore: false,
    hasAfter: false,
    anchorUnavailable: false,
    listUnavailable: false,
    agent: scope.agent ?? null,
    detail: null,
    unavailable: false,
    selected,
    selectedRow: null,
    listPath:
      options.listPath ?? (scope.agent ? `/${scope.agent}` : `/${scope.workspace}/activity`),
    trace: null,
    readAt: Date.now(),
  }
  // Exact selection does not borrow authority from the surrounding list.
  await selectedDetail(page, query, api, signal, !options.reading)
  if (
    options.run &&
    page.detail?.kind === 'invocation' &&
    page.detail.invocation.run !== options.run
  ) {
    page.detail = null
    page.selectedRow = null
    page.unavailable = true
    page.initialRecord = undefined
    page.initialRequest = undefined
  }
  try {
    const value = await api.activity.browseActivityRoots(
      {
        scope,
        before: reading ? '' : (query.get('before') ?? ''),
        after: reading ? '' : (query.get('after') ?? ''),
        // A page request continues the list from its cursor alone.
        anchor: reading || query.has('before') || query.has('after') ? '' : (rootAnchor ?? ''),
        retained: reading && !reading.followRoot ? { names: reading.roots } : undefined,
        pageSize: ACTIVITY_PAGE_SIZE,
      },
      { signal },
    )
    page.rows = value.roots.flatMap(({ record }): ActivityRow[] => {
      if (record.case === 'task') return [{ ...taskView(record.value), kind: 'task' }]
      if (record.case === 'run') return [{ ...runView(record.value), kind: 'run' }]
      return []
    })
    if (reading && !reading.followRoot) page.rows = retainRows(page.rows, reading.roots)
    page.hasBefore = value.hasBefore
    page.hasAfter = value.hasAfter
    page.anchorUnavailable = value.anchorUnavailable
  } catch (error) {
    if (!unavailable(error)) throw error
    page.rows = []
    page.listUnavailable = true
  }
  // The root the address names, else the selected record's, else the newest in the list.
  const root = namedRoot || (page.selectedRow && rootOf(page.selectedRow)) || page.rows[0]?.name
  // A further page of the list joins the list the reader shows; its timeline is already open.
  const listPage = query.get(activityPageFetchKey) === 'page'
  if (root && options.trace !== false && !listPage) {
    try {
      const trace = await loadActivityTrace(api, root, { scope, signal })
      page.trace = { ...trace, ...(await traceFacts(api, trace, signal)) }
    } catch (error) {
      // The timeline is optional context: a refusal or a passing fault hides it and keeps the page.
      if (unavailable(error)) page.traceUnavailable = 'refused'
      else if (transient(error)) page.traceUnavailable = 'failed'
      else throw error
    }
  }
  return page
}

// A Run identifier: four or five lowercase words of at most 20 letters joined
// by hyphens, or the canonical UUID of a Run from before Coral named Runs with
// words. The same pattern as `SCHEMA_PATTERN` in `crates/coral-identifier`.
const RUN_IDENTIFIER =
  /^([a-z]{1,20}(-[a-z]{1,20}){3,4}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/

export function loadRunActivity(
  request: Request,
  api: ActivityApi,
  params: { workspace: string; run: string },
) {
  const workspace = workspaceName(params.workspace)
  if (!RUN_IDENTIFIER.test(params.run))
    throw new Response('Not found', { status: 404, headers: privateHeaders })
  return loadActivity(request, api, {
    scope: { workspace },
    run: `${workspace}/runs/${params.run}`,
  })
}

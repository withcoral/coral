import { Code, ConnectError } from '@connectrpc/connect'
import { redirect } from 'react-router'
import {
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
  InspectInvocationResponse,
  ActivityRequestReference,
  ActivityRunEvent,
  ActivitySourceLocation,
  InspectRunResponse,
} from '@/generated/coral/management/v1/activity_pb'
import type {
  TaskRow,
  ActionSummary,
  InteractionRow,
  InteractionEvidence,
  DiscoveryView,
  ActivityPage,
  ActivityRow,
  ActivityBranch,
  ExecutionRow,
  InvocationRow,
  ProgramError,
  InvocationResult,
  DiscoveryContext,
  RunState,
  InvocationState,
  InvocationOutcome,
  RequestReference,
  RequestDetail,
  RunEvent,
  RunTerminal,
  RunFailureCode,
  ActionPage,
  RunDetail,
  RecordSourceLocation,
  LiveAction,
  LiveLatest,
  PendingAction,
} from '@/views/activity/types'
import {
  privateHeaders,
  resourcePath,
  workspaceName,
  type ManagementApi,
} from './management.server'
import { savedFunctionHref } from './management-views.server'
import { listAllOperations, listAllProviders, providerPath } from './providers.server'
import { operationOwner } from './call-root'
import { connectionState } from './accounts.server'
import { liveScopeOf, MAX_EXPANDED_RECORDS, type RunRow } from '@/views/activity/types'
import {
  childWindow,
  liveHold,
  recentTask,
  unfinishedRun,
  type TaskRecord,
} from '@/views/activity/navigation'
import { WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { discoveryView, functionTarget } from '@/views/activity/discovery/response'
import {
  ACTIVITY_CHILD_WINDOW,
  retainRows,
  type ActivityReadingWindow,
} from './activity-live-types'
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
        cursor: context.value.cursor ?? null,
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
 * its operation, provider or function is gone, or when the caller cannot list them.
 */
async function linkCurrentTargets(
  view: DiscoveryView,
  interaction: ActivityInteraction,
  api: ActivityApi,
) {
  const targets =
    view.kind === 'search' ? view.page.hits : view.kind === 'describe' ? [view.description] : []
  const workspace = workspaceOf(interaction.name)
  if (targets.some((target) => target.kind === 'operation')) {
    const providers = await listedOrNone(() => listAllProviders(api, workspace))
    const linked = targets.flatMap((target) => {
      const owner = target.kind === 'operation' ? operationOwner(target.callPath, providers) : null
      if (!owner) return []
      // An untitled operation shows its path inside the provider, as the provider page does.
      if (target.title === target.callPath) target.title = owner.relativePath
      return [{ target, provider: owner.provider.name, path: owner.relativePath }]
    })
    // An update can remove an operation and keep its provider, so read the
    // current operations of each provider that a target names.
    const operations = new Map(
      await Promise.all(
        [...new Set(linked.map(({ provider }) => provider))].map(
          async (provider) =>
            [
              provider,
              new Set(
                (await listedOrNone(() => listAllOperations(api, provider))).map(
                  (operation) => operation.relativePath,
                ),
              ),
            ] as const,
        ),
      ),
    )
    for (const { target, provider, path } of linked)
      if (operations.get(provider)?.has(path))
        target.currentHref = `${providerPath(resourcePath(workspace), provider)}/ops/${encodeURIComponent(path)}`
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
    operation: value.operation,
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
    operationCode: value.operationCode,
    operationTraceId: value.operationTraceId,
    operationStatus: value.operationStatus,
    operationBody: value.operationBody,
    operationBodyTruncated: value.operationBodyTruncated,
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
function transient(error: unknown) {
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
        const terminal = runTerminalView(value)
        if (
          name.endsWith('/final') &&
          (terminal.kind !== 'succeeded' || terminal.finalValue === null)
        ) {
          throw new ConnectError('unavailable', Code.NotFound)
        }
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
        page.detail = {
          kind: 'run',
          request: null,
          requestUnavailable: false,
          run: row,
          source: value.submittedSource,
          submission: value.submission ? interactionView(value.submission) : undefined,
          linkedFunctions: value.linkedFunctions.map(({ savedFunction, path, source }) => ({
            savedFunction,
            path,
            source,
          })),
          terminal,
        }
        page.selected = run
        page.selectedRow = {
          ...row,
          kind: 'run',
          submission: page.detail.submission,
          hasFinalValue: terminal.kind === 'succeeded' && terminal.finalValue !== null,
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
          (detail.kind === 'run' && /^workspaces\/[^/]+\/approvals\/[^/]+$/.test(selectedRequest))
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
          const review: Extract<RequestDetail, { kind: 'approval' }> = {
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
              request.decidedAtUnixNanos === undefined
                ? null
                : timestamp(request.decidedAtUnixNanos),
            comment: request.comment ?? null,
          }
          if (detail.kind === 'run') {
            // Approval belongs to this exact operation, not its containing Run.
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
          // Approval review shows why the Task started. The Run names its Task only when the
          // caller can see it; an unavailable Task, or a passing fault on this optional read,
          // leaves the intent out, not the review.
          const owner =
            page.detail?.kind === 'invocation' ? page.detail.context?.run.task : undefined
          if (owner) {
            try {
              const value = await api.activity.inspectTask({ name: owner, pageSize: 1 }, { signal })
              if (value.task?.intent) review.taskIntent = value.task.intent
            } catch (error) {
              if (!unavailable(error) && !transient(error)) throw error
            }
          }
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
      // that is unavailable, leaves the operation with the Run's own authorized source.
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

/** Children one scan read takes, Coral's largest page. */
const ACTION_SCAN_PAGE = 100
/** Top-level records one read of page live mode takes, Coral's largest page. */
const ROOT_SCAN_PAGE = 100

/**
 * The oldest action you can take in the selected record's Run, while live mode follows
 * it, or in the read after an approval review's decision. It reads every page of the
 * Run's current requests from the first, and times each by the operation that waits on
 * it: the earliest start, then the lowest operation identity, decides across every
 * page. An approval counts only when you can decide it, and an account connection only
 * while it is pending. A request Reef cannot read is skipped.
 */
async function oldestRunAction(
  page: ActivityPage,
  query: URLSearchParams,
  api: ActivityApi,
  scope: ActivityScope,
  signal: AbortSignal,
  snapshot: boolean,
): Promise<LiveAction | undefined> {
  const detail = page.detail
  const context =
    detail?.kind === 'run'
      ? detail
      : detail?.kind === 'invocation' || detail?.kind === 'output'
        ? detail.context
        : undefined
  // After an approval decision, the read that follows it advances the review to the next
  // action. A live snapshot of the review never does, so it reads nothing for it.
  const settled =
    !snapshot &&
    detail?.kind === 'invocation' &&
    detail.request?.kind === 'approval' &&
    detail.request.state !== 'pending'
  const actions = context?.run.actionPage
  if (!context || !actions || actions.total === '0') return undefined
  if (!liveScopeOf(query.get('live')) && !settled) return undefined
  // The first action page, whichever page the review shows.
  const first = query.get('actionsAfter') ? undefined : actions
  const timed = await runActions(context.run.name, first, api, scope, signal)
  return timed.toSorted(older)[0]?.action
}

/**
 * What page live mode needs past the list and its loaded windows, while it follows the
 * newest activity with no Run that runs open. It reads every top-level record in the
 * list's scope, past the loaded list too, and only the Runs it may follow: a standalone
 * Run at any age, or one of a Task that started `TAKS_MAX_AGE` ago or less.
 * - `action`: the oldest action you can take in any of those Runs. Each is timed as
 *   `oldestRunAction` times it, by the operation that waits on it, so the operation
 *   that started waiting first wins, not the request made first: Coral states no
 *   request time. The candidates are the Runs that run and wait on you that the list,
 *   a loaded window or your queue of required actions shows. Your queue lists a shared
 *   connection once, under one Run, so while it holds a connection, every Task whose
 *   row says a Run of it runs is read in full for the other Runs that wait. That read
 *   also finds its newest Run that runs, so no window is read twice. An archived
 *   workspace takes no action, so it reads none.
 * - `running`: every standalone Run that runs, and by Task, its newest Run that runs,
 *   read back from its end one window at a time.
 * - `newest`: the newest top-level record, when the page does not hold it, such as a
 *   snapshot of only the records you watch.
 * - `complete`: it read every top-level record and every Task it looked in, so
 *   `running` names every Run that runs that it may follow.
 * It reads the Runs of a Task only while its row in the list says a child Run runs
 * (`contextImpactPartial`). In a top-level record of this list, Coral sets it from the
 * state of each child Run you may see, apart from its byte measurement (coral-task
 * `context_totals`, coral-run `history_repository::running`). A Task read elsewhere
 * can carry no impact at all. The flag can lag a Run's end, so only the Run's own
 * record counts as running.
 */
async function liveLatestWork(
  page: ActivityPage,
  newestPage: ActivityRow[] | undefined,
  query: URLSearchParams,
  api: ActivityApi,
  scope: ActivityScope,
  signal: AbortSignal,
): Promise<LiveLatest | undefined> {
  // A Run that runs holds live mode, whatever waits elsewhere. A page that newer
  // records come before reads the newest first. A snapshot of only the records you
  // watch says newer ones exist too, which this reads from the top.
  if (liveScopeOf(query.get('live')) !== 'latest' || liveHold(page)) return undefined
  if (newestPage && page.hasBefore) return undefined
  const now = Date.now()
  const { roots, complete } = await everyRoot()
  const tasks = roots.filter(
    (row): row is TaskRecord =>
      row.kind === 'task' && recentTask(row, now) && Boolean(row.contextImpactPartial),
  )
  // The Runs that wait on you, with their Task.
  const asking = new Map<string, string>()
  // By Task, its newest Run that runs, as `readRuns` read it.
  const newestRunning = new Map<string, RunRow | undefined>()
  // Whether `readRuns` read every Task it looked in.
  let tasksRead = true
  for (const row of roots)
    if (unfinishedRun(page, row) && row.requiresAction) asking.set(row.name, '')
  for (const task of tasks)
    for (const row of page.branches[task.name]?.rows ?? [])
      if (unfinishedRun(page, row) && row.requiresAction) asking.set(row.name, task.name)
  const found: LiveLatest = {}
  const actionable =
    (asking.size > 0 || tasks.length > 0) &&
    (!api.workspaces ||
      (await api.workspaces.getWorkspace({ name: scope.workspace }, { signal })).state !==
        WorkspaceState.ARCHIVED)
  if (actionable) {
    const queue = tasks.length ? await requiredActions(page, query, api, scope, signal) : []
    const eligible = new Set(tasks.map((task) => task.name))
    for (const { run } of queue)
      if (unfinishedRun(page, run) && (eligible.has(run.task) || asking.has(run.name)))
        asking.set(run.name, run.task)
    if (queue.some(({ action }) => action.kind === 'account_connection'))
      for (const task of tasks) await readRuns(task.name, { waiting: true })
    const timed = await Promise.all(
      [...asking.keys()].map((run) => runActions(run, undefined, api, scope, signal)),
    )
    const oldest = timed.flat().toSorted(older)[0]
    if (oldest) found.action = { task: asking.get(oldest.action.run) ?? '', action: oldest.action }
  }
  const listed = new Set(page.rows.map((row) => row.name))
  for (const row of roots)
    if (row.kind === 'run' && unfinishedRun(page, row)) (found.running ??= {})[row.name] = row
  for (const task of tasks) {
    if (!newestRunning.has(task.name)) await readRuns(task.name, { waiting: false })
    const run = newestRunning.get(task.name)
    if (run) (found.running ??= {})[task.name] = run
  }
  if (roots[0] && !listed.has(roots[0].name)) found.newest = roots[0].name
  if (complete && tasksRead && roots.length) found.complete = true
  return found.action || found.running || found.newest || found.complete ? found : undefined

  /**
   * Reads the Runs of `task` from its newest back, once per snapshot, for its newest
   * Run that runs. With `waiting`, it reads every Run, for the Runs that wait on you too.
   */
  async function readRuns(task: string, { waiting }: { waiting: boolean }) {
    let run: RunRow | undefined
    const read = await newestRunsOf(task, (row) => {
      if (!unfinishedRun(page, row)) return false
      run ??= row
      if (waiting && row.requiresAction) asking.set(row.name, task)
      return !waiting
    })
    newestRunning.set(task, run)
    // A read cut short still found the newest Run that runs, when it found one.
    if (!read && !run) tasksRead = false
  }

  /**
   * Every top-level record in the list's scope, newest first: the newest page the list
   * loaded, then every older page. A standalone Run counts at any age.
   */
  async function everyRoot(): Promise<{ roots: ActivityRow[]; complete: boolean }> {
    const rows = [...(newestPage ?? [])]
    let more = newestPage ? page.hasAfter : true
    while (more) {
      let value
      try {
        value = await api.activity.browseActivityRoots(
          { scope, after: rows.at(-1)?.name ?? '', pageSize: ROOT_SCAN_PAGE },
          { signal },
        )
      } catch (error) {
        // An older page Reef cannot read hides what is past it, until the next snapshot.
        if (!unavailable(error)) throw error
        return { roots: rows, complete: false }
      }
      for (const { record } of value.roots) {
        if (record.case === 'task') rows.push({ ...taskView(record.value), kind: 'task' })
        if (record.case === 'run') rows.push({ ...runView(record.value), kind: 'run' })
      }
      more = value.hasAfter && value.roots.length > 0
    }
    return { roots: rows, complete: true }
  }

  /**
   * Visits the Runs of `task` from its newest back until `visit` says stop: those its
   * loaded window holds through its end, then the rest read from Coral. False when Reef
   * could not read a window before it stopped.
   */
  async function newestRunsOf(task: string, visit: (row: RunRow) => boolean) {
    const branch = page.expanded.includes(task) ? page.branches[task] : undefined
    let offset: number | undefined
    if (branch && !branch.unavailable) {
      const { items, total } = childWindow(branch)
      const end = Math.max(branch.offset ?? 0, ...items.map(({ index }) => index + 1))
      if (end >= total) {
        for (const { row } of items.toSorted((a, b) => b.index - a.index))
          if (row.kind === 'run' && visit(row)) return true
        offset = branch.offset ?? 0
      }
    }
    if (offset === undefined) {
      const head = await taskRead(task)
      if (!head) return false
      offset = Number(head.total)
    }
    while (offset > 0) {
      const from = Math.max(0, offset - ACTIVITY_CHILD_WINDOW)
      const value = await taskRead(task, from)
      if (!value) return false
      for (const item of value.interactions.slice(0, offset - from).toReversed())
        if (item.run && visit({ ...runView(item.run), kind: 'run' })) return true
      offset = from
    }
    return true
  }

  /** The Task `name`, with its children from `offset`, or undefined when Reef cannot read it. */
  async function taskRead(name: string, offset?: number) {
    try {
      return await api.activity.inspectTask(
        {
          name,
          pageSize: offset === undefined ? 1 : ACTIVITY_CHILD_WINDOW,
          scope,
          viewport:
            offset === undefined ? undefined : { offset: BigInt(offset), anchor: '', preceding: 0 },
        },
        { signal },
      )
    } catch (error) {
      if (!unavailable(error)) throw error
      return undefined
    }
  }
}

/**
 * Every request in your queue of required actions in the workspace, with a Run that
 * waits on it. It takes the page the pending actions already read when that holds them
 * all, and reads the rest a page at a time otherwise.
 */
async function requiredActions(
  page: ActivityPage,
  query: URLSearchParams,
  api: ActivityApi,
  scope: ActivityScope,
  signal: AbortSignal,
): Promise<PendingAction[]> {
  const shown = page.pending
  if (
    shown &&
    !shown.unavailable &&
    !query.get('pendingAfter') &&
    shown.actions.length >= Number(shown.actionCount)
  )
    return shown.actions
  const actions: PendingAction[] = []
  let after = ''
  do {
    const value = await api.activity.browseRequiredActions(
      { workspace: scope.workspace, after, pageSize: 100 },
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

/**
 * Every action you can take in `run`, each timed by the operation that waits on it. It
 * reads every page of the Run's current requests from `first`, or from the start. An
 * approval counts only when you can decide it, and an account connection only while it
 * is pending. A request Reef cannot read is skipped.
 */
async function runActions(
  run: string,
  first: ActionPage | undefined,
  api: ActivityApi,
  scope: ActivityScope,
  signal: AbortSignal,
): Promise<TimedAction[]> {
  try {
    const requests: ActionSummary[] = []
    let shown = first
    let after = ''
    do {
      if (!shown) {
        const value = await api.activity.inspectRun(
          { name: run, pageSize: 1, actionAfter: after },
          { signal },
        )
        shown = value.run ? runView(value.run).actionPage : undefined
      }
      requests.push(...(shown?.actions ?? []))
      after = shown?.next ?? ''
      shown = undefined
    } while (after)
    const approvals = await Promise.all(
      requests.flatMap((action) =>
        action.kind === 'approval' ? [approvalAction(action.name, run, api, signal)] : [],
      ),
    )
    const timed = approvals.filter((action) => action !== undefined)
    const connections = requests.flatMap((action) =>
      action.kind === 'account_connection' ? [action.name] : [],
    )
    if (connections.length)
      timed.push(...(await connectionActions(connections, run, api, scope, signal)))
    return timed
  } catch (error) {
    if (!unavailable(error)) throw error
    return []
  }
}

/** An action, with when its operation started and that operation's identity. */
interface TimedAction {
  startedAt: bigint
  operation: string
  action: LiveAction
}
/** Earlier start first, then the lower operation identity, then the lower request. */
const older = (a: TimedAction, b: TimedAction) =>
  Number(a.startedAt > b.startedAt) - Number(a.startedAt < b.startedAt) ||
  Number(a.operation > b.operation) - Number(a.operation < b.operation) ||
  Number(a.action.request > b.action.request) - Number(a.action.request < b.action.request)

/**
 * The approval `name`, when you can decide it and its operation in `run` still waits on
 * it. The approval names its operation, which says when it started.
 */
async function approvalAction(
  name: string,
  run: string,
  api: ActivityApi,
  signal: AbortSignal,
): Promise<TimedAction | undefined> {
  try {
    const approval = await api.activity.inspectApproval({ name }, { signal })
    if (!approval.actionable || approval.run !== run) return undefined
    const { invocation } = await api.activity.inspectInvocation(
      { name: approval.invocation },
      { signal },
    )
    if (!invocation || invocation.run !== run || !invocation.requiresAction) return undefined
    return {
      startedAt: invocation.startedAtUnixNanos,
      operation: invocation.name,
      action: { run, record: invocation.name, request: name },
    }
  } catch (error) {
    if (!unavailable(error)) throw error
    return undefined
  }
}

/**
 * The pending connections among `names`, each timed by its oldest waiting operation.
 * Coral exposes neither a request time nor an operation link on a connection. Read
 * every child page: admission order cannot break equal-time ties across pages, and
 * several operations can share one connection. This scan runs only for pending,
 * readable connections; approvals use exact reads instead of scanning history.
 */
async function connectionActions(
  names: string[],
  run: string,
  api: ActivityApi,
  scope: ActivityScope,
  signal: AbortSignal,
): Promise<TimedAction[]> {
  const states = await Promise.all(
    names.map(async (name) => {
      try {
        const value = await api.accounts.getAccountConnectionRequest({ name }, { signal })
        return connectionState(value.state) === 'pending' ? [name] : []
      } catch (error) {
        if (!unavailable(error)) throw error
        return []
      }
    }),
  )
  const pending = new Set(states.flat())
  const found = new Map<string, TimedAction>()
  for (let after = 0n; pending.size;) {
    const value = await api.activity.browseRunEntries(
      { name: run, after, pageSize: ACTION_SCAN_PAGE, scope },
      { signal },
    )
    const waiting = value.entries.flatMap(({ record }) =>
      record.case === 'invocation' && record.value.requiresAction ? [record.value] : [],
    )
    const read = await Promise.all(
      waiting.map((operation) =>
        api.activity.inspectInvocation({ name: operation.name }, { signal }).catch((error) => {
          if (!unavailable(error)) throw error
          return undefined
        }),
      ),
    )
    const candidates = waiting.flatMap((operation, index) =>
      (read[index]?.requests.map(requestReferenceView) ?? []).flatMap((reference) =>
        reference.kind === 'account_connection' && pending.has(reference.name)
          ? [
              {
                startedAt: operation.startedAtUnixNanos,
                operation: operation.name,
                action: { run, record: run, request: reference.name },
              },
            ]
          : [],
      ),
    )
    for (const candidate of candidates) {
      const previous = found.get(candidate.action.request)
      if (!previous || older(candidate, previous) < 0)
        found.set(candidate.action.request, candidate)
    }
    if (!value.hasMore || value.nextAfter <= after) break
    after = value.nextAfter
  }
  return [...found.values()]
}

const missingBranch = (): ActivityBranch => ({ rows: [], next: '', unavailable: true })

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

export async function loadActivity(
  request: Request,
  api: ActivityApi,
  options: {
    scope: ActivityScope
    run?: string
    reading?: ActivityReadingWindow
    /** Where the reader navigates when it is embedded outside the Activity page. */
    listPath?: string
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
  const expanded = [...new Set(query.getAll('expand'))]
  if (expanded.length > MAX_EXPANDED_RECORDS) invalid('Expand at most 20 records at a time')
  for (const name of expanded)
    if (!['task', 'run'].includes(recordKind(name))) invalid('Invalid expanded record')
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
    branches: {},
    expanded,
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
  }
  const automaticAnchors = new Map<string, string>()
  const anchor = (name: string) =>
    query.getAll(`cursor.${name}`).length
      ? ''
      : (query.get(`anchor.${name}`) ?? automaticAnchors.get(name) ?? '')
  const viewport = (name: string) => {
    const value = reading?.branches[name]
    if (value) return { ...value, offset: BigInt(value.offset) }
    const offset = query.get(`offset.${name}`)
    if (offset === null || (anchor(name) && !query.has(`preceding.${name}`))) return undefined
    const index = Number(offset)
    const preceding = Number(query.get(`preceding.${name}`) ?? 0)
    if (
      !Number.isSafeInteger(index) ||
      index < 0 ||
      !Number.isInteger(preceding) ||
      preceding < 0 ||
      preceding >= ACTIVITY_CHILD_WINDOW
    )
      invalid('Invalid activity viewport')
    return { offset: BigInt(index), anchor: anchor(name), preceding }
  }
  const taskPage = (name: string) =>
    api.activity.inspectTask(
      {
        name,
        after: viewport(name) ? '' : (query.getAll(`cursor.${name}`).at(-1) ?? ''),
        pageSize: ACTIVITY_CHILD_WINDOW,
        scope,
        anchor: viewport(name) ? '' : anchor(name),
        viewport: viewport(name),
      },
      { signal },
    )
  const runPage = (name: string) =>
    api.activity.browseRunEntries(
      {
        name,
        after: viewport(name) ? 0n : eventCursor(query.getAll(`cursor.${name}`).at(-1) ?? null),
        pageSize: ACTIVITY_CHILD_WINDOW,
        scope,
        anchor: viewport(name) ? '' : anchor(name),
        viewport: viewport(name),
      },
      { signal },
    )
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
  if (page.initialRecord && page.detail?.kind === 'invocation') {
    const run = page.detail.invocation.run
    automaticAnchors.set(run, page.initialRecord)
    query.set(`anchor.${run}`, page.initialRecord)
    for (const prefix of ['cursor', 'offset', 'preceding']) query.delete(`${prefix}.${run}`)
    if (!page.expanded.includes(run) && page.expanded.length < MAX_EXPANDED_RECORDS)
      page.expanded.push(run)
  }
  const liveAction = await oldestRunAction(page, query, api, scope, signal, Boolean(reading))
  if (liveAction) page.liveAction = liveAction
  // Initial direct links reveal only their bounded ancestor windows. Explicit tree/page state wins.
  if (!query.has('tree') && !query.has('expand') && page.selectedRow) {
    const row = page.selectedRow
    const task = 'task' in row ? (row.task ?? '') : ''
    const run = row.kind === 'invocation' || row.kind === 'output' ? row.run : ''
    if (task) automaticAnchors.set(task, run || row.name)
    if (run) automaticAnchors.set(run, row.name)
    for (const name of [task, run])
      if (name && !page.expanded.includes(name)) page.expanded.push(name)
  }
  if (!scope.agent) {
    try {
      const value = await api.activity.browseRequiredActions(
        {
          workspace: scope.workspace,
          after: query.get('pendingAfter') ?? '',
          pageSize: 10,
        },
        { signal },
      )
      page.pending = {
        actions: value.actions.flatMap(({ action, run }) =>
          action && run
            ? actionSummaryView(action).map((summary) => ({
                action: summary,
                run: { ...runView(run), kind: 'run' as const },
              }))
            : [],
        ),
        actionCount: value.actionCount.toString(),
        next: value.nextAfter,
        unavailable: false,
      }
    } catch (error) {
      if (!unavailable(error)) throw error
      page.pending = { actions: [], actionCount: '0', next: '', unavailable: true }
    }
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
  function executionBranch(
    value: Awaited<ReturnType<typeof runPage>>,
    row: Extract<ActivityRow, { kind: 'run' }>,
  ): ActivityBranch {
    return {
      total: value.total.toString(),
      offset: Number(value.offset),
      anchorUnavailable: value.anchorUnavailable,
      next: value.hasMore ? value.nextAfter.toString() : '',
      unavailable: false,
      rows: value.entries.flatMap(({ record }, index): ActivityRow[] => {
        const position = Number(value.offset) + index
        if (record.case === 'invocation')
          return [
            {
              ...invocationView(record.value),
              task: row.task,
              actor: row.actor,
              kind: 'invocation',
              position,
            },
          ]
        if (record.case !== 'event') return []
        const event = {
          ...outputRow(
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
          ),
          position,
        }
        if (
          record.value.kind === 'run.ended' &&
          (record.value.contextImpactBytes ?? 0n) > 0n &&
          event.name !== page.selected &&
          event.name !== anchor(row.name)
        )
          return [{ ...event, name: `${row.name}/final`, label: 'final value' }]
        return [event]
      }),
    }
  }
  for (const row of page.rows) {
    if (row.kind !== 'task' || !page.expanded.includes(row.name)) continue
    try {
      const value = await taskPage(row.name)
      page.branches[row.name] = {
        total: value.total.toString(),
        offset: Number(value.offset),
        anchorUnavailable: value.anchorUnavailable,
        rows: value.interactions.map((interaction, index): ActivityRow => ({
          ...(interaction.run
            ? {
                ...runView(interaction.run),
                kind: 'run' as const,
                submission: interactionView(interaction),
              }
            : toolRow(interaction)),
          position: Number(value.offset) + index,
        })),
        next: value.nextAfter,
        unavailable: false,
      }
    } catch (error) {
      if (!unavailable(error)) throw error
      page.branches[row.name] = missingBranch()
    }
  }
  // A snapshot of the records you watch holds only those, not the newest page.
  const newestPage = reading && !reading.followRoot ? undefined : page.rows
  const liveLatest = await liveLatestWork(page, newestPage, query, api, scope, signal)
  if (liveLatest) page.liveLatest = liveLatest
  // Expanded executions stay represented even outside their Task's active child viewport.
  // Their owning read supplies the authorized association and ordinal; references never pin content.
  for (const name of page.expanded.filter((value) => recordKind(value) === 'run')) {
    let row = [...page.rows, ...Object.values(page.branches).flatMap((branch) => branch.rows)].find(
      (item): item is Extract<ActivityRow, { kind: 'run' }> =>
        item.kind === 'run' && item.name === name,
    )
    if (
      !row &&
      !page.rows.some(
        (parent) =>
          parent.kind === 'task' &&
          page.branches[parent.name] &&
          !page.branches[parent.name].unavailable,
      )
    )
      continue
    try {
      const value = await runPage(name)
      if (!row) {
        const parent = value.run?.task && page.branches[value.run.task]
        if (!parent || parent.unavailable || !value.run || value.submission?.position === undefined)
          continue
        row = {
          ...runView(value.run),
          kind: 'run',
          submission: interactionView(value.submission),
          position: Number(value.submission.position),
        }
        parent.rows.push(row)
        parent.rows.sort((a, b) => (a.position ?? 0) - (b.position ?? 0))
      }
      row.hasFinalValue = value.hasFinalValue
      page.branches[name] = executionBranch(value, row)
    } catch (error) {
      if (!unavailable(error)) throw error
      if (row) page.branches[name] = missingBranch()
    }
  }
  // Emissions normalize the server request's tree state, not the browser URL. Preserve
  // effective anchors so a later close/expand still carries an implicit direct-link window.
  page.anchors = Object.fromEntries(
    page.expanded.flatMap((parent) => {
      const value = query.get(`anchor.${parent}`) ?? automaticAnchors.get(parent)
      return value ? [[parent, value]] : []
    }),
  )
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

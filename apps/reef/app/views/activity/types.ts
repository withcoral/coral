import type { ConnectionState } from '@/views/accounts/types'
import type { ToolHintsView } from '@/views/providers/types'
import type { ActivityViewport } from '@/lib/activity-live-types'

/** History, pending shortcuts, and exact selection share one authorized reader. */
export interface ActivityViewProps {
  page: ActivityPage
  windowKey?: string
  revealRecord?: string
  /**
   * The MCP URL a client connects at, which the empty state offers to copy. Agent history has
   * none: an Agent connects with its API key, which its page explains.
   */
  connect?: { endpoint: string }
  /** Replaces paging with one link to the full list, for a short glimpse of the latest roots. */
  more?: {
    to: string
    label: string
    /** How many of the latest roots it lists. */
    roots: number
  }
  /** Loads more of the list at either end. Absent, the view lists only `page.rows`. */
  list?: {
    /** Top-level records loaded so far. */
    count: number
    /** Null when nothing newer is left to load. */
    newer: ActivityLoadControl | null
    /** Null when nothing older is left to load. */
    older: ActivityLoadControl | null
  }
  live?: {
    connected: boolean
    followRoot: boolean
    atStart: (value: boolean) => void
    viewport: (parent: string, value: ActivityViewport) => void
    /** The record keys on screen, first to last. */
    visible?: (records: string[]) => void
  }
}
export interface ActivityLoadControl {
  load: () => void
  loading: boolean
  failed: boolean
  /** Reaching this end loads without a click. */
  automatic: boolean
}

export const MAX_EXPANDED_RECORDS = 20
export const runStates = [
  'running',
  'succeeded',
  'failed',
  'canceled',
  'timed_out',
  'interrupted',
] as const
export const invocationStates = ['pending', 'dispatching', 'ended'] as const
export const invocationOutcomes = [
  'upstream_outcome',
  'coral_failure',
  'run_terminated',
  'indeterminate',
] as const
export type RunState = (typeof runStates)[number] | 'unknown'
export type InvocationState = (typeof invocationStates)[number] | 'unknown'
export type InvocationOutcome = (typeof invocationOutcomes)[number] | 'unknown'
export interface ExecutionRow {
  intent?: string
  actorLabel?: string
  workspaceLabel?: string
  actionPage?: ActionPage
  contextImpactBytes?: string
  /** Qualified invoking Agent; absent for personal work. Never an authority grant. */
  agent: string
  name: string
  workspace: string
  state: RunState
  startedAt: string
  endedAt: string | null
  durationNanos: string
  requiresAction: boolean
  task: string
  actor: string
}
export interface InvocationRow extends Omit<ExecutionRow, 'state'> {
  state: InvocationState
  run: string
  provider: string
  tool: string
  callPath: string
  outcome: InvocationOutcome
  dispatchedAt: string | null
  waitingNanos: string
  /** Where the author's code made this call; absent when Coral stored none. */
  sourceLocation?: RecordSourceLocation
}
/**
 * Where in its Run's accepted source the author's code made a record. Line and
 * column start at 1; the column counts UTF-16 code units.
 */
export interface RecordSourceLocation {
  /** The accepted program, or the linked snapshot of this Saved Function resource. */
  document: { kind: 'program' } | { kind: 'saved_function'; savedFunction: string }
  line: number
  column: number
}
export interface ProgramError {
  name: string
  message: string
  frames: { source: string; line: number; column: number }[]
  toolCode: string
  toolTraceId: string
  /** HTTP status of the upstream response that rejected the call, or 0. */
  toolStatus: number
  /** Start of that response's JSON-encoded data, or empty. */
  toolBody: string
  toolBodyTruncated: boolean
  cause: ProgramError | null
  errors: ProgramError[]
  truncated: boolean
  causeOmitted: boolean
  omittedErrors: number
  circular: boolean
}
export type RunEvent = { sequence: string; occurredAt: string } & (
  | { kind: 'run.started' | 'unknown' }
  | { kind: 'invocation.started'; invocation: string }
  | { kind: 'invocation.ended'; invocation: string; outcome: InvocationOutcome }
  | { kind: 'program.emitted'; value: string }
  | {
      kind: 'program.logged'
      level: 'debug' | 'info' | 'warn' | 'error' | 'unknown'
      message: string
    }
  | {
      kind: 'run.cancellation_requested'
      actor: string
      actorKind: 'user' | 'agent' | 'unknown'
    }
  | { kind: 'run.ended'; state: Exclude<RunState, 'running'> }
)
export type RunFailureCode =
  | 'program_rejected'
  | 'invalid_program_value'
  | 'resource_limit_exceeded'
  | 'memory_limit_exceeded'
  | 'program_execution_failed'
  | 'run_authority_lost'
  | 'unknown'
export type RunTerminal =
  | { kind: 'succeeded'; finalValue: string | null }
  | {
      kind: 'failed'
      code: RunFailureCode
      message: string
      error: ProgramError | null
    }
  | { kind: 'canceled' | 'timed_out' | 'interrupted' | 'unavailable' }
export interface RunDetail {
  kind: 'run'
  request?: RequestDetail | null
  requestUnavailable?: boolean
  run: ExecutionRow
  submission?: InteractionRow
  source: string
  linkedFunctions: { savedFunction: string; path: string; source: string }[]
  terminal: RunTerminal
}
export type InvocationResult =
  | { kind: 'upstream_outcome'; value: string }
  | { kind: 'coral_failure'; code: string; message: string }
  | { kind: 'run_terminated' | 'indeterminate' | 'unknown' }
export type RequestReference =
  | { kind: 'approval' | 'account_connection'; name: string }
  | { kind: 'unknown' }
export interface ActionPage {
  actions: ActionSummary[]
  total: string
  next: string
  /** The cursor a later page starts after; absent on the first page. */
  after?: string
  /** The later page a link asked for failed, so the list holds the first page instead. */
  incomplete?: boolean
}
/** One pending request, with a Run that waits on it as context. */
export interface PendingAction {
  action: ActionSummary
  run: Extract<ActivityRow, { kind: 'run' }>
}
export type ActionSummary = Exclude<RequestReference, { kind: 'unknown' }> & {
  callPath: string
  providerLabel: string
  deadline: string
}
export type RequestDetail =
  | {
      kind: 'approval'
      actionable?: boolean
      name: string
      run: string
      invocation: string
      provider: string
      providerLabel?: string
      approverLabel?: string
      /** Intent of the Task that owns the Run; absent when that Task is unavailable. */
      taskIntent?: string
      callPath: string
      arguments: string
      state: string
      deadline: string
      approver: string
      decidedAt: string | null
      comment: string | null
    }
  | {
      kind: 'account_connection'
      name: string
      provider: string
      state: ConnectionState
      expiresAt: string | null
    }
export interface InvocationDetail {
  kind: 'invocation'
  /** Independently authorized containing Run, for concurrent actions and Program context.
   * Present whenever `request` is an approval. */
  context?: RunDetail
  invocation: InvocationRow
  arguments: string
  result: InvocationResult
  requests: RequestReference[]
  request: RequestDetail | null
  requestUnavailable: boolean
}
export type ActivityRow = { position?: number } & (
  | ({ kind: 'task' } & TaskRow)
  | ({ kind: 'run'; submission?: InteractionRow; hasFinalValue?: boolean } & ExecutionRow)
  | ({ kind: 'invocation' } & InvocationRow)
  | ({ kind: 'tool'; workspace: string; task: string } & InteractionRow)
  | {
      kind: 'output'
      contextImpactBytes?: string
      name: string
      workspace: string
      run: string
      label: string
      startedAt: string
      eventKind: string
      agent: string
      task?: string
      /** Where the author's code emitted or logged it; absent when Coral stored none. */
      sourceLocation?: RecordSourceLocation
    }
)
export interface ActivityBranch {
  /** First ordinal in the bounded child viewport; other expanded executions can be pinned outside it. */
  offset?: number
  anchorUnavailable?: boolean
  /** Authorized child count independent of this window. */
  total?: string
  rows: ActivityRow[]
  next: string
  unavailable: boolean
}
export interface ActivityPage {
  pending?: {
    /** One entry per pending request, most urgent first. */
    actions: PendingAction[]
    actionCount: string
    next: string
    unavailable: boolean
  }
  rows: ActivityRow[]
  branches: Record<string, ActivityBranch>
  expanded: string[]
  anchors?: Record<string, string>
  hasBefore: boolean
  hasAfter: boolean
  anchorUnavailable: boolean
  listUnavailable: boolean
  /** The Agent whose history this lists; null lists the caller's own Workspace activity. */
  agent: string | null
  detail:
    | { kind: 'task'; task: TaskRow; feedback?: string }
    | {
        kind: 'tool'
        task: string
        interaction: InteractionRow
        evidence: InteractionEvidence
      }
    | {
        kind: 'output'
        run: string
        event: RunEvent
        /** The containing Run's authorized source context, when the loader retained it. */
        context?: RunDetail
      }
    | RunDetail
    | InvocationDetail
    | null
  unavailable: boolean
  selected: string | null
  /** Server-selected initial request; pin this identity without refetching the page. */
  initialRequest?: string
  /** An exact approval resolves its tool as the review selection. */
  initialRecord?: string
  /**
   * The oldest action you can take in the selected record's Run, read while live mode
   * follows that Run or a request review is open. Absent when it has none.
   */
  liveAction?: LiveAction
  /**
   * What page live mode needs that the list and its loaded windows do not show. Read
   * only while it follows the newest activity with no Run that runs open.
   */
  liveLatest?: LiveLatest
  selectedRow: ActivityRow | null
  listPath: string
}

/**
 * A current request of a Run and the record that reviews it: the waiting tool for
 * an approval, the Run for an account connection.
 */
export interface LiveAction {
  run: string
  record: string
  request: string
}

export type RunRow = Extract<ActivityRow, { kind: 'run' }>

/** What page live mode knows past the list and its loaded windows. */
export interface LiveLatest {
  /**
   * The oldest action you can take in any Run page live mode may follow, timed by when
   * the tool that waits on it started, not by when the request was made. `task`
   * is the Run's Task, empty for a standalone Run.
   */
  action?: { task: string; action: LiveAction }
  /**
   * By top-level record, as Coral's records say: every standalone Run that runs, under
   * its own name, and a Task's newest Run that runs by submission. It names a Task only
   * when the Task started `TAKS_MAX_AGE` ago or less and its row says a Run of it runs,
   * so a Run of an older Task never shows here, even while it runs.
   */
  running?: Record<string, RunRow>
  /**
   * The newest top-level record, when the snapshot does not start with it, such as one
   * that holds only the records you watch.
   */
  newest?: string
  /**
   * The newest record page live mode may follow when it has nothing else to follow, read
   * only while no action waits and no Run runs. Its candidates are:
   * - each standalone Run, timed by when Coral accepted it;
   * - the newest child, as Coral orders them, of each Task that started `TAKS_MAX_AGE`
   *   ago or less, timed by when its tool call started;
   * - each such Task that has no child yet, timed by when it started.
   *
   * The one that started last wins, to the nanosecond, and at an equal time the newer
   * top-level record. `root` is its top-level record, and `run` says the record is a Run.
   * Absent unless the loader read every record and the newest child of every such Task.
   * When it is absent and no held Run, action or running Run gives page live mode a
   * target, page live mode keeps the record it shows.
   */
  latest?: { root: string; record: string; run: boolean }
  /**
   * The loader read every top-level record and every Task it looked in, so `running`
   * names every Run that runs that page live mode may follow. A row or a Task window
   * of the list it did not refresh can lag its Run's end, so `running` wins over them.
   */
  complete?: boolean
}

/**
 * What live mode follows: the selected record's Run, the newest Task or Run in the
 * list, or the Runs of the selected Task in order.
 */
export type LiveScope = 'run' | 'latest' | 'task'
/** Each scope's `live=` value. `1` is the Run scope, which older links name. */
const liveValues: Record<LiveScope, string> = { run: '1', latest: 'latest', task: 'task' }
export const liveValue = (scope: LiveScope) => liveValues[scope]
/** The scope a `live=` value names, or null when live mode is off. */
export function liveScopeOf(value: string | null): LiveScope | null {
  const scope = Object.entries(liveValues).find(([, item]) => item === value)?.[0]
  return (scope as LiveScope | undefined) ?? null
}

export interface TaskRow {
  contextImpactBytes?: string
  /** A child execution is still running, so the total covers only finished children. */
  contextImpactPartial?: boolean
  name: string
  workspace: string
  intent: string
  outcome: string
  startedAt: string
  endedAt: string | null
  actor: string
  agent: string
}
export interface InteractionRow {
  position?: number
  contextImpactBytes?: string
  agent: string
  name: string
  tool: string
  intent: string
  outcome: string
  startedAt: string
  completedAt: string | null
  acceptedRun: string
}
export type DiscoveryContext =
  | {
      kind: 'search'
      query: string | null
      provider: string | null
      limit: number
      cursor: string | null
    }
  | { kind: 'describe'; callPath: string; types: string[] }
  | { kind: 'unknown' }
/** A kind of Search Entity, as Coral stores it. */
export type DiscoveryEntityKind = 'provider_tool' | 'saved_function'
/** What a Tool's provider and Workspace policy state about a call. */
export interface ToolFlags {
  approvalRequired: boolean
  deprecated: boolean
  hints: ToolHintsView
}
export interface ToolFacts extends ToolFlags {
  providerLabel: string
}
export interface SearchHit {
  kind: DiscoveryEntityKind
  callPath: string
  /** The Tool's title when the page states it, otherwise the path under its namespace. */
  title: string
  signatureLine: string
  /** Cut Markdown. Show it as text. */
  descriptionSnippet: string
  guidanceSnippet: string | null
  tool: ToolFlags | null
  /** Why the hit matched: tags, and the field names and declaration paths an untrimmed page lists. */
  matched: { fields: string[]; tags: string[]; paths: string[] }
  /** Today's catalog page for the target; null when the target is gone. */
  currentHref: string | null
}
/** Search Coverage for one kind of Search Entity. */
export interface SearchCoverage {
  kind: DiscoveryEntityKind
  state: 'results_found' | 'empty' | 'skipped' | 'partial' | 'error'
  eligible: number
  searched: number
  failed: number
  returned: number
  hasMore: boolean
}
export interface SearchPage {
  hits: SearchHit[]
  hasMore: boolean
  coverage: SearchCoverage[]
}
export interface DescriptionDiagnostic {
  category: 'message' | 'warning' | 'error' | 'suggestion'
  code: number
  message: string
  line: number | null
  column: number | null
}
export interface Description {
  kind: DiscoveryEntityKind
  callPath: string
  title: string
  /** Provider or author prose; may contain Markdown. */
  description: string
  declaration: string
  tool: (ToolFacts & { tags: string[]; guidance: string | null }) | null
  savedFunction: {
    source: string
    diagnostics: DescriptionDiagnostic[]
    diagnosticsTruncated: boolean
  } | null
  currentHref: string | null
}
/** The stored response as Reef reads it; `raw` when Reef cannot read it. */
export type DiscoveryView =
  | { kind: 'search'; page: SearchPage }
  | { kind: 'describe'; description: Description }
  | { kind: 'raw' }
export type InteractionEvidence =
  | { kind: 'discovery'; context: DiscoveryContext; response: string; view: DiscoveryView }
  | { kind: 'run' | 'function'; reference: string }
  | { kind: 'rejection'; code: string; message: string }
  | { kind: 'unknown' }

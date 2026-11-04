import type { ConnectionState } from '@/views/accounts/types'
import type { ToolHintsView } from '@/views/providers/types'

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
  /** The parts of the Run's context impact, in exact bytes. Absent while the total is. */
  contextImpact?: RunContextImpact
}
/** Final Value plus emissions and logs, minus tool results: the Run's net context impact. */
export interface RunContextImpact {
  finalValueBytes: string
  outputBytes: string
  toolResultBytes: string
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
/** An approval request that a person approved or declined, or whose deadline passed. It never
 * holds arguments or a comment; the review reads those. */
export interface SettledApproval {
  name: string
  outcome: 'approved' | 'declined' | 'expired'
  /** When it settled. For an expired request, its deadline. */
  settledAt: string
  /** "You", the reviewer's name, or "Unavailable user". Empty when nobody decided. */
  approverLabel: string
  callPath: string
  providerLabel: string
  run: Extract<ActivityRow, { kind: 'run' }>
}
/** One page of settled approvals, newest first. `next` continues it; empty at the end. */
export interface SettledPage {
  approvals: SettledApproval[]
  next: string
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
export type ApprovalDetail = Extract<RequestDetail, { kind: 'approval' }>
/** What an approval review shows of the Run that waits on it. */
export type ApprovalContext = Pick<RunDetail, 'run' | 'submission'>
/** One exact Approval Request as its review reads it, outside any Activity selection. */
export interface ApprovalReading {
  detail: ApprovalDetail
  /** The Run that waits on it, with the source that made the call. */
  context: RunDetail
  /** The call it decides, whose recorded location the source highlights. */
  call: Extract<ActivityRow, { kind: 'invocation' }>
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
  | ({ kind: 'run'; submission?: InteractionRow } & ExecutionRow)
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
/** One top-level record and the records under it that Coral returned, as a waterfall shows them. */
export interface ActivityTrace {
  root: Extract<ActivityRow, { kind: 'task' | 'run' }>
  /**
   * Each parent's children in Coral's order: a Task's tool calls and the Runs they started, and
   * a Run's tool calls and Events. A parent with no entry has no children Coral returned.
   */
  children: Record<string, ActivityRow[]>
  /** False when the read stopped at its limit, so later records are missing. */
  complete: boolean
  /** Runs in the Task whose records you cannot read. They have no children entry. */
  unavailable: string[]
  /** The last call or Run of the root that failed, with its error as Coral recorded it. */
  failure?: TraceFailure
  /** What the agent said when it ended the Task, if it said anything. */
  feedback?: string
  /** Why a call waited a second or more, by its name: the request it waited on. */
  waitedFor?: Record<string, 'approval' | 'account connection'>
  /**
   * The function each save saved, by the save's name: its resource, its path when a later Run
   * of the trace called it, and whether a Run that succeeded called it after this save.
   */
  saves?: Record<string, { savedFunction: string; path?: string; tested?: boolean }>
}
export interface TraceFailure {
  record: string
  /** Its name, as its row shows it. */
  label: string
  code: string
  message: string
  /** What the call waited for before it ran, if it waited on a request. */
  waitedFor?: 'approval' | 'account connection'
  /** Earlier calls of the same tool in the root that failed with the same error. */
  sameBefore?: number
}
export interface ActivityPage {
  rows: ActivityRow[]
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
  selectedRow: ActivityRow | null
  listPath: string
  /**
   * The open Task or standalone Run and its records: the one `root` names, else the selected
   * record's, else the newest in the list. Null when there is none, or you cannot read it.
   */
  trace: ActivityTrace | null
  /**
   * Why the page has no timeline for its root: Coral refused the read, or failed it in passing.
   * A live update that failed keeps the timeline the reader shows.
   */
  traceUnavailable?: 'refused' | 'failed'
  /**
   * When the loader read the page, in epoch milliseconds: the "now" of ongoing bars, the same
   * on the server and in the browser. A clock sample, not evidence.
   */
  readAt?: number
}

export type RunRow = Extract<ActivityRow, { kind: 'run' }>

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
      /** Ranked results the page skipped; 0 for the first page. */
      offset: number
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

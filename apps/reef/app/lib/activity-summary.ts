/** How the ended Tool calls in a window ended, by who acts next. */
export interface CallOutcomes {
  /** The upstream returned a complete response, which may still be an error response. */
  answered: number
  /** A rule stopped the call: a disabled tool, a required approval, access, or a destination. */
  blocked: number
  /** A person declined the approval or the account connection, or let the approval expire. */
  notAllowed: number
  /** A provider, account or credential needs repair, or an account connection expired unanswered. */
  needsSetup: number
  /** The program's call or the upstream failed, or the result is unknown. */
  failed: number
  /** The run ended before the call did. */
  canceled: number
}
export type CallOutcome = keyof CallOutcomes
/** The outcomes a person or a rule stopped. */
export type StopReason = 'blocked' | 'notAllowed' | 'needsSetup'

/** One tool's stopped calls, split by who must act. */
export type StoppedTool = { callPath: string } & Record<StopReason, number>
/** One tool's ended calls. */
export interface ToolCalls {
  callPath: string
  calls: number
}
/** Approval decisions made in the window. */
export interface ApprovalDecisions {
  approved: number
  declined: number
  /** Median time from the request to the decision. Null when nobody decided. */
  medianWaitSeconds: number | null
}
/** One caller's net context bytes. */
export interface ContextEntry {
  key: string
  label: string
  /** Exact net bytes; negative means context was saved. */
  bytes: string
}

/** One window's totals. */
export interface ActivitySummaryData {
  calls: CallOutcomes
  /** Most stopped first. */
  stopped: readonly StoppedTool[]
  /** Most called first. */
  tools: readonly ToolCalls[]
  approvals: ApprovalDecisions
  contextImpactBytes?: string
  contextImpactPartial?: boolean
  /** Net bytes per caller, most saved first. They add up to `contextImpactBytes`. */
  contextActors?: readonly ContextEntry[]
}

/** Days a summary can cover. */
export const SUMMARY_DAYS = [1, 7, 30] as const
export type SummaryDays = (typeof SUMMARY_DAYS)[number]
export const DEFAULT_SUMMARY_DAYS: SummaryDays = 7

/** A period the summary offers, or undefined. */
export function summaryDays(value: unknown): SummaryDays | undefined {
  const days = typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : value
  return SUMMARY_DAYS.find((option) => option === days)
}

/** `search` with `days` as its period. The default period leaves the URL. */
export function summarySearch(search: string, days: SummaryDays) {
  const query = new URLSearchParams(search)
  if (days === DEFAULT_SUMMARY_DAYS) query.delete('days')
  else query.set('days', String(days))
  const text = query.toString()
  return text ? `?${text}` : ''
}

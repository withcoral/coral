import type { RecipeIcons } from '@/lib/provider-recipes/choice'
import type { ActivitySummaryData, StopReason } from '@/lib/activity-summary'

export const STOP_REASONS: readonly StopReason[] = ['blocked', 'notAllowed', 'needsSetup']

/** Pages a summary row can open. */
export interface SummaryLinks {
  activity: string
  /** An operation's pages, when its provider is in the workspace. */
  operation: (
    callPath: string,
  ) => { operation: string; rules: string; settings: string } | undefined
  /** The provider a call path belongs to, when it is in the workspace. */
  provider: (callPath: string) => { name: string; path: string; icons?: RecipeIcons } | undefined
}

export interface OperationRow {
  callPath: string
  /** The provider's name, or its namespace when the workspace no longer has it. */
  provider: string
  /** Groups rows by provider: `coral.providers.<namespace>`. */
  providerKey: string
  icons?: RecipeIcons
  /** The Call Path after the provider's namespace. */
  operation: string
  calls: number
  stops: Record<StopReason, number>
  stopped: number
}

export interface ProviderGroup {
  key: string
  provider: string
  icons?: RecipeIcons
  rows: OperationRow[]
  calls: number
  stops: Record<StopReason, number>
  stopped: number
}

const stoppedOf = (stops: Record<StopReason, number>) =>
  STOP_REASONS.reduce((total, reason) => total + stops[reason], 0)

/**
 * The busiest operations and the stopped ones as one list, one row per Call Path.
 * The summary lists them apart, and an operation can be stopped without being among
 * the busiest; it joins with its stopped calls as its calls.
 */
export function operationRows(
  summary: Pick<ActivitySummaryData, 'operations' | 'stopped'>,
  links?: Pick<SummaryLinks, 'provider'>,
): OperationRow[] {
  const stopped = new Map(summary.stopped.map((op) => [op.callPath, op]))
  const called = new Set(summary.operations.map((op) => op.callPath))
  return [
    ...summary.operations,
    ...summary.stopped
      .filter((op) => !called.has(op.callPath))
      .map((op) => ({ callPath: op.callPath, calls: stoppedOf(op) })),
  ].map(({ callPath, calls }) => {
    const op = stopped.get(callPath)
    const stops = {
      blocked: op?.blocked ?? 0,
      notAllowed: op?.notAllowed ?? 0,
      needsSetup: op?.needsSetup ?? 0,
    }
    const [root, providers, namespace, ...rest] = callPath.split('.')
    const provider = links?.provider(callPath)
    return {
      callPath,
      provider: provider?.name ?? namespace ?? callPath,
      providerKey: [root, providers, namespace].join('.'),
      icons: provider?.icons,
      operation: rest.join('.') || callPath,
      calls,
      stops,
      stopped: stoppedOf(stops),
    }
  })
}

/** Rows grouped by provider, in the order each provider first appears. */
export function groupByProvider(rows: readonly OperationRow[]): ProviderGroup[] {
  const groups = new Map<string, ProviderGroup>()
  for (const row of rows) {
    const group = groups.get(row.providerKey) ?? {
      key: row.providerKey,
      provider: row.provider,
      icons: row.icons,
      rows: [],
      calls: 0,
      stops: { blocked: 0, notAllowed: 0, needsSetup: 0 },
      stopped: 0,
    }
    group.rows.push(row)
    group.calls += row.calls
    for (const reason of STOP_REASONS) group.stops[reason] += row.stops[reason]
    group.stopped += row.stopped
    groups.set(row.providerKey, group)
  }
  return [...groups.values()]
}

/** The page that fixes a stop: the rules for a blocked call, the settings for one that needs setup. */
export function fixFor(
  reason: 'blocked' | 'needsSetup',
  pages: ReturnType<SummaryLinks['operation']>,
) {
  if (!pages) return undefined
  return reason === 'blocked'
    ? { to: pages.rules, label: 'Review rules' }
    : { to: pages.settings, label: 'Fix setup' }
}

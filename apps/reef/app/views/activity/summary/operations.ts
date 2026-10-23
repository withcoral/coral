import type { RecipeIcons } from '@/lib/provider-recipes/choice'
import type { ActivitySummaryData, StopReason } from '@/lib/activity-summary'

export const STOP_REASONS: readonly StopReason[] = ['blocked', 'notAllowed', 'needsSetup']

/** Pages a summary row can open. */
export interface SummaryLinks {
  /** An operation's pages, when its provider is in the workspace. */
  operation: (callPath: string) => { operation: string; settings: string } | undefined
  /** The provider a call path belongs to, when it is in the workspace. */
  provider: (callPath: string) => { name: string; path: string; icons?: RecipeIcons } | undefined
}

export interface OperationRow {
  callPath: string
  /** The provider's name, or its namespace when the workspace no longer has it. */
  provider: string
  /** The provider's namespace, which tells apart two providers with one name. */
  namespace: string
  icons?: RecipeIcons
  /** The Call Path after the provider's namespace. */
  operation: string
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
    const [, , namespace, ...rest] = callPath.split('.')
    const provider = links?.provider(callPath)
    return {
      callPath,
      provider: provider?.name ?? namespace ?? callPath,
      namespace: namespace ?? '',
      icons: provider?.icons,
      operation: rest.join('.') || callPath,
      calls,
      stops,
      stopped: stoppedOf(stops),
    }
  })
}

/**
 * The page that fixes a stop: the operation's drawer, where you change its access, for a
 * blocked call; the provider's settings for one that needs setup.
 */
export function fixFor(
  reason: 'blocked' | 'needsSetup',
  pages: ReturnType<SummaryLinks['operation']>,
) {
  if (!pages) return undefined
  return reason === 'blocked' ? pages.operation : pages.settings
}

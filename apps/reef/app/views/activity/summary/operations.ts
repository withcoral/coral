import type { RecipeIcons } from '@/lib/provider-recipes/choice'
import type { ActivitySummaryData, StopReason } from '@/lib/activity-summary'
import { operationOwner } from '@/lib/call-root'

export const STOP_REASONS: readonly StopReason[] = ['blocked', 'notAllowed', 'needsSetup']

/** A provider in the workspace, which names the rows under its Call Root and opens their pages. */
export interface SummaryProvider {
  callRoot: string
  displayName: string
  path: string
  icons?: RecipeIcons
}

export interface OperationRow {
  callPath: string
  /** The provider's name, or its namespace when the workspace no longer has it. */
  provider: string
  /** The last part of the provider's Call Root, which tells apart two providers with one name. */
  namespace: string
  icons?: RecipeIcons
  /** The Call Path after the provider's Call Root. */
  operation: string
  /** The operation's pages, when its provider is in the workspace. */
  pages?: { operation: string; settings: string }
  calls: number
  stops: Record<StopReason, number>
  stopped: number
}

/** A removed provider leaves only its Call Path: `coral.providers.<namespace>.<operation>`. */
const REMOVED_ROOT = 'coral.providers.'

/** Who owns a Call Path, and the operation's name and pages under that owner. */
function named(callPath: string, providers: readonly SummaryProvider[]) {
  const owner = operationOwner(callPath, providers)
  if (owner) {
    const { provider, relativePath } = owner
    return {
      provider: provider.displayName,
      namespace: provider.callRoot.split('.').at(-1)!,
      icons: provider.icons,
      operation: relativePath,
      pages: {
        operation: `${provider.path}/ops/${encodeURIComponent(relativePath)}`,
        settings: `${provider.path}/settings`,
      },
    }
  }
  const [namespace = '', ...rest] = callPath.startsWith(REMOVED_ROOT)
    ? callPath.slice(REMOVED_ROOT.length).split('.')
    : []
  return { provider: namespace || callPath, namespace, operation: rest.join('.') || callPath }
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
  providers: readonly SummaryProvider[] = [],
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
    return {
      callPath,
      ...named(callPath, providers),
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
export function fixFor(reason: 'blocked' | 'needsSetup', pages: OperationRow['pages']) {
  if (!pages) return undefined
  return reason === 'blocked' ? pages.operation : pages.settings
}

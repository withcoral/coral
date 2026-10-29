import type { RecipeIcons } from '@/lib/provider-recipes/choice'
import type { ActivitySummaryData, CallerCalls, StopReason } from '@/lib/activity-summary'
import { toolOwner } from '@/lib/call-root'

export const STOP_REASONS: readonly StopReason[] = ['blocked', 'notAllowed', 'needsSetup']

/** A provider in the workspace, which names the rows under its Call Root and opens their pages. */
export interface SummaryProvider {
  callRoot: string
  displayName: string
  path: string
  icons?: RecipeIcons
}

export interface ToolRow {
  callPath: string
  /** The provider's name, or its namespace when the workspace no longer has it. */
  provider: string
  /** The last part of the provider's Call Root, which tells apart two providers with one name. */
  namespace: string
  icons?: RecipeIcons
  /** The Call Path after the provider's Call Root. */
  tool: string
  /** The tool's pages, when its provider is in the workspace. */
  pages?: { tool: string; settings: string }
  calls: number
  /** Who made the calls, busiest first. */
  callers: readonly CallerCalls[]
  stops: Record<StopReason, number>
  stopped: number
}

/** A removed provider leaves only its Call Path: `coral.providers.<namespace>.<tool>`. */
const REMOVED_ROOT = 'coral.providers.'

/** Who owns a Call Path, and the tool's name and pages under that owner. */
function named(callPath: string, providers: readonly SummaryProvider[]) {
  const owner = toolOwner(callPath, providers)
  if (owner) {
    const { provider, relativePath } = owner
    return {
      provider: provider.displayName,
      namespace: provider.callRoot.split('.').at(-1)!,
      icons: provider.icons,
      tool: relativePath,
      pages: {
        tool: `${provider.path}/tools/${encodeURIComponent(relativePath)}`,
        settings: `${provider.path}/settings`,
      },
    }
  }
  const [namespace = '', ...rest] = callPath.startsWith(REMOVED_ROOT)
    ? callPath.slice(REMOVED_ROOT.length).split('.')
    : []
  return { provider: namespace || callPath, namespace, tool: rest.join('.') || callPath }
}

const stoppedOf = (stops: Record<StopReason, number>) =>
  STOP_REASONS.reduce((total, reason) => total + stops[reason], 0)

/**
 * The busiest tools and the stopped ones as one list, one row per Call Path.
 * The summary lists them apart, and a tool can be stopped without being among
 * the busiest; it joins with its stopped calls as its calls.
 */
export function toolRows(
  summary: Pick<ActivitySummaryData, 'tools' | 'stopped'>,
  providers: readonly SummaryProvider[] = [],
): ToolRow[] {
  const stopped = new Map(summary.stopped.map((tool) => [tool.callPath, tool]))
  const called = new Set(summary.tools.map((tool) => tool.callPath))
  return [
    ...summary.tools,
    ...summary.stopped
      .filter((tool) => !called.has(tool.callPath))
      .map((tool) => ({ callPath: tool.callPath, calls: stoppedOf(tool), callers: [] })),
  ].map(({ callPath, calls, callers }) => {
    const stoppedTool = stopped.get(callPath)
    const stops = {
      blocked: stoppedTool?.blocked ?? 0,
      notAllowed: stoppedTool?.notAllowed ?? 0,
      needsSetup: stoppedTool?.needsSetup ?? 0,
    }
    return {
      callPath,
      ...named(callPath, providers),
      calls,
      callers,
      stops,
      stopped: stoppedOf(stops),
    }
  })
}

/**
 * The page that fixes a stop: the tool's drawer, where you change its access, for a
 * blocked call; the provider's settings for one that needs setup.
 */
export function fixFor(reason: 'blocked' | 'needsSetup', pages: ToolRow['pages']) {
  if (!pages) return undefined
  return reason === 'blocked' ? pages.tool : pages.settings
}

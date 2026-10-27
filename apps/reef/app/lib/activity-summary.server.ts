import type { ActivitySummaryData, SummaryDays } from './activity-summary'
import { Code, ConnectError } from '@connectrpc/connect'
import type { ManagementApi } from './management.server'

const toCount = (value: bigint | undefined) => Number(value ?? 0n)

/** The caller's summary of one Workspace for the last `days`. A failed read rejects; it is never zeros. */
export async function loadActivitySummary(
  api: Pick<ManagementApi, 'activity'>,
  workspace: string,
  days: SummaryDays,
  signal?: AbortSignal,
): Promise<ActivitySummaryData> {
  const value = await api.activity.summarizeActivity({ workspace, days }, { signal })
  const wait = value.approvals?.medianWaitNanos
  return {
    calls: {
      answered: toCount(value.calls?.answered),
      blocked: toCount(value.calls?.blocked),
      notAllowed: toCount(value.calls?.notAllowed),
      needsSetup: toCount(value.calls?.needsSetup),
      failed: toCount(value.calls?.failed),
      canceled: toCount(value.calls?.canceled),
    },
    stopped: value.stopped.map((tool) => ({
      callPath: tool.callPath,
      blocked: toCount(tool.blocked),
      notAllowed: toCount(tool.notAllowed),
      needsSetup: toCount(tool.needsSetup),
    })),
    tools: value.tools.map((tool) => ({ callPath: tool.callPath, calls: toCount(tool.calls) })),
    approvals: {
      approved: toCount(value.approvals?.approved),
      declined: toCount(value.approvals?.declined),
      medianWaitSeconds: wait === undefined ? null : Number(wait) / 1e9,
    },
    contextImpactBytes: value.contextImpactBytes?.toString(),
    contextImpactPartial: value.contextImpactPartial,
    contextActors: value.actors.map(({ key, label, bytes }) => ({
      key,
      label,
      bytes: bytes.toString(),
    })),
  }
}

/** Coral answers that the summary is out of reach now, not that the request is wrong. */
const UNAVAILABLE = new Set([Code.NotFound, Code.PermissionDenied, Code.Unavailable])

/** As `loadActivitySummary`, for a page that shows the summary as a glimpse: an unavailable
 * summary is null, never zeros, and the rest of the page still loads. Any other failure,
 * such as a rejected session or a defect, rejects. */
export async function glimpseActivitySummary(
  ...args: Parameters<typeof loadActivitySummary>
): Promise<ActivitySummaryData | null> {
  try {
    return await loadActivitySummary(...args)
  } catch (error) {
    if (error instanceof ConnectError && UNAVAILABLE.has(error.code)) return null
    throw error
  }
}

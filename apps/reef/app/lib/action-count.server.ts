import { Code, ConnectError } from '@connectrpc/connect'
import type { Workspace } from '@/generated/coral/management/v1/workspace_pb'
import type { ActionCountKind, ActionCounts } from './activity-live-types'
import { resourcePath, type ManagementApi } from './management.server'
import { traceLoaderStep } from '../../server-tracing.js'

/** One authenticated count-only read per Workspace; unavailable is never reported as zero.
 * A rejected session is not one Workspace's failure, so it rejects the whole read. */
export async function loadActionCounts(
  api: Pick<ManagementApi, 'activity'>,
  workspaces: Pick<Workspace, 'name'>[],
  signal?: AbortSignal,
): Promise<ActionCounts> {
  return Object.fromEntries(
    await Promise.all(
      workspaces.map(async ({ name }) => {
        try {
          const value = await api.activity.countRequiredActions({ workspace: name }, { signal })
          const count = {
            total: value.actionCount.toString(),
            approvals: value.approvalCount.toString(),
            connections: value.connectionCount.toString(),
            revisions: {
              approvals: value.approvalRevision,
              connections: value.connectionRevision,
            },
          }
          return [resourcePath(name), count] as const
        } catch (error) {
          if (error instanceof ConnectError && error.code === Code.Unauthenticated) throw error
          return [resourcePath(name), null] as const
        }
      }),
    ),
  )
}

/** The app shell's read of every Workspace's count, in a span of its own: the shell loader
 * reads it last, so it can hold back a data response that waits for every loader. */
export function loadShellActionCounts(
  api: Pick<ManagementApi, 'activity'>,
  workspaces: Pick<Workspace, 'name'>[],
  signal?: AbortSignal,
): Promise<ActionCounts> {
  return traceLoaderStep('app-shell action counts', () => loadActionCounts(api, workspaces, signal))
}

/** The revision of one kind in `workspace`, for a page that follows it. Read it before the
 * page's rows, so the rows are at least as new as it: a newer revision on the shell's stream
 * then refreshes them, and no change falls between the two reads. Undefined when Coral
 * could not count. */
export async function loadRevision(
  api: Pick<ManagementApi, 'activity'>,
  workspace: Pick<Workspace, 'name'>,
  kind: ActionCountKind,
  signal?: AbortSignal,
): Promise<string | undefined> {
  const counts = await loadActionCounts(api, [workspace], signal)
  return counts[resourcePath(workspace.name)]?.revisions[kind]
}

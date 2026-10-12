import { Code, ConnectError } from '@connectrpc/connect'
import type { Workspace } from '@/generated/coral/management/v1/workspace_pb'
import type { ActionCounts } from './activity-live-types'
import { resourcePath, type ManagementApi } from './management.server'

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
          return [resourcePath(name), value.actionCount.toString()] as const
        } catch (error) {
          if (error instanceof ConnectError && error.code === Code.Unauthenticated) throw error
          return [resourcePath(name), null] as const
        }
      }),
    ),
  )
}

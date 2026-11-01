import { data } from 'react-router'
import type { Route } from './+types/approvals'
import { ActivityRequiredActionKind } from '@/generated/coral/management/v1/activity_pb'
import { loadActionCounts } from '@/lib/action-count.server'
import { pendingActions } from '@/lib/activity.server'
import { managementFailure, privateHeaders } from '@/lib/management.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { ApprovalsErrorBoundary, ApprovalsPage } from '@/views/approvals/approvals-page'

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace } = await authorizedWorkspace(context, params.workspace)
    // An archived Workspace takes no decisions. Run detail keeps its past approvals.
    if (workspace.state === 'archived')
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    // Read the revision first, so the rows are at least as new as it. A newer revision on
    // the shell's stream then refreshes them, and no change falls between the two reads.
    const counts = await loadActionCounts(api, [workspace], request.signal)
    const revision = counts[workspace.path]?.revisions.approvals
    const approvals = await pendingActions(
      api,
      workspace.name,
      request.signal,
      ActivityRequiredActionKind.APPROVAL,
    )
    return data({ workspace, approvals, revision }, { headers: privateHeaders })
  } catch (error) {
    managementFailure(error)
  }
}
export function ErrorBoundary({ params }: Route.ErrorBoundaryProps) {
  return <ApprovalsErrorBoundary workspace={`/workspaces/${params.workspace}`} />
}
export default function ApprovalsRoute({ loaderData }: Route.ComponentProps) {
  const { approvals, workspace, revision } = loaderData
  return <ApprovalsPage approvals={approvals} workspace={workspace.path} revision={revision} />
}

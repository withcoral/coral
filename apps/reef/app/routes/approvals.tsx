import { data } from 'react-router'
import type { Route } from './+types/approvals'
import { ActivityRequiredActionKind } from '@/generated/coral/management/v1/activity_pb'
import { loadActionCounts } from '@/lib/action-count.server'
import { approvalReading, pendingActions, settledApprovals, transient } from '@/lib/activity.server'
import { managementFailure, privateHeaders } from '@/lib/management.server'
import { providerMarks } from '@/lib/providers.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { REVIEW_PARAM } from '@/views/actions/review'
import { ApprovalsPage } from '@/views/approvals/approvals-page'
export { ApprovalsErrorBoundary as ErrorBoundary } from '@/views/approvals/approvals-page'

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
    // A request reaches history only by leaving the pending set, so the same revision covers
    // the newest settled page. Older pages load on request and stay as they are.
    const [approvals, settled, providers] = await Promise.all([
      pendingActions(api, workspace.name, request.signal, ActivityRequiredActionKind.APPROVAL),
      // Pending requests carry deadlines, so a history Coral cannot read leaves them.
      settledApprovals(api, workspace.name, '', request.signal).catch((error: unknown) => {
        if (transient(error)) return null
        throw error
      }),
      providerMarks(api, workspace.name, request.signal),
    ])
    // After the revision too, so the open review follows it as the rows do.
    const selected = new URL(request.url).searchParams.get(REVIEW_PARAM)
    const review = selected
      ? await approvalReading(api, workspace.name, selected, request.signal)
      : null
    return data(
      { workspace, approvals, settled, providers, revision, selected, review },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}
export default function ApprovalsRoute({ loaderData }: Route.ComponentProps) {
  const { workspace, ...page } = loaderData
  return <ApprovalsPage {...page} workspacePath={workspace.path} />
}

import { data } from 'react-router'
import type { Route } from './+types/approvals'
import { ActivityRequiredActionKind } from '@/generated/coral/management/v1/activity_pb'
import { pendingActions } from '@/lib/activity.server'
import { managementFailure, privateHeaders } from '@/lib/management.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { ApprovalsPage } from '@/views/approvals/approvals-page'
export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace } = await authorizedWorkspace(context, params.workspace)
    // An archived Workspace takes no decisions. Run detail keeps its past approvals.
    if (workspace.state === 'archived')
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    const approvals = await pendingActions(
      api,
      workspace.name,
      request.signal,
      ActivityRequiredActionKind.APPROVAL,
    )
    return data({ workspace, approvals }, { headers: privateHeaders })
  } catch (error) {
    managementFailure(error)
  }
}
export default function ApprovalsRoute({ loaderData }: Route.ComponentProps) {
  return <ApprovalsPage approvals={loaderData.approvals} />
}

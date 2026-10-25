import { data } from 'react-router'
import type { Route } from './+types/activity'
import {
  management,
  managementFailure,
  privateHeaders,
  workspaceName,
} from '@/lib/management.server'
import { loadActivity } from '@/lib/activity.server'
import { loadActivityPage } from '@/lib/activity-page-fetch'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { LiveActivity } from './activity-live'
import { PageLayout } from '@/wax/components/page-layout'
import { ArchivedBanner, trail } from '@/views/management/layout'
import { restoreFirst } from '@/views/activity/live'
export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const [page, { workspace }] = await Promise.all([
      loadActivity(request, management(request, context), {
        scope: { workspace: workspaceName(params.workspace) },
      }),
      authorizedWorkspace(request, context, params.workspace),
    ])
    return data({ ...page, workspace }, { headers: privateHeaders })
  } catch (error) {
    managementFailure(error)
  }
}
// The Activity reader loads more of its list from this loader through a fetcher.
export function clientLoader({ request, serverLoader }: Route.ClientLoaderArgs) {
  return loadActivityPage(request, serverLoader)
}
export default function ActivityRoute({ loaderData }: Route.ComponentProps) {
  const archived = loaderData.workspace.state === 'archived'
  const role = loaderData.workspace.effectiveRole === 'owner' ? 'owner' : 'member'
  return (
    <PageLayout
      variant="split"
      breadcrumbs={trail([], 'Activity')}
      header={
        archived && (
          <ArchivedBanner workspace={loaderData.workspace}>
            You can read its history. {restoreFirst(role, 'start new runs')}
          </ArchivedBanner>
        )
      }
    >
      {/* An archived workspace takes no new calls, so it offers no endpoint. */}
      <LiveActivity
        initial={loaderData}
        connect={archived ? undefined : { endpoint: loaderData.workspace.connectEndpoint }}
        archived={archived ? role : undefined}
      />
    </PageLayout>
  )
}

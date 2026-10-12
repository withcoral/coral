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
import { trail } from '@/views/management/layout'
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
  return (
    <PageLayout variant="split" breadcrumbs={trail([], 'Activity')}>
      <LiveActivity
        initial={loaderData}
        connect={{
          endpoint: loaderData.workspace.mcpEndpoint,
        }}
      />
    </PageLayout>
  )
}

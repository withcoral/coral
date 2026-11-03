import { data } from 'react-router'
import type { Route } from './+types/run'
import {
  management,
  managementFailure,
  privateHeaders,
  workspaceName,
} from '@/lib/management.server'
import { loadRunActivity } from '@/lib/activity.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { activityProviderMarks } from '@/lib/providers.server'
// Use the same route presentation identity so closing a stable Run link keeps its reader.
export { default } from './activity'
export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const api = management(context)
    const [page, workspace, providerMarks] = await Promise.all([
      loadRunActivity(request, api, params),
      // The Run's workspace names its breadcrumb; without access, the Run page still decides.
      authorizedWorkspace(context, params.workspace).then(
        (authorized) => authorized.workspace,
        () => undefined,
      ),
      activityProviderMarks(api, workspaceName(params.workspace), request),
    ])
    return data({ ...page, workspace, providerMarks }, { headers: privateHeaders })
  } catch (error) {
    managementFailure(error)
  }
}

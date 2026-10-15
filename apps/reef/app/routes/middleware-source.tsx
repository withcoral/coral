import { data, isRouteErrorResponse, useParams, useRouteError } from 'react-router'
import type { Route } from './+types/middleware-source'
import { managementFailure, privateHeaders, workspaceName } from '@/lib/management.server'
import { middlewareDetailView } from '@/lib/middleware.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import {
  MiddlewareSourceDialog,
  MiddlewareSourceError,
} from '@/views/middleware/middleware-source-dialog'

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace: w } = await authorizedWorkspace(request, context, params.workspace, {
      ownerOnly: true,
    })
    const middleware = await api.middleware.getMiddleware({
      name: `${workspaceName(params.workspace)}/middleware/${params.middleware}`,
    })
    return data(
      { listPath: `${w.path}/middleware`, middleware: middlewareDetailView(middleware) },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

export function ErrorBoundary() {
  const error = useRouteError()
  const { workspace = '' } = useParams()
  return (
    <MiddlewareSourceError
      status={isRouteErrorResponse(error) ? error.status : 503}
      listPath={`/workspaces/${encodeURIComponent(workspace)}/middleware`}
    />
  )
}

export default function MiddlewareSourceRoute({ loaderData }: Route.ComponentProps) {
  return <MiddlewareSourceDialog {...loaderData} />
}

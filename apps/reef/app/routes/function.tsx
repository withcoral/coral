import { Code, ConnectError } from '@connectrpc/connect'
import { data, isRouteErrorResponse, useParams, useRouteError } from 'react-router'
import type { Route } from './+types/function'
import {
  management,
  managementFailure,
  privateHeaders,
  resourcePath,
  workspaceName,
} from '@/lib/management.server'
import { savedFunctionView } from '@/lib/management-views.server'
import { FunctionDrawer, FunctionDrawerError } from '@/views/functions/function-drawer'

export async function loader({ request, context, params }: Route.LoaderArgs) {
  const parent = workspaceName(params.workspace)
  try {
    const savedFunction = await management(request, context).savedFunctions.getSavedFunction({
      parent,
      path: params.path,
    })
    return data(
      {
        listPath: `${resourcePath(parent)}/functions`,
        savedFunction: savedFunctionView(savedFunction),
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    // A path that Coral cannot parse names no function.
    if (error instanceof ConnectError && error.code === Code.InvalidArgument)
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    managementFailure(error)
  }
}

export function ErrorBoundary() {
  const error = useRouteError()
  const { workspace = '' } = useParams()
  return (
    <FunctionDrawerError
      status={isRouteErrorResponse(error) ? error.status : 503}
      listPath={`/workspaces/${encodeURIComponent(workspace)}/functions`}
    />
  )
}

export default function FunctionRoute({ loaderData }: Route.ComponentProps) {
  return <FunctionDrawer {...loaderData} />
}

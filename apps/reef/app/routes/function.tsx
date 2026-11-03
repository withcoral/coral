import { Code, ConnectError } from '@connectrpc/connect'
import {
  data,
  isRouteErrorResponse,
  redirect,
  useParams,
  useRouteError,
  useRouteLoaderData,
} from 'react-router'
import type { Route } from './+types/function'
import {
  actionFailure,
  field,
  formContext,
  management,
  managementFailure,
  mutationForm,
  privateHeaders,
  resourcePath,
  workspaceName,
} from '@/lib/management.server'
import { savedFunctionView } from '@/lib/management-views.server'
import { FunctionDrawer, FunctionDrawerError } from '@/views/functions/function-drawer'
import { FUNCTION_DELETED } from '@/views/functions/functions-page'
import type { loader as functionsLoader } from './functions'

export async function loader({ context, params }: Route.LoaderArgs) {
  const parent = workspaceName(params.workspace)
  try {
    const savedFunction = await management(context).savedFunctions.getSavedFunction({
      parent,
      path: params.path,
    })
    return data(
      {
        listPath: `${resourcePath(parent)}/functions`,
        savedFunction: savedFunctionView(savedFunction),
        form: formContext(context),
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

export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  if (field(form, 'intent') !== 'delete') throw new Response('Invalid action', { status: 400 })
  const parent = workspaceName(params.workspace)
  try {
    await management(context).savedFunctions.deleteSavedFunction({
      parent,
      path: params.path,
    })
  } catch (error) {
    return actionFailure(
      error,
      'You cannot delete this function. ',
      'It no longer exists. Close this panel.',
    )
  }
  return redirect(`${resourcePath(parent)}/functions?${FUNCTION_DELETED[0]}=1`, {
    headers: privateHeaders,
  })
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
  // An archived workspace accepts no changes.
  const state = useRouteLoaderData<typeof functionsLoader>('routes/functions')?.workspace.state
  return <FunctionDrawer {...loaderData} canDelete={state === 'active'} />
}

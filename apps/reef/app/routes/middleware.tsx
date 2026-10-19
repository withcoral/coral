import { data, Outlet, useLocation, useNavigation } from 'react-router'
import type { ShouldRevalidateFunctionArgs } from 'react-router'
import type { Route } from './+types/middleware'
import {
  actionFailure,
  managementFailure,
  mutationForm,
  privateHeaders,
} from '@/lib/management.server'
import { loadMiddleware, middlewareAction } from '@/lib/middleware.server'
import { MiddlewarePage } from '@/views/middleware/middleware-page'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

const listPath = (url: URL) =>
  /^(\/workspaces\/[^/]+\/middleware)(?:\/[^/]+)?$/.exec(url.pathname)?.[1]

// Opening or closing the source dialog keeps the list: the dialog reads its own
// Middleware. Changes, refreshes, and other pages reload it.
export function shouldRevalidate({
  currentUrl,
  nextUrl,
  formMethod,
  defaultShouldRevalidate,
}: ShouldRevalidateFunctionArgs) {
  if (formMethod || currentUrl.search !== nextUrl.search) return defaultShouldRevalidate
  const current = listPath(currentUrl)
  if (currentUrl.pathname === nextUrl.pathname || !current || current !== listPath(nextUrl))
    return defaultShouldRevalidate
  return false
}

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    return data(await loadMiddleware(request, context, params.workspace), {
      headers: privateHeaders,
    })
  } catch (error) {
    managementFailure(error)
  }
}

export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  try {
    return await middlewareAction(request, context, params.workspace, form)
  } catch (error) {
    return actionFailure(error, 'Coral could not change the middleware. ')
  }
}

export default function MiddlewareRoute({ loaderData, actionData }: Route.ComponentProps) {
  return (
    <MiddlewarePage
      key={useLocation().key}
      {...loaderData}
      result={actionData}
      pending={useNavigation().state !== 'idle'}
    >
      <Outlet />
    </MiddlewarePage>
  )
}

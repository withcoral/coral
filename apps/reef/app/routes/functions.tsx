import { data, Outlet, type ShouldRevalidateFunctionArgs } from 'react-router'
import type { Route } from './+types/functions'
import type { SavedFunctionSummary } from '@/generated/coral/management/v1/saved_function_pb'
import { managementFailure, privateHeaders } from '@/lib/management.server'
import { savedFunctionSummaryView } from '@/lib/management-views.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { FunctionsPage } from '@/views/functions/functions-page'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

const listPath = (url: URL) =>
  /^(\/workspaces\/[^/]+\/functions)(?:\/[^/]+|\/namespaces\/[^/]+)?$/.exec(url.pathname)?.[1]

// Opening or closing a function drawer, or changing tabs, keeps the list: the
// drawer's own loader reads the function. Refreshes and other pages reload it.
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
    const { api, workspace: w } = await authorizedWorkspace(request, context, params.workspace)
    // The page groups and filters every function, so it reads each bounded page.
    const summaries: SavedFunctionSummary[] = []
    let pageToken = ''
    do {
      const page = await api.savedFunctions.listSavedFunctions({
        parent: w.name,
        pageSize: 100,
        pageToken,
      })
      summaries.push(...page.savedFunctions)
      pageToken = page.nextPageToken
    } while (pageToken)
    return data(
      { workspace: w, functions: summaries.map((f) => savedFunctionSummaryView(w.path, f)) },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

export default function FunctionsRoute({ loaderData }: Route.ComponentProps) {
  return (
    <FunctionsPage {...loaderData}>
      <Outlet />
    </FunctionsPage>
  )
}

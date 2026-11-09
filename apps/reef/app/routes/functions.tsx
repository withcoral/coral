import { data, Outlet, type ShouldRevalidateFunctionArgs } from 'react-router'
import type { Route } from './+types/functions'
import type { SavedFunctionSummary } from '@/generated/coral/management/v1/saved_function_pb'
import type { PolicyState } from '@/components/policy-toggle'
import { managementFailure, privateHeaders, type ManagementApi } from '@/lib/management.server'
import { savedFunctionSummaryView } from '@/lib/management-views.server'
import { accessState, TOOL_ACCESS_PAGE_SIZE } from '@/lib/providers.server'
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

export async function loader({ context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace: w } = await authorizedWorkspace(context, params.workspace)
    // The page groups and filters every function, so it reads each bounded page.
    const summaries: SavedFunctionSummary[] = []
    let privateProvider = ''
    let pageToken = ''
    do {
      const page = await api.savedFunctions.listSavedFunctions({
        parent: w.name,
        pageSize: 100,
        pageToken,
      })
      summaries.push(...page.savedFunctions)
      privateProvider = page.privateProvider
      pageToken = page.nextPageToken
    } while (pageToken)
    const policies = privateProvider ? await ownPolicies(api, privateProvider) : new Map()
    return data(
      {
        workspace: w,
        functions: summaries.map((f) => savedFunctionSummaryView(w.path, f, policies.get(f.path))),
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

/**
 * The policy of each of the caller's own functions, by path, from the
 * private provider. An agent's tool at the same path is another row.
 */
async function ownPolicies(api: ManagementApi, privateProvider: string) {
  const policies = new Map<string, PolicyState>()
  let pageToken = ''
  do {
    const page = await api.policies.listToolAccess({
      parent: privateProvider,
      pageSize: TOOL_ACCESS_PAGE_SIZE,
      pageToken,
    })
    for (const tool of page.tools) {
      if (!tool.agent) policies.set(tool.relativePath, accessState(tool.accessState))
    }
    pageToken = page.nextPageToken
  } while (pageToken)
  return policies
}

export default function FunctionsRoute({ loaderData }: Route.ComponentProps) {
  return (
    <FunctionsPage {...loaderData}>
      <Outlet />
    </FunctionsPage>
  )
}

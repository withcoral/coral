import { data, Outlet, type ShouldRevalidateFunctionArgs } from 'react-router'
import type { Route } from './+types/providers'
import {
  formContext,
  managementFailure,
  mutationForm,
  privateHeaders,
} from '@/lib/management.server'
import { providerView, listAllProviders, uuidOr404 } from '@/lib/providers.server'
import { personalAccountStates } from '@/lib/accounts.server'
import { applyProviderChange } from '@/lib/provider-changes.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { RECIPE_CHOICES } from '@/lib/provider-recipes/library.server'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

// Selection changes on the page (provider, tool) reuse the
// tree: they are one table, and the address only names what is revealed. A
// child loader still checks Workspace authority on each. Refreshes, mutations,
// query changes, and leaving the page reload it.
export function shouldRevalidate({
  currentUrl,
  nextUrl,
  formMethod,
  actionStatus,
}: ShouldRevalidateFunctionArgs) {
  if ((formMethod && formMethod.toUpperCase() !== 'GET') || actionStatus !== undefined) return true
  if (currentUrl.search !== nextUrl.search || currentUrl.pathname === nextUrl.pathname) return true
  // `new` is the Add provider page, whose result must reload the catalog.
  const pagePath =
    /^(\/workspaces\/[^/]+\/providers)(?:\/(?!new(?:\/|$))[^/]+(?:\/tools\/[^/]+)?)?$/
  const current = pagePath.exec(currentUrl.pathname)
  const next = pagePath.exec(nextUrl.pathname)
  return !current || !next || current[1] !== next[1]
}

/** Loads the Workspace tree independently of tool selection. */
export async function loader({ context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace: w } = await authorizedWorkspace(context, params.workspace)
    const [listed, personalAccounts] = await Promise.all([
      listAllProviders(api, w.name),
      personalAccountStates(api, w.name),
    ])
    const providers = await Promise.all(
      listed.map((provider) => providerView(api, provider, w.path, personalAccounts)),
    )
    const selectedProvider = params.provider ? uuidOr404(params.provider) : null
    if (selectedProvider && !providers.some((i) => i.id === selectedProvider))
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    const selectedTool = params.tool ?? null
    if (
      selectedTool &&
      !providers
        .find((i) => i.id === selectedProvider)
        ?.tools.some((o) => o.relativePath === selectedTool)
    )
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    return data(
      {
        workspace: w,
        providers,
        recipes: RECIPE_CHOICES,
        selectedProvider,
        selectedTool,
        form: formContext(context),
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

export async function action({ request, context, params }: Route.ActionArgs) {
  return applyProviderChange(context, params, await mutationForm(request, context))
}

export type ProvidersData = Route.ComponentProps['loaderData']

export default function ProvidersRoute({ loaderData }: Route.ComponentProps) {
  return <Outlet context={loaderData} />
}

import { data, Outlet, type ShouldRevalidateFunctionArgs } from 'react-router'
import type { Route } from './+types/tools'
import {
  formContext,
  managementFailure,
  mutationForm,
  privateHeaders,
} from '@/lib/management.server'
import { deletePrivateTool, privateGroup } from '@/lib/private-tools.server'
import { providerView, listAllProviders, toolUsage, uuidOr404 } from '@/lib/providers.server'
import type { SummaryDays } from '@/lib/activity-summary'
import { glimpseActivitySummary } from '@/lib/activity-summary.server'
import { personalAccountStates } from '@/lib/accounts.server'
import { applyProviderChange } from '@/lib/provider-changes.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { RECIPE_CHOICES } from '@/lib/provider-recipes/library.server'
import type { PolicyActionResult } from '@/views/providers/types'
import { addressedTool, PRIVATE_GROUP, toolsPageWorkspace } from '@/views/tools/addresses'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

/** Whether a policy result carries all the tree needs, so the page patches itself. */
function patchesTree(formData: FormData | undefined, result: PolicyActionResult | undefined) {
  return (
    formData?.get('intent') === 'policy' &&
    result?.blockers !== undefined &&
    !result.error &&
    result.tools?.every((tool) => tool.status === 'updated') === true
  )
}

// Selection changes on the page (group, tool) reuse the
// tree: they are one table, and the address only names what is revealed. A
// child loader still checks Workspace authority on each. A saved policy change
// also reuses it, as the whole catalog is a costly read to repeat for one
// toggle; the page applies the action's result. Refreshes, other mutations,
// query changes, and leaving the page reload it.
export function shouldRevalidate({
  currentUrl,
  nextUrl,
  formMethod,
  formData,
  actionResult,
  actionStatus,
}: ShouldRevalidateFunctionArgs) {
  if (patchesTree(formData, actionResult as PolicyActionResult | undefined)) return false
  if ((formMethod && formMethod.toUpperCase() !== 'GET') || actionStatus !== undefined) return true
  if (currentUrl.search !== nextUrl.search || currentUrl.pathname === nextUrl.pathname) return true
  const current = toolsPageWorkspace(currentUrl.pathname)
  return !current || current !== toolsPageWorkspace(nextUrl.pathname)
}

/** Loads the Workspace tree independently of tool selection. */
export async function loader({ context, params, request }: Route.LoaderArgs) {
  try {
    const { api, workspace: w } = await authorizedWorkspace(context, params.workspace)
    const days: SummaryDays = 30
    const [listed, personalAccounts, summary, privateToolsGroup] = await Promise.all([
      listAllProviders(api, w.name),
      personalAccountStates(api, w.name),
      glimpseActivitySummary(api, w.name, days, request.signal),
      privateGroup(api, w),
    ])
    // The page orders the groups; the private one is not in Coral's list.
    const providers = [
      ...(await Promise.all(
        listed.map((provider) => providerView(api, provider, w.path, personalAccounts)),
      )),
      privateToolsGroup,
    ]
    // An address that names a group or a tool the catalog does not hold is not found.
    const namesGroup = params.group !== undefined || params.provider !== undefined
    const group =
      params.group === PRIVATE_GROUP
        ? privateToolsGroup
        : params.provider === undefined
          ? undefined
          : providers.find((i) => i.id === uuidOr404(params.provider!))
    const { tool } = params
    if ((namesGroup && !group) || (tool !== undefined && !(group && addressedTool(group, tool))))
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    return data(
      {
        workspace: w,
        providers,
        toolCalls: summary && { days, tools: toolUsage(summary.tools) },
        recipes: RECIPE_CHOICES,
        form: formContext(context),
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

/**
 * A change from the page. Every member deletes their own private tools; the
 * rest are an owner's changes to providers and policy.
 */
export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  if (form.get('intent') === 'deletePrivateTool')
    return deletePrivateTool(context, params.workspace, form)
  return applyProviderChange(context, params, form)
}

export type ToolsData = Route.ComponentProps['loaderData']

export default function ToolsRoute({ loaderData }: Route.ComponentProps) {
  return <Outlet context={loaderData} />
}

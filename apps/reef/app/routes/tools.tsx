import { ConnectError } from '@connectrpc/connect'
import { data, Outlet, type ShouldRevalidateFunctionArgs } from 'react-router'
import type { Route } from './+types/tools'
import {
  formContext,
  managementFailure,
  mutationForm,
  privateHeaders,
} from '@/lib/management.server'
import { deletePrivateTool, privateGroup } from '@/lib/private-tools.server'
import { removeSharedTool, sharedGroup } from '@/lib/shared-tools.server'
import { providerView, listAllProviders, toolUsage, uuidOr404 } from '@/lib/providers.server'
import type { SummaryDays } from '@/lib/activity-summary'
import { glimpseActivitySummary } from '@/lib/activity-summary.server'
import { personalAccountStates } from '@/lib/accounts.server'
import { applyProviderChange } from '@/lib/provider-changes.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { useSettled } from '@/lib/use-settled'
import { RECIPE_CHOICES } from '@/lib/provider-recipes/library.server'
import type { PolicyActionResult, ToolCallCounts, ToolCallsAbsent } from '@/views/providers/types'
import {
  addressedTool,
  PRIVATE_GROUP,
  SHARED_GROUP,
  toolsPageWorkspace,
} from '@/views/tools/addresses'

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
    // Streamed, not awaited: the catalog, a saved change and a refresh never wait for a
    // slow summary. The page shows a failed read as unavailable, never as no calls.
    const toolCalls = glimpseActivitySummary(api, w.name, days, request.signal).then(
      (summary) => summary && { days, tools: toolUsage(summary.tools) },
    )
    toolCalls.catch((error: unknown) => {
      // A defect in Reef itself must not hide behind an unavailable summary.
      if (!(error instanceof ConnectError))
        console.error('Unexpected failure in a Reef loader:', error)
    })
    // Each provider's tools are read once the provider list arrives, not after the rest.
    const personalAccounts = personalAccountStates(api, w.name)
    const [listed, sharedToolsGroup, privateToolsGroup] = await Promise.all([
      listAllProviders(api, w.name).then((list) =>
        Promise.all(list.map((provider) => providerView(api, provider, w.path, personalAccounts))),
      ),
      sharedGroup(api, w),
      privateGroup(api, w),
      personalAccounts,
    ])
    // The page orders the groups; the shared and the private one are not in Coral's list.
    const providers = [...listed, sharedToolsGroup, privateToolsGroup]
    // An address that names a group or a tool the catalog does not hold is not found.
    const namesGroup = params.group !== undefined || params.provider !== undefined
    const group =
      params.group === PRIVATE_GROUP
        ? privateToolsGroup
        : params.group === SHARED_GROUP
          ? sharedToolsGroup
          : params.provider === undefined
            ? undefined
            : providers.find((i) => i.id === uuidOr404(params.provider!))
    const { tool, agent } = params
    // Only the private group holds an agent's tools.
    if (
      (namesGroup && !group) ||
      (agent !== undefined && group !== privateToolsGroup) ||
      (tool !== undefined && !(group && addressedTool(group, tool, agent ?? null)))
    )
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    return data(
      {
        workspace: w,
        providers,
        usageDays: days,
        toolCalls,
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
 * rest are an owner's changes to shared tools, providers and policy.
 */
export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  if (form.get('intent') === 'deletePrivateTool')
    return deletePrivateTool(context, params.workspace, form)
  if (form.get('intent') === 'removeSharedTool')
    return removeSharedTool(context, params.workspace, form)
  return applyProviderChange(context, params, form)
}

/** The tree each selection renders, with the usage settled so far. */
export type ToolsData = Omit<Route.ComponentProps['loaderData'], 'toolCalls' | 'usageDays'> & {
  toolCalls: ToolCallCounts | ToolCallsAbsent
}

export default function ToolsRoute({ loaderData }: Route.ComponentProps) {
  const { toolCalls: read, usageDays, ...tree } = loaderData
  const toolCalls = useSettledUsage(read, usageDays)
  return <Outlet context={{ ...tree, toolCalls } satisfies ToolsData} />
}

/** The usage of this read: loading until it settles, and unavailable, never zeros, when it fails. */
function useSettledUsage(
  read: Promise<ToolCallCounts | null>,
  days: number,
): ToolCallCounts | ToolCallsAbsent {
  const settled = useSettled(read)
  if (settled?.read !== read) return { days, state: 'loading' }
  return settled.value ?? { days, state: 'unavailable' }
}

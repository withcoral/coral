import { data, redirect, useSearchParams, type ShouldRevalidateFunctionArgs } from 'react-router'
import type { Route } from './+types/workspace'
import { ProviderState } from '@/generated/coral/management/v1/provider_pb'
import { isBuiltinProvider } from '@/lib/call-root'
import {
  DEFAULT_SUMMARY_DAYS,
  summaryDays,
  type ActivitySummaryData,
  type SummaryDays,
} from '@/lib/activity-summary'
import { glimpseActivitySummary } from '@/lib/activity-summary.server'
import { useActivitySummary } from '@/lib/use-activity-summary'
import {
  actionFailure,
  field,
  formContext,
  isLocalDeployment,
  management,
  managementFailure,
  mutationForm,
  privateHeaders,
  workspaceName,
} from '@/lib/management.server'
import { agentView, membershipView, ownerView } from '@/lib/management-views.server'
import { RECIPE_CHOICES } from '@/lib/provider-recipes/library.server'
import { blockerLabel, listAllProviders, providerPath } from '@/lib/providers.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { ActivitySummary } from '@/views/activity/summary/summary'
import { GLIMPSE_FETCH, WorkspaceOverview } from '@/views/management/workspace-overview'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

const withoutDays = (url: URL) => {
  const query = new URLSearchParams(url.search)
  query.delete('days')
  return `${url.pathname}?${query}`
}

// A new summary period reaches the summary through its stream, whose first value is a
// fresh read. The rest of the Overview stays. Everything else reloads it.
export function shouldRevalidate({
  currentUrl,
  nextUrl,
  formMethod,
}: ShouldRevalidateFunctionArgs) {
  return (
    formMethod !== undefined ||
    currentUrl.search === nextUrl.search ||
    withoutDays(currentUrl) !== withoutDays(nextUrl)
  )
}

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace: w } = await authorizedWorkspace(request, context, params.workspace)
    // An archived Workspace has only its history to show.
    if (w.state === 'archived') return redirect(`${w.path}/activity`, { headers: privateHeaders })
    const owner = w.effectiveRole === 'owner'
    const people = !isLocalDeployment(context)
    const days = summaryDays(new URL(request.url).searchParams.get('days')) ?? DEFAULT_SUMMARY_DAYS
    const [agents, providers, owners, own, summary] = await Promise.all([
      owner ? api.agents.listAgents({ parent: w.name, pageSize: GLIMPSE_FETCH }) : null,
      listAllProviders(api, w.name),
      people ? api.memberships.listWorkspaceOwners({ parent: w.name }) : null,
      owner || !people
        ? null
        : api.memberships.getWorkspaceMembership({
            name: `${w.name}/memberships/${w.currentUser.slice('users/'.length)}`,
          }),
      glimpseActivitySummary(api, w.name, days, request.signal),
    ])
    return data(
      {
        workspace: w,
        agents: agents?.agents.map(agentView) ?? [],
        // The card shows the providers you add. Coral's Built-in Provider stays on
        // the providers page, so a new workspace still offers Add provider here.
        providers: providers
          .filter((provider) => !isBuiltinProvider(provider.callRoot))
          .map((provider) => ({
            id: provider.name.split('/').at(-1)!,
            displayName: provider.displayName,
            namespace: provider.namespace,
            path: providerPath(w.path, provider.name),
            blockers: [
              ...(provider.state === ProviderState.DISABLED ? ['Disabled'] : []),
              ...provider.blockers.map(blockerLabel),
            ],
            icons: RECIPE_CHOICES.find((r) => r.id === provider.namespace)?.icons,
          })),
        owners: owners?.owners.map(ownerView) ?? null,
        ownMembership: own ? membershipView(own) : null,
        form: formContext(context),
        recipes: RECIPE_CHOICES,
        summary,
        days,
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

const saved: Record<string, string> = {
  rename: 'Workspace name saved.',
  archive: 'Workspace archived.',
  restore: 'Workspace restored.',
}
const failed: Record<string, string> = {
  rename: 'Coral could not save the workspace name. ',
  archive: 'Coral could not archive the workspace. ',
  restore: 'Coral could not restore the workspace. ',
}

/** Renames, archives and restores the Workspace. The sidebar and banners post here from any page. */
export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  const intent = field(form, 'intent')
  if (!Object.hasOwn(saved, intent)) throw new Response('Invalid action', { status: 400 })
  const api = management(request, context).workspaces
  const name = workspaceName(params.workspace)
  const etag = field(form, 'etag')
  try {
    if (intent === 'rename')
      await api.updateWorkspace({
        workspace: { name, etag, displayName: field(form, 'displayName') },
        updateMask: { paths: ['display_name'] },
      })
    else if (intent === 'archive') await api.archiveWorkspace({ name, etag })
    else await api.restoreWorkspace({ name, etag })
    return data(
      { error: null, credential: null, message: saved[intent] },
      { headers: privateHeaders },
    )
  } catch (error) {
    return actionFailure(error, failed[intent])
  }
}

export default function WorkspaceRoute({ loaderData, params }: Route.ComponentProps) {
  // Keyed by workspace, not location: a new summary period navigates within this page.
  return (
    <WorkspaceOverview
      key={loaderData.workspace.name}
      {...loaderData}
      summary={
        loaderData.summary && (
          <LiveSummary
            initial={{ days: loaderData.days, summary: loaderData.summary }}
            workspace={params.workspace}
          />
        )
      }
    />
  )
}

function LiveSummary({
  initial,
  workspace,
}: {
  initial: { days: SummaryDays; summary: ActivitySummaryData }
  /** The Workspace ID, as the route names it. */
  workspace: string
}) {
  const [searchParams, setSearchParams] = useSearchParams()
  const days = summaryDays(searchParams.get('days')) ?? DEFAULT_SUMMARY_DAYS
  const shown = useActivitySummary(initial, workspace, days)
  return (
    <ActivitySummary
      {...shown.summary}
      days={days}
      pending={shown.days !== days}
      onDaysChange={(next) =>
        setSearchParams(
          (params) => {
            if (next === DEFAULT_SUMMARY_DAYS) params.delete('days')
            else params.set('days', String(next))
            return params
          },
          { replace: true, preventScrollReset: true },
        )
      }
    />
  )
}

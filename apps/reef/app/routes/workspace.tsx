import { data, redirect, useLocation, useNavigate, useNavigation } from 'react-router'
import type { Route } from './+types/workspace'
import { ProviderState } from '@/generated/coral/management/v1/provider_pb'
import { isBuiltinProvider } from '@/lib/call-root'
import { DEFAULT_SUMMARY_DAYS, summaryDays, summarySearch } from '@/lib/activity-summary'
import { glimpseActivitySummary } from '@/lib/activity-summary.server'
import { useRevalidateWhileVisible } from '@/lib/use-revalidate-while-visible'
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
import type { RecipeIcons } from '@/lib/provider-recipes/choice'
import { ActivitySummary, type SummaryLinks } from '@/views/activity/summary/summary'
import { GLIMPSE_FETCH, WorkspaceOverview } from '@/views/management/workspace-overview'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export function shouldRevalidate() {
  return true
}

/** How often an open Overview reads its summary again. */
const SUMMARY_REFRESH_MS = 30_000

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace: w } = await authorizedWorkspace(request, context, params.workspace)
    // An archived Workspace has only its history to show.
    if (w.state === 'archived') return redirect(`${w.path}/activity`, { headers: privateHeaders })
    const owner = w.effectiveRole === 'owner'
    const people = !isLocalDeployment(context)
    const url = new URL(request.url)
    const query = url.searchParams.get('days')
    // A period the page does not offer leaves the URL, so the URL names what you see.
    if (query !== null && summaryDays(query) === undefined)
      return redirect(`${url.pathname}${summarySearch(url.search, DEFAULT_SUMMARY_DAYS)}`, {
        headers: privateHeaders,
      })
    const days = summaryDays(query) ?? DEFAULT_SUMMARY_DAYS
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
        activity: { days, summary },
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

export default function WorkspaceRoute({ loaderData }: Route.ComponentProps) {
  const links = summaryLinks(loaderData.workspace.path, loaderData.providers)
  // Keyed by workspace, not location: a new summary period navigates within this page.
  return (
    <WorkspaceOverview
      key={loaderData.workspace.name}
      {...loaderData}
      summary={
        <LiveSummary
          {...loaderData.activity}
          owner={loaderData.workspace.effectiveRole === 'owner'}
          links={links}
        />
      }
    />
  )
}

/** The pages a summary row opens, found by the provider namespace in its Call Path. */
function summaryLinks(
  workspacePath: string,
  providers: readonly {
    namespace: string
    path: string
    displayName: string
    icons?: RecipeIcons
  }[],
): SummaryLinks {
  const providerOf = (callPath: string) =>
    providers.find((p) => callPath.startsWith(`coral.providers.${p.namespace}.`))
  return {
    provider: (callPath) => {
      const p = providerOf(callPath)
      return p && { name: p.displayName, path: p.path, icons: p.icons }
    },
    operation: (callPath) => {
      const p = providerOf(callPath)
      if (!p) return undefined
      const operation = callPath.slice(`coral.providers.${p.namespace}.`.length)
      return {
        operation: `${p.path}/ops/${encodeURIComponent(operation)}`,
        settings: `${p.path}/settings`,
      }
    },
  }
}

/** The loader's summary, read again while you look at it. While a new period loads, the
 * picker shows that period and the numbers fade.
 */
function LiveSummary({
  days,
  summary,
  owner,
  links,
}: Route.ComponentProps['loaderData']['activity'] & { owner: boolean; links: SummaryLinks }) {
  useRevalidateWhileVisible(SUMMARY_REFRESH_MS)
  const location = useLocation()
  const navigation = useNavigation()
  const navigate = useNavigate()
  const target =
    navigation.location?.pathname === location.pathname ? navigation.location : location
  const chosen = summaryDays(new URLSearchParams(target.search).get('days')) ?? DEFAULT_SUMMARY_DAYS
  return (
    <ActivitySummary
      summary={summary}
      links={links}
      owner={owner}
      days={chosen}
      pending={chosen !== days}
      onDaysChange={(next) =>
        void navigate(
          { search: summarySearch(location.search, next) },
          { replace: true, preventScrollReset: true },
        )
      }
    />
  )
}

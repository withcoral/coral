import { data, redirect } from 'react-router'
import type { Route } from './+types/workspace'
import { ProviderState } from '@/generated/coral/management/v1/provider_pb'
import { OVERVIEW_ACTIVITY_ROOTS } from '@/lib/activity-live-types'
import { loadActivity } from '@/lib/activity.server'
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
import { GLIMPSE_FETCH, WorkspaceOverview } from '@/views/management/workspace-overview'
import { LiveActivity } from './activity-live'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export function shouldRevalidate() {
  return true
}

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace: w } = await authorizedWorkspace(request, context, params.workspace)
    // An archived Workspace has only its history to show.
    if (w.state === 'archived') return redirect(`${w.path}/activity`, { headers: privateHeaders })
    const owner = w.effectiveRole === 'owner'
    const people = !isLocalDeployment(context)
    const [agents, providers, owners, own, activity] = await Promise.all([
      owner ? api.agents.listAgents({ parent: w.name, pageSize: GLIMPSE_FETCH }) : null,
      listAllProviders(api, w.name),
      people ? api.memberships.listWorkspaceOwners({ parent: w.name }) : null,
      owner || !people
        ? null
        : api.memberships.getWorkspaceMembership({
            name: `${w.name}/memberships/${w.currentUser.slice('users/'.length)}`,
          }),
      loadActivity(request, api, {
        scope: { workspace: w.name },
        listPath: `/${w.name}`,
        pageSize: OVERVIEW_ACTIVITY_ROOTS,
      }),
    ])
    return data(
      {
        workspace: w,
        agents: agents?.agents.map(agentView) ?? [],
        providers: providers.map((provider) => ({
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
        activity,
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
  // Keyed by workspace, not location: the activity reader navigates within this page.
  return (
    <WorkspaceOverview
      key={loaderData.workspace.name}
      {...loaderData}
      activity={
        <LiveActivity
          initial={loaderData.activity}
          more={{
            to: `${loaderData.workspace.path}/activity`,
            label: 'View all activity',
            roots: OVERVIEW_ACTIVITY_ROOTS,
          }}
        />
      }
    />
  )
}

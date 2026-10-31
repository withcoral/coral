import { data, useLocation, useNavigation } from 'react-router'
import type { Route } from './+types/agents'
import {
  actionFailure,
  field,
  formContext,
  management,
  managementFailure,
  mutationForm,
  privateHeaders,
  workspaceName,
} from '@/lib/management.server'
import { agentView } from '@/lib/management-views.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { AgentsPage } from '@/views/management/agents-page'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export function shouldRevalidate() {
  return true
}

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace: w } = await authorizedWorkspace(context, params.workspace, {
      ownerOnly: true,
    })
    const page = await api.agents.listAgents({
      parent: w.name,
      pageToken: new URL(request.url).searchParams.get('page') ?? '',
    })
    return data(
      {
        workspace: w,
        agents: page.agents.map(agentView),
        nextPage: page.nextPageToken,
        form: formContext(context),
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  const api = management(context)
  const name = workspaceName(params.workspace)
  try {
    if (field(form, 'intent') !== 'createAgent')
      throw new Response('Invalid action', { status: 400 })
    const created = await api.agents.createAgent({
      parent: name,
      displayName: field(form, 'displayName'),
      requestId: field(form, 'requestId'),
    })
    return data(
      {
        error: null,
        credential: created.credential,
        agentDisplayName: created.agent?.displayName,
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    return actionFailure(error, 'Coral could not create the agent. ')
  }
}

export default function AgentsRoute({ loaderData, actionData }: Route.ComponentProps) {
  return (
    <AgentsPage
      key={useLocation().key}
      {...loaderData}
      result={actionData}
      pending={useNavigation().state !== 'idle'}
    />
  )
}

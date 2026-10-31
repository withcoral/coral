import { data, redirect, useNavigation } from 'react-router'
import type { Route } from './+types/agent-settings'
import {
  actionFailure,
  agentName,
  agentState,
  field,
  formContext,
  management,
  managementFailure,
  mutationForm,
  privateHeaders,
  registrationName,
  resourcePath,
  workspaceName,
} from '@/lib/management.server'
import { agentView, registrationView, workspaceView } from '@/lib/management-views.server'
import { AgentSettingsPage } from '@/views/management/agent-settings-page'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export async function loader({ context, params }: Route.LoaderArgs) {
  const api = management(context)
  const name = agentName(params.workspace, params.agent)
  try {
    const [workspace, agent, registration] = await Promise.all([
      api.workspaces.getWorkspace({ name: workspaceName(params.workspace) }),
      api.agents.getAgent({ name }),
      api.registrations.getAgentRegistration({ name: registrationName(name) }),
    ])
    return data(
      {
        workspace: workspaceView(workspace),
        agent: agentView(agent),
        registration: registrationView(registration),
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
  const name = agentName(params.workspace, params.agent)
  const intent = field(form, 'intent')
  try {
    const etag = field(form, 'etag')
    switch (intent) {
      case 'rotate': {
        const issued = await api.registrations.rotateAgentCredential({
          name: registrationName(name),
          etag,
          requestId: field(form, 'requestId'),
        })
        return data({ error: null, credential: issued.credential }, { headers: privateHeaders })
      }
      case 'agentState':
        await api.agents.updateAgent({
          agent: { name, etag, state: agentState(field(form, 'state')) },
          updateMask: { paths: ['state'] },
        })
        break
      case 'registrationState':
        await api.registrations.updateAgentRegistration({
          registration: {
            name: registrationName(name),
            etag,
            enabled: field(form, 'enabled') === 'true',
          },
          updateMask: { paths: ['enabled'] },
        })
        break
      case 'expiration': {
        const value = field(form, 'expiration')
        const milliseconds = value ? Date.parse(`${value}Z`) : null
        if (milliseconds !== null && !Number.isFinite(milliseconds))
          throw new Response('Invalid expiration', { status: 400 })
        await api.registrations.updateAgentRegistration({
          registration: {
            name: registrationName(name),
            etag,
            expireTime:
              milliseconds === null
                ? undefined
                : {
                    seconds: BigInt(Math.floor(milliseconds / 1000)),
                    nanos: 0,
                  },
          },
          updateMask: { paths: ['expire_time'] },
        })
        break
      }
      case 'delete':
        await api.agents.deleteAgent({ name, etag })
        return redirect(`${resourcePath(workspaceName(params.workspace))}/agents`, {
          headers: privateHeaders,
        })
      default:
        throw new Response('Invalid action', { status: 400 })
    }
    const messages: Record<string, string> = {
      agentState:
        form.get('state') === 'enabled'
          ? 'Agent enabled. Other access restrictions still apply.'
          : 'Agent disabled.',
      registrationState: 'API access saved. Other access restrictions still apply.',
      expiration: 'Expiration date saved. Other access restrictions still apply.',
    }
    return data(
      {
        error: null,
        credential: null,
        message: messages[intent],
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    const failures: Record<string, string> = {
      rotate: 'Coral could not replace the API key. ',
      agentState: 'Coral could not enable or disable the agent. ',
      registrationState: 'Coral could not change API access. ',
      expiration: 'Coral could not save the expiration date. ',
      delete: 'Coral could not delete the agent. ',
    }
    return actionFailure(error, failures[intent])
  }
}

export default function AgentSettingsRoute({ loaderData, actionData }: Route.ComponentProps) {
  const navigation = useNavigation()
  return (
    <AgentSettingsPage
      key={loaderData.agent.name}
      {...loaderData}
      result={actionData}
      pending={navigation.state !== 'idle'}
    />
  )
}

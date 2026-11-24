import { Code, ConnectError } from '@connectrpc/connect'
import { data } from 'react-router'
import type { Route } from './+types/sub-agent-consent'
import { ToolAccessState } from '@/generated/coral/management/v1/policy_pb'
import {
  actionFailure,
  failureReason,
  field,
  managementFailure,
  mutationForm,
  privateHeaders,
} from '@/lib/management.server'
import { accessStateOf } from '@/lib/providers.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import type { SubAgentConsentRead } from '@/views/tools/types'

/**
 * The dialog reads the model when it opens and after Coral refuses its answer. A read on any
 * other change would replace the model under a person who is about to confirm it.
 */
export function shouldRevalidate() {
  return false
}

/** Where the sub-agent sends this workspace's data. Only a workspace owner can read it. */
export async function loader({ context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace } = await authorizedWorkspace(context, params.workspace, {
      ownerOnly: true,
    })
    return data(await consentRead(api, workspace.name), { headers: privateHeaders })
  } catch (error) {
    managementFailure(error)
  }
}

type Api = Awaited<ReturnType<typeof authorizedWorkspace>>['api']

/** The model, or why there is none to show. The dialog shows the reason in its place. */
async function consentRead(api: Api, workspace: string): Promise<SubAgentConsentRead> {
  try {
    const { id, settings } = await api.subAgentModel.getWorkspaceSubAgentModel({ workspace })
    if (!settings) throw new ConnectError('The model has no settings', Code.Internal)
    return {
      model: {
        id,
        host: new URL(settings.baseUrl).host,
        name: settings.model,
      },
      problem: null,
    }
  } catch (error) {
    // A lapsed sign-in fails the read, so the protected layout signs the person in again.
    if (error instanceof ConnectError && error.code === Code.Unauthenticated) throw error
    const [, problem] = failureReason(
      error,
      '',
      'The organization has no model for the sub-agent now. Reload the page.',
    )
    return { model: null, problem }
  }
}

/** Turns `coral.agent` on for the model that the dialog showed. */
export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  const accessState = accessStateOf(field(form, 'access'))
  if (accessState === undefined || accessState === ToolAccessState.DISABLED)
    throw new Response('Invalid form', { status: 400, headers: privateHeaders })
  try {
    const { api, workspace } = await authorizedWorkspace(context, params.workspace, {
      ownerOnly: true,
    })
    await api.subAgentModel.turnOnSubAgent({
      workspace: workspace.name,
      modelId: field(form, 'modelId'),
      accessState,
    })
    return data({ error: null, credential: null }, { headers: privateHeaders })
  } catch (error) {
    if (error instanceof ConnectError && error.rawMessage === 'SUB_AGENT_MODEL_CHANGED')
      return data(
        {
          error:
            'The model changed after you opened this dialog. Read the new values before you turn on the sub-agent.',
          credential: null,
        },
        { status: 409, headers: privateHeaders },
      )
    return actionFailure(
      error,
      'Coral could not turn on the sub-agent. ',
      'This workspace does not have the tool now. Reload the page.',
    )
  }
}

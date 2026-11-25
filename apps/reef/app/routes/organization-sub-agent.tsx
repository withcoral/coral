import { data, redirect } from 'react-router'
import type { Route } from './+types/organization-sub-agent'
import type { SubAgentModel } from '@/generated/coral/management/v1/sub_agent_model_pb'
import {
  actionFailure,
  failureReason,
  field,
  formContext,
  management,
  managementFailure,
  mutationForm,
  privateHeaders,
} from '@/lib/management.server'
import { isoTime } from '@/lib/management-views.server'
import { organizationMember } from '@/lib/organization.server'
import {
  enteredModel,
  saveConflict,
  settingRefusal,
  wireProtocols,
} from '@/lib/sub-agent-model.server'
import { SubAgentPage } from '@/views/sub-agent/sub-agent-page'
import { PROTOCOLS, type RefusedSave, type SubAgentModelView } from '@/views/sub-agent/types'
import { formatJson } from '@/wax/components/code-block/format-json'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

const path = '/organization/sub-agent'

/** Only an organization owner can read the model; Coral denies anyone else. */
export async function loader({ context }: Route.LoaderArgs) {
  try {
    const api = management(context)
    const { model } = await api.subAgentModel.getSubAgentModel({})
    const changedBy = model ? await organizationMember(api, model.changedBy) : null
    return data(
      { model: model ? modelView(model, changedBy) : null, form: formContext(context) },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

export async function action({ request, context }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  const intent = field(form, 'intent')
  const api = management(context).subAgentModel
  if (intent === 'remove') {
    try {
      await api.deleteSubAgentModel({ etag: field(form, 'etag') })
      return redirect(`${path}?removed=1`, { headers: privateHeaders })
    } catch (error) {
      return actionFailure(
        error,
        'Coral could not remove the model. ',
        'Another person already removed it.',
      )
    }
  }
  if (intent !== 'save') throw new Response('Invalid action', { status: 400 })

  const { values, keyChange, settings, apiKey, refusal } = enteredModel(form)
  const etag = form.has('etag') ? field(form, 'etag') : null
  const refuse = (status: number, refused: Pick<RefusedSave, 'field' | 'message' | 'conflict'>) =>
    // The page does not get the key back, so a person who typed one must type it again.
    data(
      { ...refused, values, keyChange: keyChange === 'optional' && apiKey ? 'replace' : keyChange },
      { status, headers: privateHeaders },
    )
  if (refusal) return refuse(400, refusal)
  try {
    if (etag === null)
      await api.createSubAgentModel({ settings, apiKey: apiKey === '' ? undefined : apiKey })
    else await api.updateSubAgentModel({ settings, etag, apiKey })
    return redirect(`${path}?saved=1`, { headers: privateHeaders })
  } catch (error) {
    // The page's banner says that the save failed, so the message is the reason alone.
    const [status, message] = failureReason(error, '')
    return refuse(status, settingRefusal(error) ?? saveConflict(error) ?? { message })
  }
}

/** A protocol that this Reef does not know has no choice in the form, so the page does not open. */
function modelView(
  model: SubAgentModel,
  changedBy: SubAgentModelView['changedBy'],
): SubAgentModelView {
  const { settings } = model
  const protocol = PROTOCOLS.find((p) => wireProtocols[p] === settings?.protocol)
  if (!settings || !protocol)
    throw new Response('Unrecognized management classification', { status: 503 })
  return {
    values: {
      protocol,
      baseUrl: settings.baseUrl,
      model: settings.model,
      contextWindow: settings.contextWindow?.toString() ?? '',
      requestArgs: formatJson(settings.requestArgsJson),
    },
    hasApiKey: model.hasApiKey,
    changedBy,
    changedAt: isoTime(model.changeTime),
    etag: model.etag,
  }
}

export default function OrganizationSubAgentRoute({
  loaderData,
  actionData,
}: Route.ComponentProps) {
  return (
    <SubAgentPage
      {...loaderData}
      refused={actionData && 'values' in actionData ? actionData : undefined}
    />
  )
}

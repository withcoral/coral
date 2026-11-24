import { Code, ConnectError } from '@connectrpc/connect'
import { data, redirect } from 'react-router'
import type { Route } from './+types/organization-sub-agent'
import {
  type SubAgentModel,
  SubAgentModelProtocol,
} from '@/generated/coral/management/v1/sub_agent_model_pb'
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
import { SubAgentPage } from '@/views/sub-agent/sub-agent-page'
import {
  KEY_CHANGES,
  type ModelValues,
  type Protocol,
  PROTOCOLS,
  type RefusedSave,
  type SubAgentModelView,
} from '@/views/sub-agent/types'
import { formatJson } from '@/wax/components/code-block/format-json'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

const path = '/organization/sub-agent'
/** The most that the form takes as arguments, so that Coral refuses what is too large for it. */
const ARGS_MAX = 64 * 1024
/** The largest window the wire holds. Coral decides which windows are too small. */
const UINT32_MAX = 0xffff_ffff

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

  const values = modelValues(form)
  const keyChange = oneOf(KEY_CHANGES, field(form, 'keyChange'))
  const key = keyChange === 'replace' || keyChange === 'optional' ? field(form, 'apiKey') : ''
  const etag = form.has('etag') ? field(form, 'etag') : null
  const contextWindow = windowTokens(values.contextWindow)
  const refuse = (status: number, refusal: Refusal) =>
    // The page does not get the key back, so a person who typed one must type it again.
    data(
      { ...refusal, values, keyChange: keyChange === 'optional' && key ? 'replace' : keyChange },
      { status, headers: privateHeaders },
    )
  // A window that is no number has no place on the wire, and an empty key would clear the saved one.
  if (contextWindow === null) return refuse(400, refusals.INVALID_CONTEXT_WINDOW)
  if (keyChange === 'replace' && key === '')
    return refuse(400, { field: 'apiKey', message: 'Enter an API key.' })

  const settings = {
    protocol: wireProtocols[values.protocol],
    baseUrl: values.baseUrl,
    model: values.model,
    contextWindow,
    requestArgsJson: values.requestArgs,
  }
  try {
    if (etag === null) await api.createSubAgentModel({ settings, apiKey: key || undefined })
    else
      await api.updateSubAgentModel({
        settings,
        etag,
        // Absent keeps the saved key, and empty clears it.
        apiKey: keyChange === 'keep' ? undefined : key,
      })
    return redirect(`${path}?saved=1`, { headers: privateHeaders })
  } catch (error) {
    // The page's banner says that the save failed, so the message is the reason alone.
    const [status, message] = failureReason(error, '')
    return refuse(status, refusalOf(error) ?? { message })
  }
}

const wireProtocols: Record<Protocol, SubAgentModelProtocol> = {
  chat_completions: SubAgentModelProtocol.CHAT_COMPLETIONS,
  openai_responses: SubAgentModelProtocol.OPENAI_RESPONSES,
  anthropic: SubAgentModelProtocol.ANTHROPIC,
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

function modelValues(form: FormData): ModelValues {
  return {
    protocol: oneOf(PROTOCOLS, field(form, 'protocol')),
    baseUrl: field(form, 'baseUrl'),
    model: field(form, 'model'),
    contextWindow: field(form, 'contextWindow'),
    // A form submission ends each line of a text area with a carriage return.
    requestArgs: field(form, 'requestArgs', ARGS_MAX).replaceAll('\r\n', '\n'),
  }
}

function oneOf<T extends string>(choices: readonly T[], value: string): T {
  const choice = choices.find((c) => c === value)
  if (choice === undefined)
    throw new Response('Invalid form', { status: 400, headers: privateHeaders })
  return choice
}

/** The window in tokens, undefined when the field is empty, or null for text that is no number. */
function windowTokens(text: string): number | undefined | null {
  // The page writes large numbers with commas, so the field takes them.
  const digits = text.trim().replaceAll(',', '')
  if (digits === '') return undefined
  const tokens = Number(digits)
  return /^\d+$/.test(digits) && tokens <= UINT32_MAX ? tokens : null
}

type Refusal = Pick<RefusedSave, 'field' | 'message' | 'conflict'>

/** What to show for each reason that Coral refuses a save with. */
const refusals = {
  INVALID_BASE_URL: {
    field: 'baseUrl',
    message: 'Enter an http or https URL. Leave out any username, password, query or fragment.',
  },
  DESTINATION_NOT_PERMITTED: {
    field: 'baseUrl',
    message: 'This deployment does not permit this URL. Enter a different URL.',
  },
  INVALID_MODEL: { field: 'model', message: 'Enter a model name of 200 characters or fewer.' },
  INVALID_API_KEY: {
    field: 'apiKey',
    message: 'Enter an API key without line breaks or other control characters.',
  },
  INVALID_CONTEXT_WINDOW: {
    field: 'contextWindow',
    message: 'Enter a whole number, 48,000 or more.',
  },
  INVALID_REQUEST_ARGS: {
    field: 'requestArgs',
    message: 'Enter a JSON object of 16 KB or less.',
  },
  REQUEST_ARG_NOT_ALLOWED: {
    field: 'requestArgs',
    message: 'Remove each name that is not in the list below.',
  },
  // The page shows what is saved now beside what the person typed.
  ETAG_MISMATCH: {
    conflict: true,
    message:
      'Another person changed the model after you opened this page. Compare your values with the saved values. Save again to replace the saved values.',
  },
  SUB_AGENT_MODEL_ALREADY_SET: {
    conflict: true,
    message:
      'Another person set the model after you opened this page. Compare your values with the saved values. Save again to replace the saved values.',
  },
} as const satisfies Record<string, Refusal>

function refusalOf(error: unknown): Refusal | null {
  if (!(error instanceof ConnectError)) return null
  const reason = error.rawMessage
  if (Object.hasOwn(refusals, reason)) return refusals[reason as keyof typeof refusals]
  if (error.code === Code.NotFound)
    return {
      conflict: true,
      message:
        'Another person removed the model after you opened this page. Save again to set the model.',
    }
  return null
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

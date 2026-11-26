import { Code, ConnectError } from '@connectrpc/connect'
import { SubAgentModelProtocol } from '@/generated/coral/management/v1/sub_agent_model_pb'
import {
  KEY_CHANGES,
  type ModelValues,
  type Protocol,
  PROTOCOLS,
  type RefusedSave,
  type SettingRefusal,
} from '@/views/sub-agent/types'
import { field, privateHeaders } from './management.server'

/** The most that the form takes as arguments, so that Coral refuses what is too large for it. */
const ARGS_MAX = 64 * 1024
/** The largest window the wire holds. Coral decides which windows are too small. */
const UINT32_MAX = 0xffff_ffff

export const wireProtocols: Record<Protocol, SubAgentModelProtocol> = {
  chat_completions: SubAgentModelProtocol.CHAT_COMPLETIONS,
  openai_responses: SubAgentModelProtocol.OPENAI_RESPONSES,
  anthropic: SubAgentModelProtocol.ANTHROPIC,
}

/** The page's form, as a save or a test reads it. */
export function enteredModel(form: FormData) {
  const values: ModelValues = {
    protocol: oneOf(PROTOCOLS, field(form, 'protocol')),
    baseUrl: field(form, 'baseUrl'),
    model: field(form, 'model'),
    contextWindow: field(form, 'contextWindow'),
    // A form submission ends each line of a text area with a carriage return.
    requestArgs: field(form, 'requestArgs', ARGS_MAX).replaceAll('\r\n', '\n'),
  }
  const keyChange = oneOf(KEY_CHANGES, field(form, 'keyChange'))
  const typed = keyChange === 'replace' || keyChange === 'optional' ? field(form, 'apiKey') : ''
  // Spaces alone are no key.
  const typedKey = typed.trim() === '' ? '' : typed
  const contextWindow = windowTokens(values.contextWindow)
  // A window that is no number has no place on the wire, and an empty key would mean none.
  const refusal: SettingRefusal | null =
    contextWindow === null
      ? settingRefusals.INVALID_CONTEXT_WINDOW
      : keyChange === 'replace' && typedKey === ''
        ? { field: 'apiKey', message: 'Enter an API key.' }
        : null
  return {
    values,
    keyChange,
    refusal,
    settings: {
      protocol: wireProtocols[values.protocol],
      baseUrl: values.baseUrl,
      model: values.model,
      contextWindow: contextWindow ?? undefined,
      requestArgsJson: values.requestArgs,
    },
    // On the wire, absent keeps the saved key and empty means none.
    apiKey: keyChange === 'keep' ? undefined : typedKey,
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
  if (text.trim() === '') return undefined
  const tokens = Number(digits)
  return /^\d+$/.test(digits) && tokens <= UINT32_MAX ? tokens : null
}

/** What to show for each reason that Coral refuses a setting with. */
const settingRefusals = {
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
} as const satisfies Record<string, SettingRefusal>

/** The setting that Coral refused, or null when it refused something else. */
export function settingRefusal(error: unknown): SettingRefusal | null {
  return error instanceof ConnectError && Object.hasOwn(settingRefusals, error.rawMessage)
    ? settingRefusals[error.rawMessage as keyof typeof settingRefusals]
    : null
}

/** What to say about a save that another person's change came before, or null. */
export function saveConflict(error: unknown): Pick<RefusedSave, 'message' | 'conflict'> | null {
  if (!(error instanceof ConnectError)) return null
  // The page shows what is saved now beside what the person typed.
  const compare =
    'Compare your values with the saved values. Save again to replace the saved values.'
  if (error.rawMessage === 'ETAG_MISMATCH')
    return {
      conflict: true,
      message: `Another person changed the model after you opened this page. ${compare}`,
    }
  if (error.rawMessage === 'SUB_AGENT_MODEL_ALREADY_SET')
    return {
      conflict: true,
      message: `Another person set the model after you opened this page. ${compare}`,
    }
  if (error.code === Code.NotFound)
    return {
      conflict: true,
      message:
        'Another person removed the model after you opened this page. Save again to set the model.',
    }
  return null
}

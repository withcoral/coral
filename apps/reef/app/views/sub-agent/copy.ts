import type { ModelTestResult, Protocol, Service } from './types'

export const serviceLabels: Record<Service, string> = {
  anthropic: 'Anthropic',
  openai: 'OpenAI',
  other: 'Other',
}

/** Where each known service serves its models, and how. */
export const servicePresets: Record<
  Exclude<Service, 'other'>,
  { protocol: Protocol; baseUrl: string }
> = {
  anthropic: { protocol: 'anthropic', baseUrl: 'https://api.anthropic.com/v1' },
  openai: { protocol: 'openai_responses', baseUrl: 'https://api.openai.com/v1' },
}

export const protocolLabels: Record<Protocol, string> = {
  chat_completions: 'Chat Completions (OpenAI-compatible)',
  openai_responses: 'Responses (OpenAI)',
  anthropic: 'Messages (Anthropic)',
}

/** What to enter as the base URL. Coral adds the protocol's path to it. */
export const baseUrlHints: Record<Protocol, string> = {
  chat_completions:
    'Enter the URL before /chat/completions, such as https://models.example.com/v1.',
  openai_responses: 'Enter the URL before /responses, such as https://api.openai.com/v1.',
  anthropic: 'Enter the URL before /messages, such as https://api.anthropic.com/v1.',
}

/**
 * The arguments Coral sends, as `REQUEST_ARG_NAMES` in the `coral-agent-model` crate lists
 * them. Coral decides; this list only tells the person which names to use.
 */
export const requestArgNames = [
  'chat_template_kwargs',
  'max_completion_tokens',
  'max_output_tokens',
  'max_tokens',
  'output_config',
  'reasoning',
  'reasoning_effort',
  'service_tier',
  'speed',
  'temperature',
  'thinking',
  'top_k',
  'top_p',
]

/** What a test's outcome tells the person to do. A newer Coral can send one this Reef lacks. */
export function testOutcomeMessage(result: Exclude<ModelTestResult, { outcome: 'invalid' }>) {
  switch (result.outcome) {
    case 'answered':
      return 'The model answered, so you can save it.'
    case 'unreachable':
      return 'Check the base URL and its server, which did not answer.'
    case 'forbidden':
      return 'Enter a different base URL, because this deployment does not permit this server.'
    case 'unreadable':
      return 'Check the protocol, because Coral cannot read the answer of this server.'
    case 'refused':
      return refusedMessage(result.status)
    case 'failed':
      return result.message
    default:
      return 'Test the model again.'
  }
}

function refusedMessage(status: number) {
  if (status === 429 || status >= 500)
    return `Test again later, because the server answered with status ${status}.`
  const check =
    status === 401 || status === 403
      ? 'the API key'
      : status === 404
        ? 'the base URL and the model name'
        : 'the model name and the arguments'
  return `Check ${check}, which the server refused with status ${status}.`
}

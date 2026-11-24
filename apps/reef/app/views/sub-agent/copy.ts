import type { Protocol } from './types'

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

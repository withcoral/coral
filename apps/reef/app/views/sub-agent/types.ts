import type { PersonView } from '@/views/management/types'

/** The wire protocols an endpoint can speak, in the order the page offers them. */
export const PROTOCOLS = ['chat_completions', 'openai_responses', 'anthropic'] as const
export type Protocol = (typeof PROTOCOLS)[number]

/** Who serves the model. All but `other` fix the protocol and the base URL. */
export const SERVICES = ['anthropic', 'openai', 'other'] as const
export type Service = (typeof SERVICES)[number]

/** The model's settings as form text. The API key is not one: no response carries it. */
export interface ModelValues {
  protocol: Protocol
  baseUrl: string
  model: string
  /** Empty leaves the size to Coral. */
  contextWindow: string
  /** A JSON object, or empty for none. */
  requestArgs: string
}

/** The model as it is saved. */
export interface SubAgentModelView {
  values: ModelValues
  hasApiKey: boolean
  /** Null for a person who left the organization. */
  changedBy: PersonView | null
  /** An RFC 3339 UTC time. */
  changedAt: string
  etag: string
}

/** What a save does to the API key. Only a saved key can be kept. An optional one can be empty. */
export const KEY_CHANGES = ['keep', 'replace', 'clear', 'optional'] as const
export type KeyChange = (typeof KEY_CHANGES)[number]

/** A field that Coral can refuse on its own. */
export type RefusableField = Exclude<keyof ModelValues, 'protocol'> | 'apiKey'

/** A save that changed nothing, with what the person typed except the API key. */
export interface RefusedSave {
  values: ModelValues
  keyChange: KeyChange
  /** Why, and what to do. */
  message: string
  /** The field to correct. Without one, the message is about the whole form. */
  field?: RefusableField
  /** The model changed after the person opened the page. */
  conflict?: boolean
}

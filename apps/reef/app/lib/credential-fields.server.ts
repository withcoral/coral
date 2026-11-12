// Credential Fields travel as one map of field name to value, for every method a
// person types a secret into. The values stay in form data and the outgoing request.
import type { CredentialField } from '@/generated/coral/management/v1/provider_pb'
import type { CredentialFieldView } from '@/views/providers/types'
import { CREDENTIAL_INPUT_PREFIX as PREFIX, REFERENCE_INPUT_PREFIX } from './credential-field-names'
import { privateHeaders } from './management.server'
const MAX_FIELDS = 16
const MAX_VALUE_LENGTH = 16 * 1024
const MAX_REFERENCE_LENGTH = 1024

/** The fields Coral asks for, in name order, as the views show them. */
export function credentialFieldViews(
  fields: Record<string, CredentialField>,
): CredentialFieldView[] {
  return Object.entries(fields)
    .toSorted(([left], [right]) => left.localeCompare(right))
    .map(([name, { label, description, sensitive }]) => ({ name, label, description, sensitive }))
}

/** Whether a form carries any Credential Field value. */
export function hasCredentialValues(form: FormData) {
  return [...form.keys()].some((key) => key.startsWith(PREFIX))
}

/** Every `credential.<field>` value in a form. Coral checks they match the method's fields. */
export function credentialValuesFrom(form: FormData): Record<string, string> {
  const values: Record<string, string> = {}
  let entries = 0
  for (const [key, value] of form.entries()) {
    if (!key.startsWith(PREFIX)) continue
    // Count every entry, so repeating one field cannot carry unbounded values.
    entries += 1
    const field = key.slice(PREFIX.length)
    if (
      typeof value !== 'string' ||
      value.length > MAX_VALUE_LENGTH ||
      entries > MAX_FIELDS ||
      field in values
    )
      throw new Response('Invalid form', { status: 400, headers: privateHeaders })
    values[field] = value
  }
  if (entries === 0) throw new Response('Invalid form', { status: 400, headers: privateHeaders })
  return values
}

const invalidForm = () => new Response('Invalid form', { status: 400, headers: privateHeaders })

/**
 * Each Credential Field as a typed value or a Secret Reference. A reference is the
 * `reference.<field>.store`, `.path` and `.key` inputs, with store and path required.
 * Coral checks the fields match the method, and reads each reference once.
 */
export function credentialFieldsFrom(form: FormData) {
  const parts = new Map<string, { store?: string; path?: string; key?: string }>()
  let entries = 0
  for (const [name, value] of form.entries()) {
    if (!name.startsWith(REFERENCE_INPUT_PREFIX)) continue
    entries += 1
    const match = /^([a-z][a-z0-9_]*)\.(store|path|key)$/.exec(
      name.slice(REFERENCE_INPUT_PREFIX.length),
    )
    if (
      !match ||
      typeof value !== 'string' ||
      value.length > MAX_REFERENCE_LENGTH ||
      entries > MAX_FIELDS * 3
    )
      throw invalidForm()
    const [, field, part] = match
    const reference = parts.get(field) ?? {}
    if (Object.hasOwn(reference, part)) throw invalidForm()
    parts.set(field, { ...reference, [part]: value })
  }
  const secretReferences: Record<string, { secretStore: string; path: string; key: string }> = {}
  for (const [field, { store, path, key }] of parts) {
    if (!store || !path) throw invalidForm()
    secretReferences[field] = { secretStore: `secretStores/${store}`, path, key: key ?? '' }
  }
  if (parts.size === 0) return { credentialValues: credentialValuesFrom(form), secretReferences }
  return {
    credentialValues: hasCredentialValues(form) ? credentialValuesFrom(form) : {},
    secretReferences,
  }
}

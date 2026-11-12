// Credential Fields travel as one map of field name to value, for every method a
// person types a secret into. The values stay in form data and the outgoing request.
import type {
  CredentialField,
  CredentialFieldReference,
} from '@/generated/coral/management/v1/provider_pb'
import { SecretStoreState } from '@/generated/coral/management/v1/secret_store_pb'
import type { OfferedSecretStore } from '@/views/accounts/types'
import type { CredentialFieldView, SavedReferenceView } from '@/views/providers/types'
import { CREDENTIAL_INPUT_PREFIX as PREFIX, REFERENCE_INPUT_PREFIX } from './credential-field-names'
import { ProviderAccountScope } from '@/generated/coral/management/v1/account_pb'
import type { ManagementApi } from './management.server'
import { privateHeaders } from './management.server'
import { secretStoreId } from './organization.server'
import { secretStoreBlockers } from './secret-store-state'
const MAX_FIELDS = 16
const MAX_VALUE_LENGTH = 16 * 1024
const MAX_REFERENCE_LENGTH = 1024

/** What each refused Secret Reference tells the Workspace Owner to do. */
export const SECRET_STORE_PROBLEMS: Record<string, string> = {
  SECRET_REFERENCE_NOT_ALLOWED:
    'Only a shared account can use a secret store. Enter the value, or share the account.',
  SECRET_STORE_NOT_GRANTED:
    'This workspace has no grant for this secret store. Ask an organization owner to grant it.',
  SECRET_STORE_UNREACHABLE:
    'Coral cannot reach this secret store. Ask your operator to check its address and outbound settings.',
  SECRET_ENTRY_NOT_FOUND: 'The secret store has no entry at this path. Check the path.',
  SECRET_KEY_NOT_FOUND: 'This entry has no such key. Check the key.',
  SECRET_STORE_PERMISSION_DENIED:
    'The secret store denied this read. Check the path, or ask your operator to check its policy.',
  SECRET_STORE_MALFORMED_RESPONSE: 'The secret store sent an unusable value. Check the entry.',
  SECRET_STORE_NEEDS_ENTERPRISE:
    'This secret store needs the Enterprise Edition. Ask your operator about the Enterprise Edition.',
  SECRET_STORE_KIND_MISSING:
    'This build of Coral cannot read this kind of secret store. Ask your operator to check it.',
}

/** The stores a shared Account may name. Only a workspace owner can list them. */
export async function offeredSecretStores(
  api: ManagementApi,
  parent: string,
): Promise<OfferedSecretStore[]> {
  const { secretStores } = await api.secretStores.listWorkspaceSecretStores({ parent })
  // A store Coral cannot read now is still offered, marked, so a saved reference to it
  // stays visible.
  return secretStores
    .filter((store) => store.state !== SecretStoreState.REMOVED)
    .map((store) => ({
      name: secretStoreId(store.name),
      blockers: secretStoreBlockers(store.state),
    }))
}

/** A saved reference in the words the views use, with its store as a bare name. */
export function savedReferenceView(reference: CredentialFieldReference): SavedReferenceView {
  return {
    store: secretStoreId(reference.secretStore),
    path: reference.path,
    key: reference.key,
  }
}

/** The references a Provider's shared Account holds now, by field name. */
export async function savedSecretReferences(
  api: ManagementApi,
  parent: string,
  provider: string,
): Promise<Record<string, SavedReferenceView>> {
  const { account } = await api.accounts.getProviderAccount({
    parent,
    provider,
    scope: ProviderAccountScope.SHARED,
    agent: '',
  })
  return Object.fromEntries(
    Object.entries(account?.secretReferences ?? {}).map(([name, reference]) => [
      name,
      savedReferenceView(reference),
    ]),
  )
}

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

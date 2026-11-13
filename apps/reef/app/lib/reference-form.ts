// What a Secret Reference to one store holds, as the store's kind says, so the
// form asks for exactly that and a saved reference reads in the same words.
import type { SecretReferenceForm } from '@/generated/coral/management/v1/secret_store_pb'

/** The parts of a reference to one store, as the views use them. */
export interface ReferenceFormView {
  /** What the path is, such as "Path" or "Variable name". */
  pathLabel: string
  /** What every path the store reads starts with, when the store sets that. */
  pathPrefix?: string
  /** The key, when a reference to the store holds one. */
  key?: { label: string; required: boolean }
}

/** For a store whose form Coral did not send: a path, and a key if there is one. */
export const UNKNOWN_REFERENCE_FORM: ReferenceFormView = {
  pathLabel: 'Path',
  key: { label: 'Key', required: false },
}

/** The view of the form a store sent, or the unknown one when it sent none. */
export function referenceFormView(form: SecretReferenceForm | undefined): ReferenceFormView {
  if (!form?.pathLabel) return UNKNOWN_REFERENCE_FORM
  return {
    pathLabel: form.pathLabel,
    ...(form.pathPrefix ? { pathPrefix: form.pathPrefix } : {}),
    ...(form.keyLabel ? { key: { label: form.keyLabel, required: form.keyRequired } } : {}),
  }
}

/** A saved reference in words, such as "secret store env, variable name MYCO_API_KEY". */
export function referenceDescription(
  reference: { store: string; path: string; key: string },
  form: ReferenceFormView = UNKNOWN_REFERENCE_FORM,
) {
  const key = reference.key ? `, ${(form.key?.label ?? 'Key').toLowerCase()} ${reference.key}` : ''
  return `secret store ${reference.store}, ${form.pathLabel.toLowerCase()} ${reference.path}${key}`
}

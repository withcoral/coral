// What Coral sends in each credential header or query parameter, and the
// Credential Fields they carry. Each edit keeps only the fields a binding still
// sends. A header may put text before the value; a query parameter may not.

import { nameFor, unusedFieldName } from './credential-names'
import type { ConfigurationValues, CredentialBindingValue } from './types'

export type CredentialBindings = {
  bindings: CredentialBindingValue[]
  credentialFields: ConfigurationValues['credentialFields']
}
type Fields = ConfigurationValues['credentialFields']

/** The label a name suggests: `x-api-key` becomes `Api key`. */
function labelFor(name: string) {
  const words = name
    .replace(/^x-/, '')
    .split(/[-_\s]+/)
    .filter(Boolean)
    .join(' ')
  return words.charAt(0).toUpperCase() + words.slice(1)
}

/** The bindings, with only the Credential Fields one of them still sends. */
function sent(bindings: CredentialBindingValue[], fields: Fields): CredentialBindings {
  const names = new Set(
    bindings.flatMap((binding) => (binding.source === 'field' ? [binding.field] : [])),
  )
  return {
    bindings,
    credentialFields: Object.fromEntries(
      Object.entries(fields).filter(([name]) => names.has(name)),
    ),
  }
}

/** The label of the credential a binding sends; empty for a fixed value. */
export function credentialLabel(values: CredentialBindings, binding: CredentialBindingValue) {
  return binding.source === 'field' ? (values.credentialFields[binding.field]?.label ?? '') : ''
}

/**
 * Edits what Coral sends. `origin` holds what was saved or read from the source.
 * Its names never change, because accounts keep their values under them, and a
 * binding that sent one of them sends it again when it is a credential once more.
 * `prefix` says whether a new credential binding carries text before the value,
 * which headers do and query parameters do not.
 */
export function bindingEditor(origin: CredentialBindings, { prefix = false } = {}) {
  const fixed = Object.keys(origin.credentialFields)

  const replaced = (
    values: CredentialBindings,
    index: number,
    binding: CredentialBindingValue,
    fields = values.credentialFields,
  ) =>
    sent(
      values.bindings.map((old, at) => (at === index ? binding : old)),
      fields,
    )

  /** Labels a credential. A new credential's name follows the label, in every binding that sends it. */
  const labelled = (
    values: CredentialBindings,
    from: string,
    label: string,
  ): CredentialBindings => {
    const fields = values.credentialFields
    const others = Object.keys(fields).filter((name) => name !== from)
    const to = fixed.includes(from) ? from : (nameFor(label, others) ?? from)
    const presentation = { ...(fields[from] ?? { description: '', sensitive: true }), label }
    return sent(
      values.bindings.map((binding) =>
        binding.source === 'field' && binding.field === from ? { ...binding, field: to } : binding,
      ),
      Object.fromEntries(
        Object.entries({ ...fields, [from]: presentation }).map(([name, field]) => [
          name === from ? to : name,
          field,
        ]),
      ),
    )
  }

  /** `values` with the binding at `index` sending a credential: the one it had, or a new one. */
  const sendingCredential = (values: CredentialBindings, index: number): CredentialBindings => {
    const { name } = values.bindings[index]
    const had = origin.bindings.find(
      (binding) => binding.source === 'field' && binding.name === name,
    )
    // Another binding may still send it; then its current label stays.
    if (had?.source === 'field')
      return replaced(values, index, had, {
        ...values.credentialFields,
        [had.field]: values.credentialFields[had.field] ?? origin.credentialFields[had.field],
      })
    const field = unusedFieldName(Object.keys(values.credentialFields))
    const added = replaced(
      values,
      index,
      { name, source: 'field', field, ...(prefix ? { prefix: '' } : {}) },
      { ...values.credentialFields, [field]: { label: '', description: '', sensitive: true } },
    )
    return name ? labelled(added, field, labelFor(name)) : added
  }

  return {
    /** Adds a binding that sends a new credential. */
    add(values: CredentialBindings) {
      const row = { name: '', source: 'literal', value: '' } as const
      return sendingCredential(
        { ...values, bindings: [...values.bindings, row] },
        values.bindings.length,
      )
    },
    /** Renames a binding. A label that still follows the name keeps following it. */
    setName(values: CredentialBindings, index: number, name: string) {
      const binding = values.bindings[index]
      const next = replaced(values, index, { ...binding, name })
      const label = credentialLabel(values, binding)
      return binding.source === 'field' && (label === '' || label === labelFor(binding.name))
        ? labelled(next, binding.field, labelFor(name))
        : next
    },
    /** Makes a binding send a credential or a fixed value. */
    setSource(values: CredentialBindings, index: number, source: CredentialBindingValue['source']) {
      const { name, source: current } = values.bindings[index]
      if (source === current) return values
      return source === 'literal'
        ? replaced(values, index, { name, source, value: '' })
        : sendingCredential(values, index)
    },
    setLabel(values: CredentialBindings, index: number, label: string) {
      const binding = values.bindings[index]
      return binding.source === 'field' ? labelled(values, binding.field, label) : values
    },
    /** Sets the text a credential header sends before the value, such as `Token token=`. */
    setPrefix(values: CredentialBindings, index: number, text: string) {
      const binding = values.bindings[index]
      return binding.source === 'field'
        ? replaced(values, index, { ...binding, prefix: text })
        : values
    },
    /** Sets the value a fixed binding sends. */
    setValue(values: CredentialBindings, index: number, value: string) {
      const binding = values.bindings[index]
      return binding.source === 'literal' ? replaced(values, index, { ...binding, value }) : values
    },
    remove(values: CredentialBindings, index: number) {
      return sent(
        values.bindings.filter((_binding, at) => at !== index),
        values.credentialFields,
      )
    },
  }
}

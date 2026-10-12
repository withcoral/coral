// The username and password Coral encodes as HTTP Basic, and the Credential
// Fields they carry. Each edit keeps only the fields a part still sends.

import { nameFor, unusedFieldName } from './credential-names'
import type { BasicAuthValue, ConfigurationValues, CredentialSourceValue } from './types'

export type BasicCredentials = Pick<ConfigurationValues, 'basic' | 'credentialFields'>
export type BasicPart = keyof BasicAuthValue
type Fields = ConfigurationValues['credentialFields']

/** Each part in the order Basic sends it, with the label a new credential starts from. */
export const BASIC_PARTS: [BasicPart, string][] = [
  ['username', 'Username'],
  ['password', 'Password'],
]

function sends(part: CredentialSourceValue, field: string) {
  return part.source === 'field' && part.field === field
}

/** The parts, with only the Credential Fields a part still sends. */
function sent(basic: BasicAuthValue, fields: Fields): BasicCredentials {
  return {
    basic,
    credentialFields: Object.fromEntries(
      Object.entries(fields).filter(
        ([name]) => sends(basic.username, name) || sends(basic.password, name),
      ),
    ),
  }
}

/** The label of the credential a part sends; empty for a fixed value. */
export function partLabel(values: BasicCredentials, part: BasicPart) {
  const source = values.basic[part]
  return source.source === 'field' ? (values.credentialFields[source.field]?.label ?? '') : ''
}

/**
 * Edits the username and password. `origin` holds what was saved or read from
 * the source. Its names never change, because accounts keep their values under
 * them, and a part that sent one of them sends it again when it is a credential
 * once more.
 */
export function basicCredentialEditor(origin: BasicCredentials) {
  const fixed = Object.keys(origin.credentialFields)

  const replaced = (
    values: BasicCredentials,
    part: BasicPart,
    source: CredentialSourceValue,
    fields = values.credentialFields,
  ) => sent({ ...values.basic, [part]: source }, fields)

  /** Labels a credential. A new credential's name follows the label, in both parts that send it. */
  const labelled = (values: BasicCredentials, from: string, label: string): BasicCredentials => {
    const fields = values.credentialFields
    const others = Object.keys(fields).filter((name) => name !== from)
    const to = fixed.includes(from) ? from : (nameFor(label, others) ?? from)
    const presentation = { ...(fields[from] ?? { description: '', sensitive: true }), label }
    const renamed = (source: CredentialSourceValue): CredentialSourceValue =>
      sends(source, from) ? { source: 'field', field: to } : source
    return sent(
      { username: renamed(values.basic.username), password: renamed(values.basic.password) },
      Object.fromEntries(
        Object.entries({ ...fields, [from]: presentation }).map(([name, field]) => [
          name === from ? to : name,
          field,
        ]),
      ),
    )
  }

  /** `values` with `part` sending a credential: the one it had, or a new one. */
  const sendingCredential = (values: BasicCredentials, part: BasicPart): BasicCredentials => {
    const had = origin.basic[part]
    // The other part may still send it; then its current label stays.
    if (had.source === 'field')
      return replaced(values, part, had, {
        ...values.credentialFields,
        [had.field]: values.credentialFields[had.field] ?? origin.credentialFields[had.field],
      })
    const field = unusedFieldName(Object.keys(values.credentialFields))
    const added = replaced(
      values,
      part,
      { source: 'field', field },
      { ...values.credentialFields, [field]: { label: '', description: '', sensitive: true } },
    )
    return labelled(added, field, BASIC_PARTS.find(([name]) => name === part)![1])
  }

  return {
    /** Makes a part send a credential or a fixed value. */
    setSource(values: BasicCredentials, part: BasicPart, source: CredentialSourceValue['source']) {
      if (source === values.basic[part].source) return values
      return source === 'literal'
        ? replaced(values, part, { source, value: '' })
        : sendingCredential(values, part)
    },
    setLabel(values: BasicCredentials, part: BasicPart, label: string) {
      const source = values.basic[part]
      return source.source === 'field' ? labelled(values, source.field, label) : values
    },
    /** Sets the value a fixed part sends, such as Mailgun's username `api`. */
    setValue(values: BasicCredentials, part: BasicPart, value: string) {
      const source = values.basic[part]
      return source.source === 'literal' ? replaced(values, part, { ...source, value }) : values
    },
  }
}

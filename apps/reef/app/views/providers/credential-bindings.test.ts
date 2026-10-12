import { describe, expect, it } from 'vitest'

import { bindingEditor, type CredentialBindings } from './credential-bindings'

const empty: CredentialBindings = { bindings: [], credentialFields: {} }
// Datadog's shape, as saved or filled in by its recipe.
const saved: CredentialBindings = {
  bindings: [
    { name: 'dd-api-key', source: 'field', field: 'api_key', prefix: '' },
    { name: 'dd-application-key', source: 'field', field: 'application_key', prefix: '' },
  ],
  credentialFields: {
    api_key: { label: 'API key', description: 'From Organization settings', sensitive: true },
    application_key: { label: 'Application key', description: '', sensitive: false },
  },
}

describe('a new credential', () => {
  const editor = bindingEditor(empty, { prefix: true })

  it('takes its name from its label, in every header that sends it', () => {
    let values = editor.add(empty)
    values = editor.setName(values, 0, 'x-api-key')
    values = editor.setLabel(values, 0, 'Secret token')
    expect(values).toEqual({
      bindings: [{ name: 'x-api-key', source: 'field', field: 'secret_token', prefix: '' }],
      credentialFields: {
        secret_token: { label: 'Secret token', description: '', sensitive: true },
      },
    })
    // A label that yields no name leaves the name as it was.
    expect(editor.setLabel(values, 0, 'Ключ').bindings[0]).toMatchObject({ field: 'secret_token' })
  })

  it('takes the label its header suggests until someone types their own', () => {
    let values = editor.setName(editor.add(empty), 0, 'x-api-key')
    expect(values.credentialFields).toEqual({
      api_key: { label: 'Api key', description: '', sensitive: true },
    })
    values = editor.setLabel(values, 0, 'Key')
    values = editor.setName(values, 0, 'authorization')
    expect(values.credentialFields).toEqual({
      key: { label: 'Key', description: '', sensitive: true },
    })
  })

  it('never takes the name of another credential', () => {
    let values = editor.add(editor.add(empty))
    values = editor.setLabel(values, 0, 'Key')
    values = editor.setLabel(values, 1, 'Key')
    expect(Object.keys(values.credentialFields)).toEqual(['key', 'key_2'])
    // After a removal, a new credential skips the name still in use.
    values = editor.add(editor.remove(editor.add(editor.add(empty)), 0))
    expect(values.bindings.map((binding) => binding.source === 'field' && binding.field)).toEqual([
      'field_2',
      'field_3',
    ])
  })
})

describe('a saved credential', () => {
  const editor = bindingEditor(saved, { prefix: true })

  it('keeps its name when its label changes', () => {
    const values = editor.setLabel(saved, 0, 'Datadog API key')
    expect(values.bindings[0]).toMatchObject({ field: 'api_key' })
    expect(values.credentialFields.api_key.label).toBe('Datadog API key')
  })

  it('comes back to a header that shares it with another header', () => {
    const shared: CredentialBindings = {
      ...saved,
      bindings: [
        ...saved.bindings,
        { name: 'authorization', source: 'field', field: 'api_key', prefix: 'Token token=' },
      ],
    }
    const sharing = bindingEditor(shared, { prefix: true })
    // The label edit on the other header is kept, not reset to the saved one.
    const relabelled = sharing.setLabel(sharing.setSource(shared, 2, 'literal'), 0, 'Key')
    expect(sharing.setSource(relabelled, 2, 'field')).toEqual({
      bindings: shared.bindings,
      credentialFields: {
        ...shared.credentialFields,
        api_key: { ...shared.credentialFields.api_key, label: 'Key' },
      },
    })
  })

  it('comes back unchanged when its header sends a fixed value, then a credential again', () => {
    const fixed = editor.setSource(saved, 1, 'literal')
    expect(Object.keys(fixed.credentialFields)).toEqual(['api_key'])
    expect(editor.setSource(fixed, 1, 'field')).toEqual(saved)
  })
})

describe('headers', () => {
  const editor = bindingEditor(saved, { prefix: true })

  it('drop the credentials no header sends any more', () => {
    expect(editor.remove(saved, 0).credentialFields).toEqual({
      application_key: saved.credentialFields.application_key,
    })
  })

  it('keep a credential another header still sends', () => {
    const shared = {
      ...saved,
      bindings: [
        ...saved.bindings,
        { name: 'authorization', source: 'field', field: 'api_key', prefix: 'Key ' } as const,
      ],
    }
    expect(Object.keys(editor.remove(shared, 0).credentialFields)).toEqual([
      'api_key',
      'application_key',
    ])
  })

  it('set a prefix only on a credential and a value only on a fixed header', () => {
    let values = editor.setSource(saved, 1, 'literal')
    values = editor.setValue(values, 1, '2024-01')
    values = editor.setPrefix(values, 0, 'Token token=')
    expect(editor.setValue(values, 0, 'ignored')).toBe(values)
    expect(editor.setPrefix(values, 1, 'ignored')).toBe(values)
    expect(values.bindings).toEqual([
      { name: 'dd-api-key', source: 'field', field: 'api_key', prefix: 'Token token=' },
      { name: 'dd-application-key', source: 'literal', value: '2024-01' },
    ])
  })
})

describe('query parameters', () => {
  // Trello's shape: an application key and a user token, neither with a prefix.
  const trello: CredentialBindings = {
    bindings: [
      { name: 'key', source: 'field', field: 'api_key' },
      { name: 'token', source: 'field', field: 'api_token' },
    ],
    credentialFields: {
      api_key: { label: 'Application API key', description: '', sensitive: true },
      api_token: { label: 'User API token', description: '', sensitive: true },
    },
  }
  const editor = bindingEditor(trello)

  it('sends no prefix, because a query parameter carries none', () => {
    const values = editor.add(trello)
    expect(values.bindings.at(-1)).toEqual({ name: '', source: 'field', field: 'field_3' })
    expect(editor.setPrefix(values, 2, 'Token token=').bindings.at(-1)).toEqual({
      name: '',
      source: 'field',
      field: 'field_3',
      prefix: 'Token token=',
    })
  })

  it('takes the label its parameter suggests until someone types their own', () => {
    let values = editor.setName(editor.add(trello), 2, 'api_key')
    expect(values.credentialFields.api_key_2.label).toBe('Api key')
    values = editor.setLabel(values, 2, 'Spare key')
    values = editor.setName(values, 2, 'spare')
    expect(values.credentialFields.spare_key.label).toBe('Spare key')
  })

  it('comes back with its label when the parameter sends it again', () => {
    const fixed = editor.setSource(trello, 0, 'literal')
    expect(fixed.credentialFields.api_key).toBeUndefined()
    const again = editor.setSource(fixed, 0, 'field')
    expect(again.bindings[0]).toEqual({ name: 'key', source: 'field', field: 'api_key' })
    expect(again.credentialFields.api_key.label).toBe('Application API key')
  })
})

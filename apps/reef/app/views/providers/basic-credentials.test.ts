import { describe, expect, it } from 'vitest'

import { basicCredentialEditor, partLabel, type BasicCredentials } from './basic-credentials'

const empty: BasicCredentials = {
  basic: { username: { source: 'literal', value: '' }, password: { source: 'literal', value: '' } },
  credentialFields: {},
}
// Jira's shape, as saved or filled in by its recipe.
const saved: BasicCredentials = {
  basic: {
    username: { source: 'field', field: 'email' },
    password: { source: 'field', field: 'api_token' },
  },
  credentialFields: {
    email: { label: 'Atlassian account email', description: '', sensitive: false },
    api_token: {
      label: 'API token',
      description: 'From Atlassian account settings',
      sensitive: true,
    },
  },
}

describe('a new credential', () => {
  const editor = basicCredentialEditor(empty)

  it('takes the label its part suggests until someone types their own', () => {
    let values = editor.setSource(empty, 'username', 'field')
    expect(values.credentialFields).toEqual({
      username: { label: 'Username', description: '', sensitive: true },
    })
    values = editor.setLabel(values, 'username', 'Account email')
    expect(values.basic.username).toEqual({ source: 'field', field: 'account_email' })
    expect(partLabel(values, 'username')).toBe('Account email')
  })

  it('never takes the name of the other part', () => {
    let values = editor.setSource(editor.setSource(empty, 'username', 'field'), 'password', 'field')
    values = editor.setLabel(values, 'username', 'Key')
    values = editor.setLabel(values, 'password', 'Key')
    expect(Object.keys(values.credentialFields)).toEqual(['key', 'key_2'])
  })

  it('keeps only what a part still sends', () => {
    const values = editor.setSource(
      editor.setSource(empty, 'password', 'field'),
      'password',
      'literal',
    )
    expect(values).toEqual(empty)
  })
})

describe('a saved credential', () => {
  const editor = basicCredentialEditor(saved)

  it('keeps its name when its label changes, so accounts keep their values', () => {
    const values = editor.setLabel(saved, 'password', 'Token')
    expect(values.basic.password).toEqual({ source: 'field', field: 'api_token' })
    expect(values.credentialFields.api_token).toEqual({
      label: 'Token',
      description: 'From Atlassian account settings',
      sensitive: true,
    })
  })

  it('comes back with its label when the part sends it again', () => {
    const fixed = editor.setSource(saved, 'username', 'literal')
    expect(fixed.credentialFields.email).toBeUndefined()
    expect(fixed.basic.username).toEqual({ source: 'literal', value: '' })
    const again = editor.setSource(fixed, 'username', 'field')
    expect(again.basic.username).toEqual({ source: 'field', field: 'email' })
    expect(partLabel(again, 'username')).toBe('Atlassian account email')
  })

  it('sends a fixed username, as Mailgun asks for api', () => {
    let values = editor.setSource(saved, 'username', 'literal')
    values = editor.setValue(values, 'username', 'api')
    expect(values.basic.username).toEqual({ source: 'literal', value: 'api' })
    expect(partLabel(values, 'username')).toBe('')
  })
})

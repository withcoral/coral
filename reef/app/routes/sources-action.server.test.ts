import { describe, expect, it } from 'vitest'

import type { Source, SourceInfo } from '@/generated/coral/v1/sources_pb'

import {
  editBindingsFromForm,
  firstMissingRequiredInput,
  installBindingsFromForm,
} from './sources-action.server'

describe('sources action form mapping', () => {
  it('maps install variable and secret inputs to source bindings', () => {
    const form = new FormData()
    form.set('var:region', ' us-east-1 ')
    form.set('sec:api_key', ' token ')

    expect(installBindingsFromForm(sourceInfo(), form)).toEqual([
      { key: 'region', secret: false, value: 'us-east-1' },
      { key: 'api_key', secret: true, value: 'token' },
    ])
  })

  it('reports the first missing required source-config secret', () => {
    const form = new FormData()
    form.set('var:region', 'us-east-1')

    expect(firstMissingRequiredInput(sourceInfo(), form)).toBe('api_key')
  })

  it('keeps existing edit variables and omits unchanged secrets', () => {
    const form = new FormData()
    form.set('var:region', 'eu-west-1')
    form.set('sec:api_key', '')

    expect(editBindingsFromForm(source(), sourceInfo(), form)).toEqual([
      { key: 'region', secret: false, value: 'eu-west-1' },
    ])
  })

  it('omits absent edit secret fields as unchanged', () => {
    const form = new FormData()
    form.set('var:region', 'eu-west-1')

    expect(editBindingsFromForm(source(), sourceInfo(), form)).toEqual([
      { key: 'region', secret: false, value: 'eu-west-1' },
    ])
  })

  it('falls back to installed bindings when source info is unavailable', () => {
    const form = new FormData()
    form.set('var:region', 'ap-south-1')
    form.set('sec:api_key', 'new-token')

    expect(editBindingsFromForm(source(), null, form)).toEqual([
      { key: 'region', secret: false, value: 'ap-south-1' },
      { key: 'api_key', secret: true, value: 'new-token' },
    ])
  })
})

function sourceInfo(): SourceInfo {
  return {
    inputs: [
      {
        hint: '',
        input: { case: 'variable', value: { defaultValue: 'us-west-2' } },
        key: 'region',
        required: true,
      },
      {
        hint: '',
        input: { case: 'secret', value: { credential: undefined } },
        key: 'api_key',
        required: true,
      },
    ],
  } as SourceInfo
}

function source(): Source {
  return {
    name: 'demo',
    secrets: [{ key: 'api_key', value: '' }],
    variables: [{ key: 'region', value: 'us-west-2' }],
    version: '1.0.0',
  } as Source
}

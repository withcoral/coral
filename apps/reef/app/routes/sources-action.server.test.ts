import { create } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'

import {
  OAuthCredentialMethodSchema,
  SourceCredentialMethodSchema,
  SourceCredentialSchema,
  SourceConfigCredentialMethodSchema,
  SourceInfoSchema,
  SourceInputSpecSchema,
  SourceOrigin,
  SourceSchema,
  SourceSecretInputSchema,
} from '@/generated/coral/v1/sources_pb'

import { editBindingsFromForm } from './sources-action'

const source = create(SourceSchema, {
  name: 'github',
  origin: SourceOrigin.BUNDLED,
  secrets: [{ key: 'token' }],
})

const oauthMethod = create(SourceCredentialMethodSchema, {
  label: 'OAuth',
  method: {
    case: 'oauth',
    value: create(OAuthCredentialMethodSchema, {
      redirectUri: 'http://127.0.0.1/callback',
    }),
  },
})

const sourceConfigMethod = create(SourceCredentialMethodSchema, {
  label: 'Paste token',
  method: {
    case: 'sourceConfig',
    value: create(SourceConfigCredentialMethodSchema),
  },
})

const sourceInfo = create(SourceInfoSchema, {
  inputs: [
    create(SourceInputSpecSchema, {
      key: 'token',
      input: {
        case: 'secret',
        value: create(SourceSecretInputSchema, {
          credential: create(SourceCredentialSchema, {
            methods: [oauthMethod, sourceConfigMethod],
          }),
        }),
      },
    }),
  ],
})

describe('editBindingsFromForm', () => {
  it('keeps legacy edit secret submissions when no credential method is posted', () => {
    const formData = new FormData()
    formData.set('sec:token', 'new-secret')

    expect(editBindingsFromForm(source, sourceInfo, formData)).toEqual([
      { key: 'token', secret: true, value: 'new-secret' },
    ])
  })

  it('does not map an OAuth-selected edit secret field to a source secret binding', () => {
    const formData = new FormData()
    formData.set('method:token', '0')
    formData.set('sec:token', 'should-not-submit')

    expect(editBindingsFromForm(source, sourceInfo, formData)).toEqual([])
  })

  it('still maps source-config-selected edit secrets to source secret bindings', () => {
    const formData = new FormData()
    formData.set('method:token', '1')
    formData.set('sec:token', 'new-secret')

    expect(editBindingsFromForm(source, sourceInfo, formData)).toEqual([
      { key: 'token', secret: true, value: 'new-secret' },
    ])
  })
})

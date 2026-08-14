import { create } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'

import {
  OAuthCredentialMethodSchema,
  OAuthCredentialClientIdSchema,
  OAuthCredentialClientSchema,
  OAuthCredentialClientSecretSchema,
  SourceCredentialMethodSchema,
  SourceCredentialSchema,
  SourceConfigCredentialMethodSchema,
  SourceInfoSchema,
  SourceInputSpecSchema,
  SourceOrigin,
  SourceSchema,
  SourceSecretInputSchema,
  SourceVariableInputSchema,
} from '@/generated/coral/v1/sources_pb'

import {
  editBindingsFromForm,
  firstMissingRequiredInput,
  installBindingsFromForm,
  oauthCredentialRetrievalsFromForm,
} from './source-install-form'

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

const oauthWithCredentialInputsMethod = create(SourceCredentialMethodSchema, {
  label: 'OAuth',
  method: {
    case: 'oauth',
    value: create(OAuthCredentialMethodSchema, {
      client: create(OAuthCredentialClientSchema, {
        id: create(OAuthCredentialClientIdSchema, {
          defaultValue: 'default-client-id',
          input: 'GITHUB_CLIENT_ID',
        }),
        secret: create(OAuthCredentialClientSecretSchema, {
          input: 'GITHUB_CLIENT_SECRET',
        }),
      }),
      redirectUri: 'http://127.0.0.1/callback',
    }),
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

const oauthSourceInfo = create(SourceInfoSchema, {
  inputs: [
    create(SourceInputSpecSchema, {
      input: {
        case: 'variable',
        value: create(SourceVariableInputSchema),
      },
      key: 'owner',
      required: true,
    }),
    create(SourceInputSpecSchema, {
      input: {
        case: 'secret',
        value: create(SourceSecretInputSchema, {
          credential: create(SourceCredentialSchema, {
            methods: [oauthWithCredentialInputsMethod, sourceConfigMethod],
          }),
        }),
      },
      key: 'GITHUB_TOKEN',
      required: true,
    }),
  ],
  name: 'github',
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

describe('installBindingsFromForm', () => {
  it('does not map OAuth credential inputs to source-visible secrets', () => {
    const formData = new FormData()
    formData.set('method:GITHUB_TOKEN', '0')
    formData.set('oauth:GITHUB_TOKEN:GITHUB_CLIENT_ID', 'custom-client-id')
    formData.set('oauth:GITHUB_TOKEN:GITHUB_CLIENT_SECRET', 'client-secret')
    formData.set('owner', 'ignored')
    formData.set('var:owner', 'coral')
    formData.set('sec:GITHUB_TOKEN', 'must-not-submit')

    expect(installBindingsFromForm(oauthSourceInfo, formData)).toEqual([
      { key: 'owner', secret: false, value: 'coral' },
    ])
  })

  it('does not let an out-of-range method index submit an OAuth secret directly', () => {
    const formData = new FormData()
    formData.set('method:GITHUB_TOKEN', '99')
    formData.set('sec:GITHUB_TOKEN', 'must-not-submit')

    expect(installBindingsFromForm(oauthSourceInfo, formData)).toEqual([])
    expect(oauthCredentialRetrievalsFromForm(oauthSourceInfo, formData)).toHaveLength(1)
  })
})

describe('oauthCredentialRetrievalsFromForm', () => {
  it('builds OAuth retrieval inputs without source secret bindings', () => {
    const formData = new FormData()
    formData.set('method:GITHUB_TOKEN', '0')
    formData.set('oauth:GITHUB_TOKEN:GITHUB_CLIENT_SECRET', 'client-secret')

    const retrievals = oauthCredentialRetrievalsFromForm(oauthSourceInfo, formData)

    expect(retrievals).toHaveLength(1)
    expect(retrievals[0]).toMatchObject({
      inputKey: 'GITHUB_TOKEN',
      methodIndex: 0,
      credentialInputs: [
        { key: 'GITHUB_CLIENT_ID', value: 'default-client-id' },
        { key: 'GITHUB_CLIENT_SECRET', value: 'client-secret' },
      ],
    })
  })
})

describe('firstMissingRequiredInput', () => {
  it('requires OAuth client inputs while accepting OAuth defaults', () => {
    const formData = new FormData()
    formData.set('var:owner', 'coral')
    formData.set('method:GITHUB_TOKEN', '0')

    expect(firstMissingRequiredInput(oauthSourceInfo, formData)).toBe('GITHUB_CLIENT_SECRET')

    formData.set('oauth:GITHUB_TOKEN:GITHUB_CLIENT_SECRET', 'client-secret')

    expect(firstMissingRequiredInput(oauthSourceInfo, formData)).toBeNull()
  })
})

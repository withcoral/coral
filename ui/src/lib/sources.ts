import { create } from '@bufbuild/protobuf'

import {
  CreateBundledSourceRequestSchema,
  CreateBundledSourceWithOAuthRequestSchema,
  DeleteSourceRequestSchema,
  DiscoverSourcesRequestSchema,
  GetSourceInfoRequestSchema,
  GetSourceRequestSchema,
  OAuthCredentialRetrievalSchema,
  SourceOrigin,
  type Source,
  type SourceCredentialMethod,
  type SourceInfo,
  type SourceInputSpec,
} from '@/generated/coral/v1/sources_pb'

import { sourceClient, WORKSPACE } from './coral-clients'

export type SourceOriginLabel = 'bundled' | 'imported' | 'unknown'

export interface CatalogEntry {
  name: string
  description: string
  version: string
  installed: boolean
  origin: SourceOriginLabel
}

export interface ResolvedSourceInfo {
  info: SourceInfoView
}

export interface SourceVariableView {
  key: string
  value: string
}

export interface SourceSecretView {
  key: string
}

export interface SourceView {
  name: string
  origin: SourceOriginLabel
  secrets: SourceSecretView[]
  variables: SourceVariableView[]
  version: string
}

export interface SourceInfoView extends CatalogEntry {
  inputs: SourceInputView[]
}

export type SourceInputView =
  | {
      hint: string
      input: { case: 'variable'; value: SourceVariableInputView }
      key: string
      required: boolean
    }
  | {
      hint: string
      input: { case: 'secret'; value: SourceSecretInputView }
      key: string
      required: boolean
    }
  | {
      hint: string
      input: { case: undefined; value?: undefined }
      key: string
      required: boolean
    }

export interface SourceVariableInputView {
  defaultValue: string
}

export interface SourceSecretInputView {
  credential?: SourceCredentialView
}

export interface SourceCredentialView {
  methods: SourceCredentialMethodView[]
}

export interface SourceCredentialMethodView {
  description: string
  hint: string
  label: string
  method:
    | { case: 'sourceConfig' }
    | { case: 'oauth'; value: OAuthCredentialMethodView }
    | { case: undefined; value?: undefined }
}

export interface OAuthCredentialMethodView {
  client?: {
    id?: {
      defaultValue: string
      input: string
    }
    secret?: {
      input: string
    }
  }
}

export interface OAuthCredentialRetrievalInput {
  credentialInputs: { key: string; value: string }[]
  inputKey: string
  methodIndex: number
}

export interface InstallInput {
  key: string
  value: string
  secret: boolean
}

export function originLabel(origin: SourceOrigin | SourceOriginLabel): SourceOriginLabel {
  if (typeof origin === 'string') return origin
  if (origin === SourceOrigin.BUNDLED) return 'bundled'
  if (origin === SourceOrigin.IMPORTED) return 'imported'
  return 'unknown'
}

function toCatalogEntry(s: SourceInfo): CatalogEntry {
  return {
    name: s.name,
    description: s.description,
    version: s.version,
    installed: s.installed,
    origin: originLabel(s.origin),
  }
}

function toSourceCredentialMethodView(method: SourceCredentialMethod): SourceCredentialMethodView {
  const base = {
    description: method.description,
    hint: method.hint,
    label: method.label,
  }

  if (method.method.case === 'sourceConfig') {
    return { ...base, method: { case: 'sourceConfig' } }
  }

  if (method.method.case === 'oauth') {
    return {
      ...base,
      method: {
        case: 'oauth',
        value: {
          client: method.method.value.client
            ? {
                id: method.method.value.client.id
                  ? {
                      defaultValue: method.method.value.client.id.defaultValue,
                      input: method.method.value.client.id.input,
                    }
                  : undefined,
                secret: method.method.value.client.secret
                  ? {
                      input: method.method.value.client.secret.input,
                    }
                  : undefined,
              }
            : undefined,
        },
      },
    }
  }

  return { ...base, method: { case: undefined } }
}

function toSourceInputView(input: SourceInputSpec): SourceInputView {
  const base = {
    hint: input.hint,
    key: input.key,
    required: input.required,
  }

  if (input.input.case === 'variable') {
    return {
      ...base,
      input: { case: 'variable', value: { defaultValue: input.input.value.defaultValue } },
    }
  }

  if (input.input.case === 'secret') {
    return {
      ...base,
      input: {
        case: 'secret',
        value: {
          credential: input.input.value.credential
            ? {
                methods: input.input.value.credential.methods.map(toSourceCredentialMethodView),
              }
            : undefined,
        },
      },
    }
  }

  return { ...base, input: { case: undefined } }
}

function toSourceInfoView(s: SourceInfo): SourceInfoView {
  return {
    ...toCatalogEntry(s),
    inputs: s.inputs.map(toSourceInputView),
  }
}

function toSourceView(source: Source): SourceView {
  return {
    name: source.name,
    origin: originLabel(source.origin),
    secrets: source.secrets.map((secret) => ({ key: secret.key })),
    variables: source.variables.map((variable) => ({
      key: variable.key,
      value: variable.value,
    })),
    version: source.version,
  }
}

export async function discoverBundled(): Promise<CatalogEntry[]> {
  const resp = await sourceClient.discoverSources(
    create(DiscoverSourcesRequestSchema, { workspace: WORKSPACE }),
  )
  return resp.sources.map(toCatalogEntry)
}

export async function getSourceInfo(name: string): Promise<ResolvedSourceInfo> {
  const resp = await sourceClient.getSourceInfo(
    create(GetSourceInfoRequestSchema, { workspace: WORKSPACE, name }),
  )
  if (!resp.sourceInfo) {
    throw new Error(`source '${name}' has no info`)
  }
  return { info: toSourceInfoView(resp.sourceInfo) }
}

export async function getInstalledSource(name: string): Promise<SourceView> {
  const resp = await sourceClient.getSource(
    create(GetSourceRequestSchema, { workspace: WORKSPACE, name }),
  )
  if (!resp.source) throw new Error(`source '${name}' not found`)
  return toSourceView(resp.source)
}

export async function deleteSource(name: string): Promise<void> {
  await sourceClient.deleteSource(create(DeleteSourceRequestSchema, { workspace: WORKSPACE, name }))
}

function splitBindings(inputs: InstallInput[]) {
  const variables = inputs.filter((i) => !i.secret).map((i) => ({ key: i.key, value: i.value }))
  const secrets = inputs.filter((i) => i.secret).map((i) => ({ key: i.key, value: i.value }))
  return { variables, secrets }
}

export async function createBundledSource(
  name: string,
  inputs: InstallInput[],
): Promise<SourceView> {
  const { variables, secrets } = splitBindings(inputs)
  const resp = await sourceClient.createBundledSource(
    create(CreateBundledSourceRequestSchema, {
      workspace: WORKSPACE,
      name,
      variables,
      secrets,
    }),
  )
  if (!resp.source) throw new Error(`createBundledSource returned no source`)
  return toSourceView(resp.source)
}

export interface OAuthFlowCallbacks {
  onAuthorization?: (event: {
    inputKey: string
    authorizationUrl: string
    expiresInSeconds: bigint
    userCode: string
    verificationUri: string
    verificationUriComplete: string
  }) => void
  onCompleted?: (event: { inputKey: string; metadata: Map<string, string> }) => void
}

/** Run the bundled-source OAuth install stream and deliver progress events. */
export async function createBundledSourceWithOAuth(
  name: string,
  inputs: InstallInput[],
  oauthRetrievals: OAuthCredentialRetrievalInput[],
  callbacks: OAuthFlowCallbacks = {},
): Promise<SourceView> {
  const { variables, secrets } = splitBindings(inputs)
  const stream = sourceClient.createBundledSourceWithOAuth(
    create(CreateBundledSourceWithOAuthRequestSchema, {
      workspace: WORKSPACE,
      name,
      variables,
      secrets,
      oauthCredentialRetrievals: oauthRetrievals.map((retrieval) =>
        create(OAuthCredentialRetrievalSchema, retrieval),
      ),
    }),
  )
  for await (const response of stream) {
    const event = response.event
    if (event.case === 'source') return toSourceView(event.value)
    if (event.case === 'oauthAuthorization') {
      callbacks.onAuthorization?.({
        inputKey: event.value.inputKey,
        authorizationUrl: event.value.authorizationUrl,
        expiresInSeconds: event.value.expiresInSeconds,
        userCode: event.value.userCode,
        verificationUri: event.value.verificationUri,
        verificationUriComplete: event.value.verificationUriComplete,
      })
      // Keep the device-code prompt visible if a fast backend streams the
      // completion event immediately after authorization starts.
      if (event.value.userCode) {
        await new Promise((resolve) => setTimeout(resolve, 1000))
      }
    } else if (event.case === 'oauthCompleted') {
      const metadata = new Map<string, string>()
      for (const item of event.value.metadata) metadata.set(item.key, item.value)
      callbacks.onCompleted?.({ inputKey: event.value.inputKey, metadata })
    }
  }
  throw new Error(`install stream ended without a source event`)
}

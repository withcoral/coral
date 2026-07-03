import { create } from '@bufbuild/protobuf'

import {
  CreateSourceRequestSchema,
  CreateSourceWithOAuthRequestSchema,
  DeleteSourceRequestSchema,
  DiscoverSourcesRequestSchema,
  GetSourceInfoRequestSchema,
  GetSourceRequestSchema,
  SourceOrigin,
  type OAuthCredentialRetrieval,
  type Source,
  type SourceInfo,
} from '@/generated/coral/v1/sources_pb'

import { sourceClient, WORKSPACE } from './coral-clients'

export type SourceOriginLabel = 'bundled' | 'imported' | 'global-spec' | 'unknown'

export interface CatalogEntry {
  name: string
  description: string
  version: string
  installed: boolean
  origin: SourceOriginLabel
}

export interface ResolvedSourceInfo {
  info: SourceInfo
}

export interface InstallInput {
  key: string
  value: string
  secret: boolean
}

export function originLabel(origin: SourceOrigin): SourceOriginLabel {
  if (origin === SourceOrigin.BUNDLED) return 'bundled'
  if (origin === SourceOrigin.IMPORTED) return 'imported'
  if (origin === SourceOrigin.GLOBAL_SPEC) return 'global-spec'
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
  return { info: resp.sourceInfo }
}

export async function getInstalledSource(name: string): Promise<Source> {
  const resp = await sourceClient.getSource(
    create(GetSourceRequestSchema, { workspace: WORKSPACE, name }),
  )
  if (!resp.source) throw new Error(`source '${name}' not found`)
  return resp.source
}

export async function deleteSource(name: string): Promise<void> {
  await sourceClient.deleteSource(create(DeleteSourceRequestSchema, { workspace: WORKSPACE, name }))
}

function splitBindings(inputs: InstallInput[]) {
  const variables = inputs
    .filter((i) => !i.secret)
    .map((i) => ({ key: i.key, value: i.value.trim() }))
  const secrets = inputs.filter((i) => i.secret).map((i) => ({ key: i.key, value: i.value.trim() }))
  return { variables, secrets }
}

export async function createSource(name: string, inputs: InstallInput[]): Promise<Source> {
  const { variables, secrets } = splitBindings(inputs)
  const resp = await sourceClient.createSource(
    create(CreateSourceRequestSchema, {
      workspace: WORKSPACE,
      name,
      variables,
      secrets,
    }),
  )
  if (!resp.source) throw new Error(`createSource returned no source`)
  return resp.source
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

/** Run the source OAuth install stream and deliver progress events. */
export async function createSourceWithOAuth(
  name: string,
  inputs: InstallInput[],
  oauthRetrievals: OAuthCredentialRetrieval[],
  callbacks: OAuthFlowCallbacks = {},
): Promise<Source> {
  const { variables, secrets } = splitBindings(inputs)
  const stream = sourceClient.createSourceWithOAuth(
    create(CreateSourceWithOAuthRequestSchema, {
      workspace: WORKSPACE,
      name,
      variables,
      secrets,
      oauthCredentialRetrievals: oauthRetrievals,
    }),
  )
  for await (const response of stream) {
    const event = response.event
    if (event.case === 'source') return event.value
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

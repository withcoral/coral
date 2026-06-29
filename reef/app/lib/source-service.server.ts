import { create } from '@bufbuild/protobuf'

import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'
import {
  CreateBundledSourceRequestSchema,
  CreateBundledSourceResponseSchema,
  DeleteSourceRequestSchema,
  DeleteSourceResponseSchema,
  DiscoverSourcesRequestSchema,
  DiscoverSourcesResponseSchema,
  GetSourceInfoRequestSchema,
  GetSourceInfoResponseSchema,
  GetSourceRequestSchema,
  GetSourceResponseSchema,
  ListSourcesRequestSchema,
  ListSourcesResponseSchema,
  SourceSecretSchema,
  SourceVariableSchema,
  type Source,
  type SourceInfo,
} from '@/generated/coral/v1/sources_pb'

import { grpcWebUnary } from './grpc-web'
import {
  SOURCE_SERVICE_PATH,
  catalogEntries,
  type CatalogEntry,
  type InstallInput,
  type ResolvedSourceInfo,
} from './source-data'

const DEFAULT_DEV_CORAL_ENDPOINT = 'http://127.0.0.1:1457'
const workspace = create(WorkspaceSchema, { name: 'default' })

export function sourceServiceForRequest(request: Request): SourceServiceResource {
  return new SourceServiceResource(coralEndpointForRequest(request))
}

export function coralEndpointForRequest(request: Request): string {
  const configured = process.env.CORAL_ENDPOINT?.trim()
  if (configured) return trimTrailingSlash(configured)

  // In production the backend endpoint must be configured explicitly. Deriving it
  // from the request origin would let an attacker-controlled Host header redirect
  // RPCs to a server they control, so refuse instead of falling back to the origin.
  if (process.env.NODE_ENV === 'production') {
    throw new Error('CORAL_ENDPOINT must be set in production')
  }

  const url = new URL(request.url)
  if (isLocalDevOrigin(url)) return DEFAULT_DEV_CORAL_ENDPOINT
  return url.origin
}

export class SourceServiceResource {
  constructor(private readonly endpoint: string) {}

  async listCatalog(): Promise<CatalogEntry[]> {
    const [discovered, installed] = await Promise.all([this.discoverSources(), this.listSources()])
    return catalogEntries(discovered, installed)
  }

  async getInstalledSource(name: string): Promise<Source> {
    const response = await grpcWebUnary({
      input: { name, workspace },
      inputSchema: GetSourceRequestSchema,
      outputSchema: GetSourceResponseSchema,
      path: this.path('GetSource'),
    })
    if (!response.source) throw new Error(`Source ${name} was not found`)
    return response.source
  }

  async getSourceInfo(name: string): Promise<ResolvedSourceInfo> {
    const response = await grpcWebUnary({
      input: { name, workspace },
      inputSchema: GetSourceInfoRequestSchema,
      outputSchema: GetSourceInfoResponseSchema,
      path: this.path('GetSourceInfo'),
    })
    if (!response.sourceInfo) throw new Error(`Source info for ${name} was not found`)
    return { info: response.sourceInfo }
  }

  async createBundledSource(name: string, bindings: InstallInput[]): Promise<Source> {
    const response = await grpcWebUnary({
      input: { name, workspace, ...bindingsToRequest(bindings) },
      inputSchema: CreateBundledSourceRequestSchema,
      outputSchema: CreateBundledSourceResponseSchema,
      path: this.path('CreateBundledSource'),
    })
    if (!response.source) throw new Error(`Coral did not return installed source ${name}`)
    return response.source
  }

  async deleteSource(name: string): Promise<void> {
    await grpcWebUnary({
      input: { name, workspace },
      inputSchema: DeleteSourceRequestSchema,
      outputSchema: DeleteSourceResponseSchema,
      path: this.path('DeleteSource'),
    })
  }

  private async discoverSources(): Promise<SourceInfo[]> {
    const response = await grpcWebUnary({
      input: { workspace },
      inputSchema: DiscoverSourcesRequestSchema,
      outputSchema: DiscoverSourcesResponseSchema,
      path: this.path('DiscoverSources'),
    })
    return response.sources
  }

  private async listSources(): Promise<Source[]> {
    const response = await grpcWebUnary({
      input: { workspace },
      inputSchema: ListSourcesRequestSchema,
      outputSchema: ListSourcesResponseSchema,
      path: this.path('ListSources'),
    })
    return response.sources
  }

  private path(method: string): string {
    return new URL(`${SOURCE_SERVICE_PATH}/${method}`, `${this.endpoint}/`).toString()
  }
}

function bindingsToRequest(bindings: InstallInput[]) {
  return {
    secrets: bindings
      .filter((binding) => binding.secret)
      .map((binding) => create(SourceSecretSchema, { key: binding.key, value: binding.value })),
    variables: bindings
      .filter((binding) => !binding.secret)
      .map((binding) => create(SourceVariableSchema, { key: binding.key, value: binding.value })),
  }
}

function isLocalDevOrigin(url: URL): boolean {
  return (url.hostname === 'localhost' || url.hostname === '127.0.0.1') && url.port !== '1457'
}

function trimTrailingSlash(value: string): string {
  return value.endsWith('/') ? value.slice(0, -1) : value
}

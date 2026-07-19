import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/source-oauth-install'

import {
  CreateBundledSourceWithOAuthRequestSchema,
  GetSourceInfoRequestSchema,
  type CreateBundledSourceWithOAuthResponse,
  type SourceInfo,
} from '@/generated/coral/v1/sources_pb'
import type { Workspace } from '@/generated/coral/v1/resources_pb'
import { sourceClientForRequest } from '@/lib/coral-request.server'
import {
  firstMissingRequiredInput,
  firstOAuthMethodInput,
  formValue,
  installBindingsFromForm,
  oauthCredentialRetrievalsFromForm,
  splitInstallBindings,
} from '@/lib/source-install-form'
import { originLabel } from '@/lib/sources'
import {
  oauthInstallEventToNdjson,
  type OAuthInstallStreamEvent,
} from '@/lib/source-oauth-install-stream'
import { errorMessage } from '@/lib/utils'
import { workspaceFromParams } from '@/lib/workspace-routing'

const NDJSON_HEADERS = {
  'Cache-Control': 'no-store',
  'Content-Type': 'application/x-ndjson; charset=utf-8',
} as const

// Resource route: normal source CRUD stays in React Router loaders/actions, but
// interactive OAuth/device-code installs need browser-visible server-streaming
// progress. The browser fetches this same-origin endpoint; it never imports or
// calls Coral's gRPC-Web client directly.
export async function action({ params, request }: Route.ActionArgs): Promise<Response> {
  const formData = await request.formData()
  let name: string
  try {
    const resolvedName = resolveSourceName(params.sourceName, formData)
    if (!resolvedName) return ndjsonErrorResponse('Missing source name', 400)
    name = resolvedName
  } catch (error) {
    return ndjsonErrorResponse(errorMessage(error), 400)
  }

  const sourceClient = sourceClientForRequest(request)
  try {
    const workspace = workspaceFromParams(params)
    const info = await getSourceInfo(sourceClient, name, workspace)
    if (info.installed && originLabel(info.origin) !== 'bundled') {
      return ndjsonErrorResponse("Imported sources can't be installed here yet", 400)
    }

    if (!firstOAuthMethodInput(info, formData)) {
      return ndjsonErrorResponse(
        'Selected credential method is not OAuth; use the normal install action.',
        400,
      )
    }

    const missing = firstMissingRequiredInput(info, formData)
    if (missing) return ndjsonErrorResponse(`${missing} is required`, 400)

    const oauthCredentialRetrievals = oauthCredentialRetrievalsFromForm(info, formData)
    if (oauthCredentialRetrievals.length === 0) {
      return ndjsonErrorResponse(
        'No OAuth credential retrieval was selected; use the normal install action.',
        400,
      )
    }

    const { secrets, variables } = splitInstallBindings(installBindingsFromForm(info, formData))
    const stream = sourceClient.createBundledSourceWithOAuth(
      create(CreateBundledSourceWithOAuthRequestSchema, {
        name,
        oauthCredentialRetrievals,
        secrets,
        variables,
        workspace,
      }),
      { signal: request.signal },
    )
    return oauthInstallStreamResponse(stream, request.signal)
  } catch (error) {
    return ndjsonErrorResponse(errorMessage(error), 500)
  }
}

export function oauthInstallStreamResponse(
  responses: AsyncIterable<CreateBundledSourceWithOAuthResponse>,
  signal?: AbortSignal,
): Response {
  const encoder = new TextEncoder()
  let closed = false

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const send = (event: OAuthInstallStreamEvent) => {
        if (closed || signal?.aborted) return
        controller.enqueue(encoder.encode(oauthInstallEventToNdjson(event)))
      }

      try {
        await relayOAuthInstallStreamEvents(responses, send, signal)
      } catch (error) {
        if (!signal?.aborted) send({ type: 'error', message: errorMessage(error) })
      } finally {
        if (!closed) {
          closed = true
          if (!signal?.aborted) controller.close()
        }
      }
    },
    cancel() {
      closed = true
    },
  })

  return new Response(stream, { headers: NDJSON_HEADERS })
}

export async function relayOAuthInstallStreamEvents(
  responses: AsyncIterable<CreateBundledSourceWithOAuthResponse>,
  send: (event: OAuthInstallStreamEvent) => void,
  signal?: AbortSignal,
): Promise<void> {
  for await (const response of responses) {
    if (signal?.aborted) return
    const event = response.event
    switch (event.case) {
      case 'oauthAuthorization':
        send({
          type: 'oauthAuthorization',
          authorizationUrl: event.value.authorizationUrl,
          expiresInSeconds: event.value.expiresInSeconds.toString(),
          inputKey: event.value.inputKey,
          userCode: event.value.userCode,
          verificationUri: event.value.verificationUri,
          verificationUriComplete: event.value.verificationUriComplete,
        })
        break
      case 'oauthCallbackReceived':
        send({ type: 'oauthCallbackReceived', inputKey: event.value.inputKey })
        break
      case 'oauthCompleted':
        send({
          type: 'oauthCompleted',
          inputKey: event.value.inputKey,
          metadata: event.value.metadata.map((item) => ({ key: item.key, value: item.value })),
        })
        break
      case 'source':
        send({ type: 'source', name: event.value.name, version: event.value.version })
        return
      case undefined:
        throw new Error('OAuth install stream included an empty event')
      default: {
        const exhaustive: never = event
        return exhaustive
      }
    }
  }
  throw new Error('OAuth install stream ended without a source event')
}

function resolveSourceName(paramName: string | undefined, formData: FormData): string | null {
  const formName = formValue(formData, 'name')
  const param = paramName?.trim() ?? ''
  if (param && formName && param !== formName) {
    throw new Error(`Source name mismatch: route has ${param}, form has ${formName}`)
  }
  return param || formName || null
}

async function getSourceInfo(
  sourceClient: ReturnType<typeof sourceClientForRequest>,
  name: string,
  workspace: Workspace,
): Promise<SourceInfo> {
  const response = await sourceClient.getSourceInfo(
    create(GetSourceInfoRequestSchema, { name, workspace }),
  )
  if (!response.sourceInfo) throw new Error(`Source info for ${name} was not found`)
  return response.sourceInfo
}

function ndjsonErrorResponse(message: string, status: number): Response {
  return new Response(oauthInstallEventToNdjson({ type: 'error', message }), {
    headers: NDJSON_HEADERS,
    status,
  })
}

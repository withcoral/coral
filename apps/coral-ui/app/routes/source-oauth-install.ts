import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/source-oauth-install'

import { requestAuthContext } from '@/auth/server-context'
import {
  CreateBundledSourceWithOAuthRequestSchema,
  GetSourceInfoRequestSchema,
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
  oauthSourceStreamResponse,
  oauthStreamErrorResponse,
} from '@/lib/source-oauth-response.server'
import { errorMessage } from '@/lib/utils'
import { workspaceFromParams } from '@/lib/workspace-routing'

// Resource route: normal source CRUD stays in React Router loaders/actions, but
// interactive OAuth/device-code installs need browser-visible server-streaming
// progress. The browser fetches this same-origin endpoint; it never imports or
// calls Coral's gRPC client directly.
export async function action({ context, params, request }: Route.ActionArgs): Promise<Response> {
  const formData = await request.formData()
  let name: string
  try {
    const resolvedName = resolveSourceName(params.sourceName, formData)
    if (!resolvedName) return oauthStreamErrorResponse('Missing source name', 400)
    name = resolvedName
  } catch (error) {
    return oauthStreamErrorResponse(errorMessage(error), 400)
  }

  const sourceClient = sourceClientForRequest(request, context.get(requestAuthContext).accessToken)
  try {
    const workspace = workspaceFromParams(params)
    const info = await getSourceInfo(sourceClient, name, workspace)
    if (info.installed && originLabel(info.origin) !== 'bundled') {
      return oauthStreamErrorResponse("Imported sources can't be installed here yet", 400)
    }

    if (!firstOAuthMethodInput(info, formData)) {
      return oauthStreamErrorResponse(
        'Selected credential method is not OAuth; use the normal install action.',
        400,
      )
    }

    const missing = firstMissingRequiredInput(info, formData)
    if (missing) return oauthStreamErrorResponse(`${missing} is required`, 400)

    const oauthCredentialRetrievals = oauthCredentialRetrievalsFromForm(info, formData)
    if (oauthCredentialRetrievals.length === 0) {
      return oauthStreamErrorResponse(
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
    return await oauthSourceStreamResponse(stream, request.signal)
  } catch (error) {
    return oauthStreamErrorResponse(errorMessage(error), 500)
  }
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

import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/source-oauth-import'

import { requestAuthContext } from '@/auth/server-context'
import {
  DescribeSourceManifestRequestSchema,
  ImportSourceRequestSchema,
} from '@/generated/coral/v1/sources_pb'
import { sourceClientForRequest } from '@/lib/coral-request.server'
import {
  oauthSourceStreamResponse,
  oauthStreamErrorResponse,
} from '@/lib/source-oauth-response.server'
import {
  installBindingsFromForm,
  oauthCredentialRetrievalsFromForm,
  splitInstallBindings,
} from '@/lib/source-install-form'
import { errorMessage } from '@/lib/utils'
import { workspaceFromParams } from '@/lib/workspace-routing'

export async function action({ context, params, request }: Route.ActionArgs): Promise<Response> {
  const formData = await request.formData()
  const manifestYaml = formData.get('manifest_yaml')

  if (typeof manifestYaml !== 'string' || !manifestYaml.trim()) {
    return oauthStreamErrorResponse('Missing source manifest', 400)
  }

  const workspace = workspaceFromParams(params)
  const sourceClient = sourceClientForRequest(request, context.get(requestAuthContext).accessToken)

  try {
    // The manifest is the only description of its own inputs, so ask Coral what it
    // declares before mapping the submitted form onto it.
    const described = await sourceClient.describeSourceManifest(
      create(DescribeSourceManifestRequestSchema, { manifestYaml, workspace }),
      { signal: request.signal },
    )
    const info = described.sourceInfo
    if (!info) return oauthStreamErrorResponse('Coral did not describe the source manifest', 502)

    const oauthCredentialRetrievals = oauthCredentialRetrievalsFromForm(info, formData)
    if (oauthCredentialRetrievals.length === 0) {
      return oauthStreamErrorResponse(
        'No OAuth credential retrieval was selected; use the normal import action.',
        400,
      )
    }

    const { secrets, variables } = splitInstallBindings(installBindingsFromForm(info, formData))
    const stream = sourceClient.importSource(
      create(ImportSourceRequestSchema, {
        manifestYaml,
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

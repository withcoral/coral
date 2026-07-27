import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/source-oauth-import'

import {
  ImportSourceRequestSchema,
  OAuthCredentialRetrievalSchema,
} from '@/generated/coral/v1/sources_pb'
import { sourceClientForRequest } from '@/lib/coral-request.server'
import {
  oauthSourceStreamResponse,
  oauthStreamErrorResponse,
} from '@/lib/source-oauth-response.server'
import { formValue } from '@/lib/source-install-form'
import { errorMessage } from '@/lib/utils'
import { workspaceFromParams } from '@/lib/workspace-routing'

export async function action({ params, request }: Route.ActionArgs): Promise<Response> {
  const formData = await request.formData()
  const manifestYaml = formData.get('manifest_yaml')
  const inputKey = formValue(formData, 'oauth_input_key')
  const methodIndex = Number(formValue(formData, 'oauth_method_index'))

  if (typeof manifestYaml !== 'string' || !manifestYaml.trim()) {
    return oauthStreamErrorResponse('Missing source manifest', 400)
  }
  if (!inputKey) return oauthStreamErrorResponse('Missing OAuth source input key', 400)
  if (!Number.isInteger(methodIndex) || methodIndex < 0) {
    return oauthStreamErrorResponse('Invalid OAuth credential method index', 400)
  }

  try {
    const stream = sourceClientForRequest(request).importSource(
      create(ImportSourceRequestSchema, {
        manifestYaml,
        oauthCredentialRetrievals: [
          create(OAuthCredentialRetrievalSchema, {
            inputKey,
            methodIndex,
          }),
        ],
        workspace: workspaceFromParams(params),
      }),
      { signal: request.signal },
    )
    return oauthSourceStreamResponse(stream, request.signal)
  } catch (error) {
    return oauthStreamErrorResponse(errorMessage(error), 500)
  }
}

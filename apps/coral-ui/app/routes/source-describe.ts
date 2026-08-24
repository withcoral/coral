import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/source-describe'

import { requestAuthContext } from '@/auth/server-context'
import { DescribeSourceManifestRequestSchema } from '@/generated/coral/v1/sources_pb'
import { sourceClientForRequest } from '@/lib/coral-request.server'
import type { SourceDescribeData } from '@/lib/source-describe'
import { toCatalogEntry } from '@/lib/sources'
import { errorMessage } from '@/lib/utils'
import { workspaceFromParams } from '@/lib/workspace-routing'

export async function action({
  context,
  params,
  request,
}: Route.ActionArgs): Promise<SourceDescribeData> {
  const formData = await request.formData()
  const manifestYaml = formData.get('manifest_yaml')
  if (typeof manifestYaml !== 'string' || !manifestYaml.trim()) {
    return { message: 'Paste a source manifest', status: 'error' }
  }

  try {
    const response = await sourceClientForRequest(
      request,
      context.get(requestAuthContext).accessToken,
    ).describeSourceManifest(
      create(DescribeSourceManifestRequestSchema, {
        manifestYaml,
        workspace: workspaceFromParams(params),
      }),
      { signal: request.signal },
    )
    if (!response.sourceInfo) {
      return { message: 'Coral did not describe the source manifest', status: 'error' }
    }
    return { entry: toCatalogEntry(response.sourceInfo), status: 'success' }
  } catch (error) {
    return { message: errorMessage(error), status: 'error' }
  }
}

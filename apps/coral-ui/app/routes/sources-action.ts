import { create } from '@bufbuild/protobuf'
import { redirect, type RouterContextProvider } from 'react-router'

import { requestAuthContext } from '@/auth/server-context'
import {
  CreateBundledSourceRequestSchema,
  DeleteSourceRequestSchema,
  DescribeSourceManifestRequestSchema,
  GetSourceInfoRequestSchema,
  GetSourceRequestSchema,
  ImportSourceRequestSchema,
} from '@/generated/coral/v1/sources_pb'
import type { Workspace } from '@/generated/coral/v1/resources_pb'
import { sourceClientForRequest } from '@/lib/coral-request.server'
import {
  editBindingsFromForm,
  firstMissingRequiredInput,
  firstOAuthMethodInput,
  formValue,
  installBindingsFromForm,
  splitInstallBindings,
  type InstallInput,
} from '@/lib/source-install-form'
import { originLabel } from '@/lib/sources'
import { errorMessage } from '@/lib/utils'
import { workspaceFromParams } from '@/lib/workspace-routing'
import { routePath } from '@/routing/routemap'

export {
  editBindingsFromForm,
  firstMissingRequiredInput,
  firstOAuthMethodInput,
  installBindingsFromForm,
  oauthCredentialRetrievalsFromForm,
  type InstallInput,
} from '@/lib/source-install-form'

export type SourceActionIntent = 'delete' | 'edit' | 'import' | 'install'

export type SourcesActionData =
  | {
      intent: SourceActionIntent
      message: string
      name: string
      status: 'error'
    }
  | {
      intent: SourceActionIntent
      name: string
      status: 'success'
    }
  | undefined

interface SourcesActionArgs {
  context: Readonly<RouterContextProvider>
  params: { workspaceId?: string }
  request: Request
}

export async function action({
  context,
  params,
  request,
}: SourcesActionArgs): Promise<SourcesActionData | Response> {
  const workspace = workspaceFromParams(params)
  const result = await runSourcesAction(
    request,
    workspace,
    context.get(requestAuthContext).accessToken,
  )
  return result.status === 'success'
    ? redirect(routePath('workspaceSources', { workspaceId: workspace.name }))
    : result
}

type SourceActionResult = Exclude<SourcesActionData, undefined>

export async function runSourcesAction(
  request: Request,
  workspace: Workspace,
  accessToken: string | null,
): Promise<SourceActionResult> {
  const formData = await request.formData()
  const intent = formValue(formData, '_intent')
  const name = formValue(formData, 'name')
  if (!name) return actionError('install', '', 'Missing source name')

  const sourceClient = sourceClientForRequest(request, accessToken)
  try {
    if (intent === 'install') {
      const info = await getSourceInfo(sourceClient, workspace, name)
      if (info.installed && originLabel(info.origin) !== 'bundled') {
        return actionError('install', name, "Imported sources can't be installed here yet")
      }
      if (firstOAuthMethodInput(info, formData)) {
        return actionError('install', name, 'OAuth install is not available in this shell yet')
      }
      const missing = firstMissingRequiredInput(info, formData)
      if (missing) return actionError('install', name, `${missing} is required`)
      await createBundledSource(
        sourceClient,
        workspace,
        name,
        installBindingsFromForm(info, formData),
      )
      return actionSuccess('install', name)
    }
    if (intent === 'edit') {
      const source = await getInstalledSource(sourceClient, workspace, name)
      if (originLabel(source.origin) !== 'bundled') {
        return actionError('edit', name, "Imported sources can't be edited here yet")
      }
      const info = await getSourceInfo(sourceClient, workspace, name).catch(() => null)
      await createBundledSource(
        sourceClient,
        workspace,
        name,
        editBindingsFromForm(source, info, formData),
      )
      return actionSuccess('edit', name)
    }
    if (intent === 'delete') {
      await sourceClient.deleteSource(create(DeleteSourceRequestSchema, { name, workspace }))
      return actionSuccess('delete', name)
    }
    if (intent === 'import') {
      const manifestYaml = formData.get('manifest_yaml')
      if (typeof manifestYaml !== 'string' || manifestYaml.trim().length === 0) {
        return actionError('import', name, 'Missing source manifest')
      }
      const info = await describeSourceManifest(sourceClient, workspace, manifestYaml)
      const missing = firstMissingRequiredInput(info, formData)
      if (missing) return actionError('import', name, `${missing} is required`)
      await importSourceManifest(
        sourceClient,
        workspace,
        manifestYaml,
        installBindingsFromForm(info, formData),
      )
      return actionSuccess('import', name)
    }
    return actionError('install', name, 'Unknown source action')
  } catch (error) {
    return actionError(
      intent === 'edit' || intent === 'delete' || intent === 'import' ? intent : 'install',
      name,
      errorMessage(error),
    )
  }
}

async function getSourceInfo(
  sourceClient: ReturnType<typeof sourceClientForRequest>,
  workspace: Workspace,
  name: string,
) {
  const response = await sourceClient.getSourceInfo(
    create(GetSourceInfoRequestSchema, { name, workspace }),
  )
  if (!response.sourceInfo) throw new Error(`Source info for ${name} was not found`)
  return response.sourceInfo
}

async function getInstalledSource(
  sourceClient: ReturnType<typeof sourceClientForRequest>,
  workspace: Workspace,
  name: string,
) {
  const response = await sourceClient.getSource(create(GetSourceRequestSchema, { name, workspace }))
  if (!response.source) throw new Error(`Source ${name} was not found`)
  return response.source
}

async function createBundledSource(
  sourceClient: ReturnType<typeof sourceClientForRequest>,
  workspace: Workspace,
  name: string,
  bindings: InstallInput[],
) {
  const { secrets, variables } = splitInstallBindings(bindings)
  const response = await sourceClient.createBundledSource(
    create(CreateBundledSourceRequestSchema, {
      name,
      workspace,
      variables,
      secrets,
    }),
  )
  if (!response.source) throw new Error(`Coral did not return installed source ${name}`)
  return response.source
}

async function describeSourceManifest(
  sourceClient: ReturnType<typeof sourceClientForRequest>,
  workspace: Workspace,
  manifestYaml: string,
) {
  const response = await sourceClient.describeSourceManifest(
    create(DescribeSourceManifestRequestSchema, { manifestYaml, workspace }),
  )
  if (!response.sourceInfo) throw new Error('Coral did not describe the source manifest')
  return response.sourceInfo
}

async function importSourceManifest(
  sourceClient: ReturnType<typeof sourceClientForRequest>,
  workspace: Workspace,
  manifestYaml: string,
  bindings: InstallInput[],
) {
  const { secrets, variables } = splitInstallBindings(bindings)
  const stream = sourceClient.importSource(
    create(ImportSourceRequestSchema, { manifestYaml, secrets, variables, workspace }),
  )
  // Callers route OAuth manifests through the streaming resource route, so the
  // stream here only carries the terminal source event.
  for await (const response of stream) {
    if (response.event.case === 'source') return response.event.value
  }
  throw new Error('Coral did not return the imported source')
}

function actionError(
  intent: SourceActionIntent,
  name: string,
  message: string,
): SourceActionResult {
  return { intent, message, name, status: 'error' }
}

function actionSuccess(intent: SourceActionIntent, name: string): SourceActionResult {
  return { intent, name, status: 'success' }
}

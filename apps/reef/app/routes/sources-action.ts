import { create } from '@bufbuild/protobuf'
import { redirect } from 'react-router'

import {
  CreateBundledSourceRequestSchema,
  DeleteSourceRequestSchema,
  GetSourceInfoRequestSchema,
  GetSourceRequestSchema,
} from '@/generated/coral/v1/sources_pb'
import { sourceClientForRequest } from '@/lib/coral-request.server'
import { WORKSPACE } from '@/lib/constants'
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

export {
  editBindingsFromForm,
  firstMissingRequiredInput,
  firstOAuthMethodInput,
  installBindingsFromForm,
  oauthCredentialRetrievalsFromForm,
  type InstallInput,
} from '@/lib/source-install-form'

export type SourceActionIntent = 'delete' | 'edit' | 'install'

export type SourcesActionData =
  | {
      intent: SourceActionIntent
      message: string
      name: string
      status: 'error'
    }
  | undefined

export async function action({
  request,
}: {
  request: Request
}): Promise<SourcesActionData | Response> {
  const formData = await request.formData()
  const intent = formValue(formData, '_intent')
  const name = formValue(formData, 'name')
  if (!name) return actionError('install', '', 'Missing source name')

  const sourceClient = sourceClientForRequest(request)
  try {
    if (intent === 'install') {
      const info = await getSourceInfo(sourceClient, name)
      if (info.installed && originLabel(info.origin) !== 'bundled') {
        return actionError('install', name, "Imported sources can't be installed here yet")
      }
      if (firstOAuthMethodInput(info, formData)) {
        return actionError('install', name, 'OAuth install is not available in this shell yet')
      }
      const missing = firstMissingRequiredInput(info, formData)
      if (missing) return actionError('install', name, `${missing} is required`)
      await createBundledSource(sourceClient, name, installBindingsFromForm(info, formData))
      return redirect('/sources')
    }
    if (intent === 'edit') {
      const source = await getInstalledSource(sourceClient, name)
      if (originLabel(source.origin) !== 'bundled') {
        return actionError('edit', name, "Imported sources can't be edited here yet")
      }
      const info = await getSourceInfo(sourceClient, name).catch(() => null)
      await createBundledSource(sourceClient, name, editBindingsFromForm(source, info, formData))
      return redirect('/sources')
    }
    if (intent === 'delete') {
      await sourceClient.deleteSource(
        create(DeleteSourceRequestSchema, { name, workspace: WORKSPACE }),
      )
      return redirect('/sources')
    }
    return actionError('install', name, 'Unknown source action')
  } catch (error) {
    return actionError(
      intent === 'edit' || intent === 'delete' ? intent : 'install',
      name,
      errorMessage(error),
    )
  }
}

async function getSourceInfo(
  sourceClient: ReturnType<typeof sourceClientForRequest>,
  name: string,
) {
  const response = await sourceClient.getSourceInfo(
    create(GetSourceInfoRequestSchema, { name, workspace: WORKSPACE }),
  )
  if (!response.sourceInfo) throw new Error(`Source info for ${name} was not found`)
  return response.sourceInfo
}

async function getInstalledSource(
  sourceClient: ReturnType<typeof sourceClientForRequest>,
  name: string,
) {
  const response = await sourceClient.getSource(
    create(GetSourceRequestSchema, { name, workspace: WORKSPACE }),
  )
  if (!response.source) throw new Error(`Source ${name} was not found`)
  return response.source
}

async function createBundledSource(
  sourceClient: ReturnType<typeof sourceClientForRequest>,
  name: string,
  bindings: InstallInput[],
) {
  const { secrets, variables } = splitInstallBindings(bindings)
  const response = await sourceClient.createBundledSource(
    create(CreateBundledSourceRequestSchema, {
      name,
      workspace: WORKSPACE,
      variables,
      secrets,
    }),
  )
  if (!response.source) throw new Error(`Coral did not return installed source ${name}`)
  return response.source
}

function actionError(intent: SourceActionIntent, name: string, message: string): SourcesActionData {
  return { intent, message, name, status: 'error' }
}

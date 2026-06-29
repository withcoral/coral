import { create } from '@bufbuild/protobuf'
import { redirect } from 'react-router'

import {
  CreateBundledSourceRequestSchema,
  DeleteSourceRequestSchema,
  GetSourceInfoRequestSchema,
  GetSourceRequestSchema,
  type Source,
  type SourceInfo,
  type SourceInputSpec,
} from '@/generated/coral/v1/sources_pb'
import { sourceClientForRequest } from '@/lib/coral-request.server'
import { WORKSPACE } from '@/lib/constants'
import { type InstallInput, originLabel } from '@/lib/sources'
import { errorMessage } from '@/lib/utils'

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

export function installBindingsFromForm(info: SourceInfo, formData: FormData): InstallInput[] {
  const bindings: InstallInput[] = []
  for (const input of info.inputs) {
    if (input.input.case === 'variable') {
      const value = formValue(formData, `var:${input.key}`, input.input.value.defaultValue)
      if (value.length > 0) bindings.push({ key: input.key, secret: false, value })
      continue
    }
    if (input.input.case !== 'secret') continue
    const methodIndex = Number(formValue(formData, `method:${input.key}`, '0'))
    const method = input.input.value.credential?.methods[methodIndex]
    if (method?.method.case === 'oauth') continue
    const value = formValue(formData, `sec:${input.key}`)
    if (value.length > 0) bindings.push({ key: input.key, secret: true, value })
  }
  return bindings
}

export function editBindingsFromForm(
  source: Source,
  info: SourceInfo | null,
  formData: FormData,
): InstallInput[] {
  if (!info) return bindingsFromInstalledSource(source, formData)
  const bindings: InstallInput[] = []
  const variables = new Map(source.variables.map((variable) => [variable.key, variable.value]))
  for (const input of info.inputs) {
    if (input.input.case === 'variable') {
      const existingValue = variables.get(input.key) ?? input.input.value.defaultValue
      const value = formValue(formData, `var:${input.key}`, existingValue)
      if (value.length > 0) bindings.push({ key: input.key, secret: false, value })
      continue
    }
    if (input.input.case !== 'secret') continue
    const method = submittedCredentialMethod(input, formData)
    if (method?.method.case === 'oauth') continue
    const value = formValue(formData, `sec:${input.key}`)
    if (value.length > 0) bindings.push({ key: input.key, secret: true, value })
  }
  return bindings
}

export function firstMissingRequiredInput(info: SourceInfo, formData: FormData): string | null {
  for (const input of info.inputs) {
    if (!input.required) continue
    if (input.input.case === 'variable') {
      const value = formValue(formData, `var:${input.key}`, input.input.value.defaultValue)
      if (value.length === 0) return input.key
      continue
    }
    if (input.input.case !== 'secret') continue
    const methodIndex = Number(formValue(formData, `method:${input.key}`, '0'))
    const method = input.input.value.credential?.methods[methodIndex]
    if (!method || method.method.case === 'sourceConfig') {
      if (formValue(formData, `sec:${input.key}`).length === 0) return input.key
    }
  }
  return null
}

export function firstOAuthMethodInput(info: SourceInfo, formData: FormData): string | null {
  for (const input of info.inputs) {
    if (input.input.case !== 'secret') continue
    const methodIndex = Number(formValue(formData, `method:${input.key}`, '0'))
    const method = input.input.value.credential?.methods[methodIndex]
    if (method?.method.case === 'oauth') return input.key
  }
  return null
}

function bindingsFromInstalledSource(source: Source, formData: FormData): InstallInput[] {
  const bindings: InstallInput[] = source.variables.map((variable) => ({
    key: variable.key,
    secret: false,
    value: formValue(formData, `var:${variable.key}`, variable.value),
  }))
  for (const secret of source.secrets) {
    const value = formValue(formData, `sec:${secret.key}`)
    if (value.length > 0) bindings.push({ key: secret.key, secret: true, value })
  }
  return bindings
}

function submittedCredentialMethod(input: SourceInputSpec, formData: FormData) {
  if (input.input.case !== 'secret') return undefined
  const submittedMethod = formData.get(`method:${input.key}`)
  if (typeof submittedMethod !== 'string') return undefined
  const methodIndex = Number(submittedMethod)
  if (!Number.isInteger(methodIndex) || methodIndex < 0) return undefined
  return input.input.value.credential?.methods[methodIndex]
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
  const response = await sourceClient.createBundledSource(
    create(CreateBundledSourceRequestSchema, {
      name,
      workspace: WORKSPACE,
      variables: bindings
        .filter((binding) => !binding.secret)
        .map((binding) => ({ key: binding.key, value: binding.value })),
      secrets: bindings
        .filter((binding) => binding.secret)
        .map((binding) => ({ key: binding.key, value: binding.value })),
    }),
  )
  if (!response.source) throw new Error(`Coral did not return installed source ${name}`)
  return response.source
}

function actionError(intent: SourceActionIntent, name: string, message: string): SourcesActionData {
  return { intent, message, name, status: 'error' }
}

function formValue(formData: FormData, key: string, defaultValue = ''): string {
  const value = formData.get(key)
  if (typeof value !== 'string') return defaultValue.trim()
  return value.trim()
}

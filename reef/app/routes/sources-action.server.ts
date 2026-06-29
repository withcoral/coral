import { redirect } from 'react-router'
import type { Route } from './+types/sources'

import type { Source, SourceInfo } from '@/generated/coral/v1/sources_pb'
import { type InstallInput, originLabel } from '@/lib/source-data'
import { sourceServiceForRequest } from '@/lib/source-service.server'

export type SourceActionIntent = 'delete' | 'edit' | 'install'

export type SourcesActionData =
  | {
      intent: SourceActionIntent
      message: string
      name: string
      status: 'error'
    }
  | undefined

export async function action({ request }: Route.ActionArgs): Promise<SourcesActionData | Response> {
  const formData = await request.formData()
  const intent = formValue(formData, '_intent')
  const name = formValue(formData, 'name')
  if (!name) return actionError('install', '', 'Missing source name')

  const sources = sourceServiceForRequest(request)
  try {
    if (intent === 'install') {
      const { info } = await sources.getSourceInfo(name)
      if (info.installed && originLabel(info.origin) !== 'bundled') {
        return actionError('install', name, "Imported sources can't be installed here yet")
      }
      if (firstOAuthMethodInput(info, formData)) {
        return actionError('install', name, 'OAuth install is not available in this shell yet')
      }
      const missing = firstMissingRequiredInput(info, formData)
      if (missing) return actionError('install', name, `${missing} is required`)
      await sources.createBundledSource(name, installBindingsFromForm(info, formData))
      return redirect('/sources')
    }
    if (intent === 'edit') {
      const source = await sources.getInstalledSource(name)
      if (originLabel(source.origin) !== 'bundled') {
        return actionError('edit', name, "Imported sources can't be edited here yet")
      }
      const info = await sources
        .getSourceInfo(name)
        .then((resolved) => resolved.info)
        .catch(() => null)
      await sources.createBundledSource(name, editBindingsFromForm(source, info, formData))
      return redirect('/sources')
    }
    if (intent === 'delete') {
      await sources.deleteSource(name)
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
  if (!info) return fallbackEditBindings(source, formData)
  const bindings: InstallInput[] = []
  const variables = new Map(source.variables.map((variable) => [variable.key, variable.value]))
  for (const input of info.inputs) {
    if (input.input.case === 'variable') {
      const fallback = variables.get(input.key) ?? input.input.value.defaultValue
      const value = formValue(formData, `var:${input.key}`, fallback)
      if (value.length > 0) bindings.push({ key: input.key, secret: false, value })
      continue
    }
    if (input.input.case !== 'secret') continue
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

function fallbackEditBindings(source: Source, formData: FormData): InstallInput[] {
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

function actionError(intent: SourceActionIntent, name: string, message: string): SourcesActionData {
  return { intent, message, name, status: 'error' }
}

function formValue(formData: FormData, key: string, fallback = ''): string {
  const value = formData.get(key)
  if (typeof value !== 'string') return fallback.trim()
  return value.trim()
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

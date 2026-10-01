import { create } from '@bufbuild/protobuf'

import {
  OAuthCredentialRetrievalSchema,
  type OAuthCredentialMethod,
  type OAuthCredentialRetrieval,
  type Source,
  type SourceInfo,
  type SourceInputSpec,
} from '@/generated/coral/v1/sources_pb'

export interface InstallInput {
  key: string
  value: string
  secret: boolean
}

export interface SplitInstallBindings {
  secrets: { key: string; value: string }[]
  variables: { key: string; value: string }[]
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
    const method = submittedCredentialMethod(input, formData)
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
    const method = submittedCredentialMethod(input, formData, false)
    if (method?.method.case === 'oauth') continue
    const value = formValue(formData, `sec:${input.key}`)
    if (value.length > 0) bindings.push({ key: input.key, secret: true, value })
  }
  return bindings
}

export function firstMissingRequiredInput(info: SourceInfo, formData: FormData): string | null {
  for (const input of info.inputs) {
    if (input.input.case === 'variable') {
      if (!input.required) continue
      const value = formValue(formData, `var:${input.key}`, input.input.value.defaultValue)
      if (value.length === 0) return input.key
      continue
    }
    if (input.input.case !== 'secret') continue
    const method = submittedCredentialMethod(input, formData)
    if (method?.method.case === 'oauth') {
      const missingOAuthInput = firstMissingOAuthCredentialInput(
        input.key,
        method.method.value,
        formData,
      )
      if (missingOAuthInput) return missingOAuthInput
      continue
    }
    if (!input.required) continue
    if (formValue(formData, `sec:${input.key}`).length === 0) return input.key
  }
  return null
}

export function firstOAuthMethodInput(info: SourceInfo, formData: FormData): string | null {
  for (const input of info.inputs) {
    const method = submittedCredentialMethod(input, formData)
    if (method?.method.case === 'oauth') return input.key
  }
  return null
}

export function oauthCredentialRetrievalsFromForm(
  info: SourceInfo,
  formData: FormData,
): OAuthCredentialRetrieval[] {
  const retrievals: OAuthCredentialRetrieval[] = []
  for (const input of info.inputs) {
    if (input.input.case !== 'secret') continue
    const methodIndex = submittedCredentialMethodIndex(input, formData) ?? 0
    const method = input.input.value.credential?.methods[methodIndex]
    if (method?.method.case !== 'oauth') continue
    retrievals.push(
      create(OAuthCredentialRetrievalSchema, {
        inputKey: input.key,
        methodIndex,
        credentialInputs: oauthCredentialInputsFromForm(input.key, method.method.value, formData),
      }),
    )
  }
  return retrievals
}

export function splitInstallBindings(bindings: InstallInput[]): SplitInstallBindings {
  return {
    secrets: bindings
      .filter((binding) => binding.secret)
      .map((binding) => ({ key: binding.key, value: binding.value })),
    variables: bindings
      .filter((binding) => !binding.secret)
      .map((binding) => ({ key: binding.key, value: binding.value })),
  }
}

export function formValue(formData: FormData, key: string, defaultValue = ''): string {
  const value = formData.get(key)
  if (typeof value !== 'string') return defaultValue.trim()
  return value.trim()
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

function submittedCredentialMethod(
  input: SourceInputSpec,
  formData: FormData,
  defaultToFirst = true,
) {
  if (input.input.case !== 'secret') return undefined
  const methodIndex = submittedCredentialMethodIndex(input, formData, defaultToFirst)
  if (methodIndex === null) return undefined
  return input.input.value.credential?.methods[methodIndex]
}

function submittedCredentialMethodIndex(
  input: SourceInputSpec,
  formData: FormData,
  defaultToFirst = true,
): number | null {
  if (input.input.case !== 'secret') return 0
  const submittedMethod = formData.get(`method:${input.key}`)
  if (typeof submittedMethod !== 'string') return defaultToFirst ? 0 : null
  const methodIndex = Number(submittedMethod)
  const methodCount = input.input.value.credential?.methods.length ?? 0
  if (!Number.isInteger(methodIndex) || methodIndex < 0 || methodIndex >= methodCount) return 0
  return methodIndex
}

function oauthCredentialInputsFromForm(
  inputKey: string,
  oauth: OAuthCredentialMethod,
  formData: FormData,
): { key: string; value: string }[] {
  return oauthClientInputs(oauth)
    .map(({ defaultValue, key }) => ({
      key,
      value: formValue(formData, oauthFieldName(inputKey, key), defaultValue),
    }))
    .filter((entry) => entry.value.length > 0)
}

function firstMissingOAuthCredentialInput(
  sourceInputKey: string,
  oauth: OAuthCredentialMethod,
  formData: FormData,
): string | null {
  for (const input of oauthClientInputs(oauth)) {
    if (!input.required) continue
    const fallback = formValue(formData, oauthFieldName('', input.key), input.defaultValue)
    const scopedValue = formValue(formData, oauthFieldName(sourceInputKey, input.key), fallback)
    if (scopedValue.length === 0) return input.key
  }
  return null
}

export interface OAuthClientInput {
  key: string
  defaultValue?: string
  required: boolean
  secret: boolean
}

/**
 * Which client fields an OAuth method asks the user for. The parameter is the
 * structural subset that both the generated proto method and its catalog
 * projection satisfy, so form serialization and field rendering share one rule.
 */
export function oauthClientInputs(oauth: {
  client?: { id?: { defaultValue?: string; input?: string }; secret?: { input?: string } }
}): OAuthClientInput[] {
  const out: OAuthClientInput[] = []
  const id = oauth.client?.id
  if (id?.input) {
    out.push({
      key: id.input,
      defaultValue: id.defaultValue,
      required: !id.defaultValue,
      secret: false,
    })
  }
  const secret = oauth.client?.secret
  if (secret?.input) out.push({ key: secret.input, required: true, secret: true })
  return out
}

function oauthFieldName(inputKey: string, credentialInputKey: string): string {
  return inputKey ? `oauth:${inputKey}:${credentialInputKey}` : `oauth:${credentialInputKey}`
}

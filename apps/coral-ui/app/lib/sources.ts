import {
  SourceOrigin,
  type OAuthCredentialMethod,
  type Source,
  type SourceCredentialMethod,
  type SourceInfo,
  type SourceInputSpec,
} from '@/generated/coral/v1/sources_pb'

export type SourceOriginLabel = 'bundled' | 'imported' | 'unknown'

export interface CatalogSourceBinding {
  key: string
  value: string
}

export interface CatalogSource {
  name: string
  origin: SourceOriginLabel
  secrets: CatalogSourceBinding[]
  variables: CatalogSourceBinding[]
  version: string
}

export interface CatalogSourceInputSpec {
  hint: string
  input:
    | {
        case: 'variable'
        value: {
          defaultValue: string
        }
      }
    | {
        case: 'secret'
        value: {
          credential?: {
            methods: CatalogSourceCredentialMethod[]
          }
        }
      }
    | { case: undefined; value?: undefined }
  key: string
  required: boolean
}

export interface CatalogSourceCredentialMethod {
  description: string
  hint: string
  label: string
  method:
    | {
        case: 'sourceConfig'
        value: Record<string, never>
      }
    | {
        case: 'oauth'
        value: CatalogOAuthCredentialMethod
      }
    | { case: undefined; value?: undefined }
}

export interface CatalogOAuthCredentialMethod {
  client?: {
    id?: {
      defaultValue: string
      input: string
    }
    secret?: {
      input: string
    }
  }
}

export interface CatalogEntry {
  name: string
  description: string
  version: string
  installed: boolean
  origin: SourceOriginLabel
  inputSpecs?: CatalogSourceInputSpec[]
  source?: CatalogSource
}

export function originLabel(origin: SourceOrigin): SourceOriginLabel {
  if (origin === SourceOrigin.BUNDLED) return 'bundled'
  if (origin === SourceOrigin.IMPORTED) return 'imported'
  return 'unknown'
}

export function catalogEntries(discovered: SourceInfo[], installed: Source[]): CatalogEntry[] {
  const entries = new Map<string, CatalogEntry>()
  for (const info of discovered) {
    entries.set(info.name, toCatalogEntry(info))
  }
  for (const source of installed) {
    const existing = entries.get(source.name)
    if (existing) {
      existing.installed = true
      const installedOrigin = originLabel(source.origin)
      if (installedOrigin !== 'unknown') existing.origin = installedOrigin
      existing.source = toCatalogSource(source)
      existing.version = source.version || existing.version
      continue
    }
    entries.set(source.name, {
      description:
        source.origin === SourceOrigin.IMPORTED ? 'Imported source' : 'Configured source',
      installed: true,
      name: source.name,
      origin: originLabel(source.origin),
      source: toCatalogSource(source),
      version: source.version,
    })
  }
  return [...entries.values()]
}

export function toCatalogSource(source: Source): CatalogSource {
  return {
    name: source.name,
    origin: originLabel(source.origin),
    secrets: source.secrets.map(({ key, value }) => ({ key, value })),
    variables: source.variables.map(({ key, value }) => ({ key, value })),
    version: source.version,
  }
}

export function toCatalogSourceInputSpecs(info: SourceInfo): CatalogSourceInputSpec[] {
  return info.inputs.map(toCatalogSourceInputSpec)
}

export function toCatalogEntry(s: SourceInfo): CatalogEntry {
  return {
    name: s.name,
    description: s.description,
    version: s.version,
    installed: s.installed,
    origin: originLabel(s.origin),
    inputSpecs: toCatalogSourceInputSpecs(s),
  }
}

function toCatalogSourceInputSpec(input: SourceInputSpec): CatalogSourceInputSpec {
  const base = {
    hint: input.hint,
    key: input.key,
    required: input.required,
  }
  if (input.input.case === 'variable') {
    return {
      ...base,
      input: {
        case: 'variable',
        value: {
          defaultValue: input.input.value.defaultValue,
        },
      },
    }
  }
  if (input.input.case === 'secret') {
    return {
      ...base,
      input: {
        case: 'secret',
        value: {
          credential: input.input.value.credential
            ? {
                methods: input.input.value.credential.methods.map(toCatalogCredentialMethod),
              }
            : undefined,
        },
      },
    }
  }
  return {
    ...base,
    input: { case: undefined },
  }
}

function toCatalogCredentialMethod(method: SourceCredentialMethod): CatalogSourceCredentialMethod {
  const base = {
    description: method.description,
    hint: method.hint,
    label: method.label,
  }
  if (method.method.case === 'sourceConfig') {
    return {
      ...base,
      method: {
        case: 'sourceConfig',
        value: {},
      },
    }
  }
  if (method.method.case === 'oauth') {
    return {
      ...base,
      method: {
        case: 'oauth',
        value: toCatalogOAuthCredentialMethod(method.method.value),
      },
    }
  }
  return {
    ...base,
    method: { case: undefined },
  }
}

function toCatalogOAuthCredentialMethod(
  method: OAuthCredentialMethod,
): CatalogOAuthCredentialMethod {
  return {
    client: method.client
      ? {
          id: method.client.id
            ? {
                defaultValue: method.client.id.defaultValue,
                input: method.client.id.input,
              }
            : undefined,
          secret: method.client.secret
            ? {
                input: method.client.secret.input,
              }
            : undefined,
        }
      : undefined,
  }
}

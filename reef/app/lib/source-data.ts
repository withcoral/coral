import type { Source, SourceInfo } from '@/generated/coral/v1/sources_pb'
import { SourceOrigin } from '@/generated/coral/v1/sources_pb'

export const SOURCE_SERVICE_PATH = '/coral.v1.SourceService'

export type SourceOriginLabel = 'bundled' | 'imported' | 'unknown'

export interface CatalogEntry {
  description: string
  installed: boolean
  name: string
  origin: SourceOriginLabel
  version: string
}

export interface InstallInput {
  key: string
  secret: boolean
  value: string
}

export interface ResolvedSourceInfo {
  info: SourceInfo
}

type UiData<T> = T extends string | number | boolean | null | undefined
  ? T
  : T extends Array<infer U>
    ? UiData<U>[]
    : T extends ReadonlyArray<infer U>
      ? UiData<U>[]
      : T extends object
        ? { [K in keyof T as K extends '$unknown' ? never : K]: UiData<T[K]> }
        : never

export type SourceData = UiData<Source>
export type SourceInfoData = UiData<SourceInfo>

export function catalogEntries(discovered: SourceInfo[], installed: Source[]): CatalogEntry[] {
  const entries = new Map<string, CatalogEntry>()
  for (const info of discovered) {
    entries.set(info.name, {
      description: info.description,
      installed: info.installed,
      name: info.name,
      origin: originLabel(info.origin),
      version: info.version,
    })
  }
  for (const source of installed) {
    const existing = entries.get(source.name)
    if (existing) {
      existing.installed = true
      existing.version ||= source.version
      continue
    }
    entries.set(source.name, {
      description:
        source.origin === SourceOrigin.IMPORTED ? 'Imported source' : 'Configured source',
      installed: true,
      name: source.name,
      origin: originLabel(source.origin),
      version: source.version,
    })
  }
  return [...entries.values()]
}

export function originLabel(origin: SourceOrigin): SourceOriginLabel {
  if (origin === SourceOrigin.BUNDLED) return 'bundled'
  if (origin === SourceOrigin.IMPORTED) return 'imported'
  return 'unknown'
}

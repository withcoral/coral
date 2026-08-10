import {
  SearchProvider,
  type SearchField,
  type SearchFieldValues,
  type SearchResult,
  type SearchResultTruncation,
  type SearchSurfaceRef,
} from '@/generated/coral/v1/search_pb'
import type { TraceSearchResponse } from '@/generated/coral/v1/traces_pb'

type RetainedProtoData<T> = T extends readonly (infer Item)[]
  ? readonly RetainedProtoData<Item>[]
  : T extends object
    ? {
        readonly [Key in keyof T as Key extends '$typeName' | '$unknown'
          ? never
          : Key]: RetainedProtoData<T[Key]>
      }
    : T

export type TraceSearchResponseData = RetainedProtoData<TraceSearchResponse>

export interface SearchFieldView {
  dataType: string
  name: string
  required: boolean
}

export interface SearchMatchingValuesView {
  field: string
  values: string[]
}

export interface SearchProviderView {
  label: string
  tone: 'catalog' | 'neutral' | 'observed'
}

export interface SearchTruncationView {
  maxResults: number
  note: string
  returnedCount: number
  truncated: boolean
}

export interface SearchKnownResultViewBase {
  description: string
  guide: string
  matchingValues: SearchMatchingValuesView[]
  omittedMatchingFieldCount: number
  providers: SearchProviderView[]
  sqlReference: string
}

export interface SearchTableResultView extends SearchKnownResultViewBase {
  fields: SearchFieldView[]
  kind: 'table'
}

export interface SearchFunctionResultView extends SearchKnownResultViewBase {
  arguments: SearchFieldView[]
  kind: 'function'
  returns: SearchFieldView[]
}

export interface SearchUnknownResultView {
  kind: 'unknown'
}

export type SearchResultView =
  | SearchFunctionResultView
  | SearchTableResultView
  | SearchUnknownResultView

export type SearchResultsView =
  | {
      results: SearchResultView[]
      state: 'available'
      truncation?: SearchTruncationView
    }
  | { state: 'tooLarge' }
  | { state: 'unavailable' }

function mapFields(fields: readonly RetainedProtoData<SearchField>[]): SearchFieldView[] {
  return fields.map((field) => ({
    dataType: field.dataType,
    name: field.name,
    required: field.required,
  }))
}

function mapMatchingValues(
  values: readonly RetainedProtoData<SearchFieldValues>[],
): SearchMatchingValuesView[] {
  return values.map((entry) => ({ field: entry.field, values: [...entry.values] }))
}

export function formatSearchSqlIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`
}

export function formatSearchSqlReference(surface: RetainedProtoData<SearchSurfaceRef>): string {
  return [surface.catalogName, surface.schemaName, surface.name]
    .filter((part) => part.length > 0)
    .map(formatSearchSqlIdentifier)
    .join('.')
}

function mapProvider(provider: SearchProvider): SearchProviderView {
  if (provider === SearchProvider.CATALOG_METADATA) {
    return { label: 'Catalog', tone: 'catalog' }
  }
  if (provider === SearchProvider.OBSERVED_VALUES) {
    return { label: 'Observed values', tone: 'observed' }
  }
  if (provider === SearchProvider.NATIVE_FANOUT) {
    return { label: 'Native fanout', tone: 'neutral' }
  }
  return { label: 'Unknown provider', tone: 'neutral' }
}

function mapTruncation(
  truncation: RetainedProtoData<SearchResultTruncation>,
): SearchTruncationView {
  return {
    maxResults: truncation.maxResults,
    note: truncation.note,
    returnedCount: truncation.returnedCount,
    truncated: truncation.truncated,
  }
}

function knownResultBase(
  result: RetainedProtoData<SearchResult>,
  surface: RetainedProtoData<SearchSurfaceRef>,
) {
  return {
    description: result.description,
    guide: result.guide,
    matchingValues: mapMatchingValues(result.matchingValues),
    omittedMatchingFieldCount: result.omittedMatchingFieldCount,
    providers: result.providers.map(mapProvider),
    sqlReference: formatSearchSqlReference(surface),
  }
}

function formatFunctionArguments(functionArguments: SearchFieldView[]): string {
  const argumentsToShow = functionArguments
    .filter((argument) => argument.required)
    .map((argument) => argument.name)
  if (functionArguments.some((argument) => !argument.required)) argumentsToShow.push('...')
  return `(${argumentsToShow.join(', ')})`
}

function mapResult(result: RetainedProtoData<SearchResult>): SearchResultView {
  if (!result.surface) return { kind: 'unknown' }

  if (result.shape.case === 'table') {
    return {
      ...knownResultBase(result, result.surface),
      fields: mapFields(result.shape.value.fields),
      kind: 'table',
    }
  }

  if (result.shape.case === 'function') {
    const functionArguments = mapFields(result.shape.value.arguments)
    return {
      ...knownResultBase(result, result.surface),
      arguments: functionArguments,
      kind: 'function',
      returns: mapFields(result.shape.value.returns),
      sqlReference: `${formatSearchSqlReference(result.surface)}${formatFunctionArguments(functionArguments)}`,
    }
  }

  return { kind: 'unknown' }
}

export function mapTraceSearchResponse(
  searchResponse?: TraceSearchResponseData,
): SearchResultsView {
  if (!searchResponse) return { state: 'unavailable' }
  if (searchResponse.outcome.case === 'tooLarge') return { state: 'tooLarge' }
  if (searchResponse.outcome.case !== 'response') return { state: 'unavailable' }

  const response = searchResponse.outcome.value
  const view: Extract<SearchResultsView, { state: 'available' }> = {
    results: response.results.map(mapResult),
    state: 'available',
  }
  if (response.truncation) view.truncation = mapTruncation(response.truncation)
  return view
}

export function searchResultsTabLabel(view: SearchResultsView): string {
  return view.state === 'available' ? `Results ${view.results.length}` : 'Results'
}

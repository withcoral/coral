import { z } from 'zod'
import type {
  Description,
  DiscoveryContext,
  DiscoveryView,
  OperationFacts,
  SearchHit,
  SearchPage,
} from '../types'

/*
 * The keys Reef reads from a stored `search` or `describe` response. Coral
 * writes them in `crates/coral-search/src/projection.rs` and advertises them as
 * the tools' output schemas. `response.test.ts` checks each key and type here
 * against the snapshots of those schemas, so change both together.
 */
const entityKind = z.enum(['operation', 'saved_function'])
const hints = z.object({
  read_only: z.boolean().optional(),
  destructive: z.boolean().optional(),
  idempotent: z.boolean().optional(),
  open_world: z.boolean().optional(),
})
const hitFields = {
  call_path: z.string(),
  signature_line: z.string(),
  description_snippet: z.string(),
  evidence: z.object({ fields: z.array(z.string()), paths: z.array(z.string()) }),
}
const count = z.int().min(0)
export const searchPageSchema = z.object({
  hits: z.array(
    z.discriminatedUnion('kind', [
      z.object({
        kind: z.literal('operation'),
        ...hitFields,
        provider_display_name: z.string(),
        title: z.string().nullable(),
        hints,
        approval_required: z.boolean(),
        deprecated: z.boolean(),
        guidance_snippet: z.string().nullish(),
        matched_tags: z.array(z.string()).optional(),
      }),
      z.object({ kind: z.literal('saved_function'), ...hitFields }),
    ]),
  ),
  has_more: z.boolean(),
  retriever_statuses: z.array(
    z.object({
      retriever: entityKind,
      state: z.enum(['results_found', 'empty', 'skipped', 'partial', 'error']),
      coverage: z.object({
        eligible_units: count,
        searched_units: count,
        failed_units: count,
        returned_count: count,
        has_more: z.boolean(),
      }),
    }),
  ),
})
const authoringFields = {
  call_path: z.string(),
  description: z.string(),
  typescript_declaration: z.string(),
}
const position = z.int().min(0).nullable()
export const descriptionSchema = z.discriminatedUnion('kind', [
  z.object({
    kind: z.literal('operation'),
    ...authoringFields,
    provider_display_name: z.string(),
    title: z.string().nullable(),
    deprecated: z.boolean(),
    tags: z.array(z.string()),
    guidance: z.string().nullable(),
    hints,
    approval_required: z.boolean(),
  }),
  z.object({
    kind: z.literal('saved_function'),
    ...authoringFields,
    // Absent when `describe` returned only the types it was asked for.
    source: z.string().optional(),
    diagnostics: z
      .array(
        z.object({
          category: z.enum(['message', 'warning', 'error', 'suggestion']),
          code: z.int().min(0),
          message: z.string(),
          line: position,
          column: position,
        }),
      )
      .optional(),
    diagnostics_truncated: z.boolean().optional(),
  }),
])

const OPERATIONS = 'coral.providers.'
const FUNCTIONS = 'coral.functions.'
/** The provider namespace and the path inside it, for a complete Operation Call Path. */
export function operationTarget(callPath: string) {
  if (!callPath.startsWith(OPERATIONS)) return null
  const rest = callPath.slice(OPERATIONS.length)
  const dot = rest.indexOf('.')
  return dot > 0 ? { namespace: rest.slice(0, dot), relativePath: rest.slice(dot + 1) } : null
}
/** The Saved Function Path, for a complete Saved Function Call Path. */
export function functionTarget(callPath: string) {
  return callPath.startsWith(FUNCTIONS) ? callPath.slice(FUNCTIONS.length) : null
}

function operationFacts(value: {
  provider_display_name: string
  approval_required: boolean
  deprecated: boolean
  hints: z.infer<typeof hints>
}): OperationFacts {
  return {
    providerLabel: value.provider_display_name,
    approvalRequired: value.approval_required,
    deprecated: value.deprecated,
    hints: {
      readOnly: value.hints.read_only,
      destructive: value.hints.destructive,
      idempotent: value.hints.idempotent,
      openWorld: value.hints.open_world,
    },
  }
}
/** An Operation without a title and a Saved Function show their path. */
function title(value: { call_path: string; title?: string | null }) {
  return (
    value.title ||
    functionTarget(value.call_path) ||
    operationTarget(value.call_path)?.relativePath ||
    value.call_path
  )
}

function searchPage(value: z.infer<typeof searchPageSchema>): SearchPage {
  return {
    hits: value.hits.map((hit): SearchHit => ({
      kind: hit.kind,
      callPath: hit.call_path,
      title: title(hit),
      signatureLine: hit.signature_line,
      descriptionSnippet: hit.description_snippet,
      guidanceSnippet: (hit.kind === 'operation' && hit.guidance_snippet) || null,
      operation: hit.kind === 'operation' ? operationFacts(hit) : null,
      matched: {
        fields: hit.evidence.fields,
        tags: (hit.kind === 'operation' && hit.matched_tags) || [],
        paths: hit.evidence.paths,
      },
      currentHref: null,
    })),
    hasMore: value.has_more,
    coverage: value.retriever_statuses.map(({ retriever, state, coverage }) => ({
      kind: retriever,
      state,
      eligible: coverage.eligible_units,
      searched: coverage.searched_units,
      failed: coverage.failed_units,
      returned: coverage.returned_count,
      hasMore: coverage.has_more,
    })),
  }
}

function description(value: z.infer<typeof descriptionSchema>): Description {
  return {
    kind: value.kind,
    callPath: value.call_path,
    title: title(value),
    description: value.description,
    declaration: value.typescript_declaration,
    operation:
      value.kind === 'operation'
        ? { ...operationFacts(value), tags: value.tags, guidance: value.guidance }
        : null,
    savedFunction:
      value.kind === 'saved_function' && value.source !== undefined
        ? {
            source: value.source,
            diagnostics: value.diagnostics ?? [],
            diagnosticsTruncated: value.diagnostics_truncated ?? false,
          }
        : null,
    currentHref: null,
  }
}

/**
 * Reads one stored discovery response. Links to the current catalog start
 * empty: the Activity loader resolves them.
 */
export function discoveryView(context: DiscoveryContext, response: string): DiscoveryView {
  let value: unknown
  try {
    value = JSON.parse(response)
  } catch {
    return { kind: 'raw' }
  }
  if (context.kind === 'search') {
    const page = searchPageSchema.safeParse(value)
    if (page.success) return { kind: 'search', page: searchPage(page.data) }
  }
  if (context.kind === 'describe') {
    const exact = descriptionSchema.safeParse(value)
    if (exact.success) return { kind: 'describe', description: description(exact.data) }
  }
  return { kind: 'raw' }
}

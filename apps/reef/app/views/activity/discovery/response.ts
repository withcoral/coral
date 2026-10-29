import { z } from 'zod'
import type {
  Description,
  DiscoveryContext,
  DiscoveryView,
  ToolFacts,
  ToolFlags,
  SearchCoverage,
  SearchHit,
  SearchPage,
} from '../types'

/*
 * The keys Reef reads from a stored `search` or `describe` response. Coral
 * writes them in `crates/coral-search/src/projection.rs` and advertises them as
 * the tools' output schemas. `response.test.ts` checks each key and type here
 * against the snapshots of those schemas, so change both together.
 */
const entityKind = z.enum(['provider_tool', 'saved_function'])
const hints = z.object({
  read_only: z.boolean().optional(),
  destructive: z.boolean().optional(),
  idempotent: z.boolean().optional(),
  open_world: z.boolean().optional(),
})
const count = z.int().min(0)
const retrieverCoverage = z.object({
  eligible_units: count,
  searched_units: count,
  failed_units: count,
  returned_count: count,
  has_more: z.boolean(),
})
export const searchPageSchema = z.object({
  hits: z.array(
    z.object({
      call_path: z.string(),
      signature_line: z.string(),
      description_snippet: z.string(),
      guidance_snippet: z.string().nullish(),
      matched_tags: z.array(z.string()).optional(),
      matched_inputs: z.array(z.string()).optional(),
      hints: hints.optional(),
      approval_required: z.boolean().optional(),
      deprecated: z.boolean().optional(),
    }),
  ),
  has_more: z.boolean(),
  retriever_statuses: z
    .array(
      z.object({
        retriever: entityKind,
        state: z.enum(['partial', 'error']),
        coverage: retrieverCoverage,
      }),
    )
    .optional(),
})

/*
 * A search page that Coral retained before it trimmed each hit to what an
 * agent acts on. Coral no longer writes this shape, so no advertised schema
 * checks it. Keep it while Activity can hold such pages.
 */
const untrimmedHitFields = {
  call_path: z.string(),
  signature_line: z.string(),
  description_snippet: z.string(),
  evidence: z.object({ fields: z.array(z.string()), paths: z.array(z.string()) }),
}
const untrimmedSearchPageSchema = z.object({
  hits: z.array(
    z.discriminatedUnion('kind', [
      z.object({
        kind: z.literal('provider_tool'),
        ...untrimmedHitFields,
        provider_display_name: z.string(),
        title: z.string().nullable(),
        hints,
        approval_required: z.boolean(),
        deprecated: z.boolean(),
        guidance_snippet: z.string().nullish(),
        matched_tags: z.array(z.string()).optional(),
      }),
      z.object({ kind: z.literal('saved_function'), ...untrimmedHitFields }),
    ]),
  ),
  has_more: z.boolean(),
  retriever_statuses: z.array(
    z.object({
      retriever: entityKind,
      state: z.enum(['results_found', 'empty', 'skipped', 'partial', 'error']),
      coverage: retrieverCoverage,
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
    kind: z.literal('provider_tool'),
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

const FUNCTIONS = 'coral.functions.'
/** The Saved Function Path, for a complete Saved Function Call Path. */
export function functionTarget(callPath: string) {
  return callPath.startsWith(FUNCTIONS) ? callPath.slice(FUNCTIONS.length) : null
}

function toolFlags(value: {
  approval_required?: boolean
  deprecated?: boolean
  hints?: z.infer<typeof hints>
}): ToolFlags {
  return {
    approvalRequired: value.approval_required ?? false,
    deprecated: value.deprecated ?? false,
    hints: {
      readOnly: value.hints?.read_only,
      destructive: value.hints?.destructive,
      idempotent: value.hints?.idempotent,
      openWorld: value.hints?.open_world,
    },
  }
}
function toolFacts(
  value: Parameters<typeof toolFlags>[0] & { provider_display_name: string },
): ToolFacts {
  return { providerLabel: value.provider_display_name, ...toolFlags(value) }
}
/**
 * A Saved Function shows its path. A Tool without a title shows its Call
 * Path until the loader links it to a provider and shortens it.
 */
function title(value: { call_path: string; title?: string | null }) {
  return value.title || functionTarget(value.call_path) || value.call_path
}

function searchCoverage({
  retriever,
  state,
  coverage,
}: z.infer<typeof untrimmedSearchPageSchema>['retriever_statuses'][number]): SearchCoverage {
  return {
    kind: retriever,
    state,
    eligible: coverage.eligible_units,
    searched: coverage.searched_units,
    failed: coverage.failed_units,
    returned: coverage.returned_count,
    hasMore: coverage.has_more,
  }
}

/** The Call Path names the kind: Saved Functions live under `coral.functions`. */
function searchPage(value: z.infer<typeof searchPageSchema>): SearchPage {
  return {
    hits: value.hits.map((hit): SearchHit => {
      const kind = functionTarget(hit.call_path) === null ? 'provider_tool' : 'saved_function'
      return {
        kind,
        callPath: hit.call_path,
        title: title(hit),
        signatureLine: hit.signature_line,
        descriptionSnippet: hit.description_snippet,
        guidanceSnippet: hit.guidance_snippet ?? null,
        tool: kind === 'provider_tool' ? toolFlags(hit) : null,
        matched: {
          fields: [],
          tags: hit.matched_tags ?? [],
          paths: (hit.matched_inputs ?? []).map((name) => `input.${name}`),
        },
        currentHref: null,
      }
    }),
    hasMore: value.has_more,
    coverage: (value.retriever_statuses ?? []).map(searchCoverage),
  }
}

function untrimmedSearchPage(value: z.infer<typeof untrimmedSearchPageSchema>): SearchPage {
  return {
    hits: value.hits.map((hit): SearchHit => ({
      kind: hit.kind,
      callPath: hit.call_path,
      title: title(hit),
      signatureLine: hit.signature_line,
      descriptionSnippet: hit.description_snippet,
      guidanceSnippet: (hit.kind === 'provider_tool' && hit.guidance_snippet) || null,
      tool: hit.kind === 'provider_tool' ? toolFlags(hit) : null,
      matched: {
        fields: hit.evidence.fields,
        tags: (hit.kind === 'provider_tool' && hit.matched_tags) || [],
        paths: hit.evidence.paths,
      },
      currentHref: null,
    })),
    hasMore: value.has_more,
    coverage: value.retriever_statuses.map(searchCoverage),
  }
}

function description(value: z.infer<typeof descriptionSchema>): Description {
  return {
    kind: value.kind,
    callPath: value.call_path,
    title: title(value),
    description: value.description,
    declaration: value.typescript_declaration,
    tool:
      value.kind === 'provider_tool'
        ? { ...toolFacts(value), tags: value.tags, guidance: value.guidance }
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
    // The current schema drops keys it does not read, so it could take an untrimmed page and lose its titles.
    const untrimmed = untrimmedSearchPageSchema.safeParse(value)
    if (untrimmed.success) return { kind: 'search', page: untrimmedSearchPage(untrimmed.data) }
    const page = searchPageSchema.safeParse(value)
    if (page.success) return { kind: 'search', page: searchPage(page.data) }
  }
  if (context.kind === 'describe') {
    const exact = descriptionSchema.safeParse(value)
    if (exact.success) return { kind: 'describe', description: description(exact.data) }
  }
  return { kind: 'raw' }
}

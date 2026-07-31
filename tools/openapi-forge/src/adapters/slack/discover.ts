/**
 * Discovering Slack's Web API methods.
 *
 * Two upstream indexes, joined:
 *
 * - `https://docs.slack.dev/llms-sitemap.md` lists every documentation page as
 *   a markdown URL. Filtering to `/reference/methods/` enumerates the API. The
 *   slugs are lowercased, so `chat.postMessage` appears as `chat.postmessage`.
 *
 * - `slackapi/java-slack-sdk` carries a recorded response sample per method
 *   under `json-logs/samples/api/`, named in the method's real casing. That
 *   directory listing is what recovers the casing the sitemap flattened, and
 *   the samples themselves are the only published description of Slack's
 *   response shapes — they are what Slack's own Node SDK generates its response
 *   types from.
 */

export const DOCS_ORIGIN = 'https://docs.slack.dev'
export const SITEMAP_URL = `${DOCS_ORIGIN}/llms-sitemap.md`
export const SAMPLE_INDEX_URL =
  'https://api.github.com/repos/slackapi/java-slack-sdk/contents/json-logs/samples/api?ref=main'
export const SAMPLE_RAW_BASE =
  'https://raw.githubusercontent.com/slackapi/java-slack-sdk/main/json-logs/samples/api'

/**
 * `@slack/web-api`'s hand-maintained request types, used to cross-check the
 * arguments read off the reference pages.
 */
export const SDK_INDEX_URL =
  'https://api.github.com/repos/slackapi/node-slack-sdk/contents/packages/web-api/src/types/request?ref=main'
export const SDK_RAW_BASE =
  'https://raw.githubusercontent.com/slackapi/node-slack-sdk/main/packages/web-api/src/types/request'

export function sdkUrlFor(file: string): string {
  return `${SDK_RAW_BASE}/${file}`
}

/**
 * SDK files worth fetching: the families in scope plus `common.ts`, which
 * holds the mixins every request interface composes.
 */
export function sdkFilesFor(methods: readonly string[], available: readonly string[]): string[] {
  const families = new Set(methods.map((method) => method.split('.')[0]?.toLowerCase()))
  const wanted = new Set(['common.ts'])
  for (const file of available) {
    if (families.has(file.replace(/\.ts$/, '').toLowerCase())) {
      wanted.add(file)
    }
  }
  return [...wanted].toSorted((left, right) => left.localeCompare(right))
}

/** TypeScript filenames from a GitHub contents listing. */
export function parseSdkIndex(listing: string): string[] {
  const entries = JSON.parse(listing) as { name?: unknown; type?: unknown }[]
  if (!Array.isArray(entries)) {
    throw new Error('SDK index is not a GitHub contents listing')
  }
  return entries
    .filter((entry) => entry.type === 'file' && typeof entry.name === 'string')
    .map((entry) => String(entry.name))
    .filter((name) => name.endsWith('.ts'))
    .toSorted((left, right) => left.localeCompare(right))
}

const METHOD_PAGE = /https:\/\/docs\.slack\.dev\/reference\/methods\/([a-z0-9._]+)\.md/g

/** One discovered method and the upstream inputs that describe it. */
export interface DiscoveredMethod {
  /** The method in its real casing, e.g. `chat.postMessage`. */
  name: string
  /** The lowercased documentation slug, e.g. `chat.postmessage`. */
  slug: string
  docsUrl: string
  /** Absent when the SDK records no sample for this method. */
  sampleUrl?: string
}

/** Lowercased documentation slugs, sorted and deduplicated. */
export function parseMethodIndex(sitemap: string): string[] {
  const slugs = new Set<string>()
  for (const match of sitemap.matchAll(METHOD_PAGE)) {
    const slug = match[1]
    if (slug !== undefined) {
      slugs.add(slug)
    }
  }
  return [...slugs].toSorted((left, right) => left.localeCompare(right))
}

interface GitHubContentEntry {
  name?: unknown
  type?: unknown
}

/** Real-cased method names taken from the sample filenames. */
export function parseSampleIndex(listing: string): string[] {
  const entries = JSON.parse(listing) as GitHubContentEntry[]
  if (!Array.isArray(entries)) {
    throw new Error('sample index is not a GitHub contents listing')
  }
  return entries
    .filter((entry) => entry.type === 'file' && typeof entry.name === 'string')
    .map((entry) => String(entry.name))
    .filter((name) => name.endsWith('.json'))
    .map((name) => name.slice(0, -'.json'.length))
    .toSorted((left, right) => left.localeCompare(right))
}

export function docsUrlFor(slug: string): string {
  return `${DOCS_ORIGIN}/reference/methods/${slug}.md`
}

export function sampleUrlFor(method: string): string {
  return `${SAMPLE_RAW_BASE}/${method}.json`
}

/** The reference page for one scope, e.g. `channels.read`. */
export function scopeUrlFor(slug: string): string {
  return `${DOCS_ORIGIN}/reference/scopes/${slug}.md`
}

/**
 * Join the two indexes.
 *
 * The sitemap decides what exists — it is Slack's own documentation — while the
 * sample listing supplies real casing and response shapes. A documented method
 * with no recorded sample is still returned, because its arguments are usable
 * even when its response has to be treated as opaque.
 */
export function joinIndexes(
  slugs: readonly string[],
  sampleNames: readonly string[],
): {
  methods: DiscoveredMethod[]
  /** Samples with no matching documentation page; usually removed methods. */
  samplesWithoutDocs: string[]
} {
  const byLowercase = new Map(sampleNames.map((name) => [name.toLowerCase(), name]))
  const methods = slugs.map((slug) => {
    const sampleName = byLowercase.get(slug)
    return {
      name: sampleName ?? slug,
      slug,
      docsUrl: docsUrlFor(slug),
      ...(sampleName === undefined ? {} : { sampleUrl: sampleUrlFor(sampleName) }),
    }
  })
  const documented = new Set(slugs)
  return {
    methods,
    samplesWithoutDocs: sampleNames.filter((name) => !documented.has(name.toLowerCase())),
  }
}

/**
 * Select the configured methods, matched case-insensitively.
 *
 * Anything configured but not discovered is reported rather than skipped: a
 * method that disappears upstream should surface as an error, not as a table
 * that quietly stops being generated.
 */
export function selectScope(
  methods: readonly DiscoveredMethod[],
  configured: readonly string[],
): { selected: DiscoveredMethod[]; missing: string[] } {
  const byLowercase = new Map(methods.map((method) => [method.name.toLowerCase(), method]))
  const selected: DiscoveredMethod[] = []
  const missing: string[] = []
  for (const name of configured) {
    const method = byLowercase.get(name.toLowerCase())
    if (method === undefined) {
      missing.push(name)
      continue
    }
    selected.push(method)
  }
  return { selected, missing }
}

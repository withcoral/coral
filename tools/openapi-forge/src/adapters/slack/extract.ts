/**
 * Assembling the Slack {@link ApiModel} from the pinned snapshot.
 *
 * Reads only the snapshot, never the network, so a build is reproducible from
 * what is committed.
 */

import type {
  ApiModel,
  ObjectNode,
  Operation,
  SchemaNode,
  Scopes,
  SecurityRequirement,
  SecurityScheme,
} from '../../core/model.ts'
import type { ForgeConfig } from '../../core/config.ts'
import type { Snapshot } from '../../core/snapshot.ts'
import { inferSchema } from '../../core/infer.ts'
import { pascalCase } from '../../core/emit.ts'
import { parseDocsPage, parseSourceUrl } from './docs.ts'
import { crossCheckArguments, parseSdkTypes } from './sdkTypes.ts'
import type { ScopeFacts } from './scopes.ts'
import { parseScopePage } from './scopes.ts'

/** One method's inputs within the snapshot. */
interface SnapshotEntry {
  /** Real casing, e.g. `conversations.history`. */
  method: string
  docsPath: string
  samplePath?: string
}

export async function extractApiModel(config: ForgeConfig, snapshot: Snapshot): Promise<ApiModel> {
  const entries = indexSnapshot(snapshot)
  const warnings: string[] = []

  const configured = new Set(config.methods.map((method) => method.toLowerCase()))
  const available = new Set(entries.map((entry) => entry.method.toLowerCase()))
  const notFetched = [...configured].filter((method) => !available.has(method))
  if (notFetched.length > 0) {
    throw new Error(
      `snapshot is missing configured methods: ${notFetched.toSorted().join(', ')}. ` +
        `Run 'forge fetch --api slack' to refresh it.`,
    )
  }
  const stale = [...available].filter((method) => !configured.has(method))
  if (stale.length > 0) {
    warnings.push(
      `snapshot holds methods that are no longer in scope: ${stale.toSorted().join(', ')}`,
    )
  }

  const sdk = await readSdkTypes(snapshot)
  const scopeFacts = await readScopeFacts(snapshot)
  const undescribed = new Set<string>()
  const operations: Operation[] = []
  for (const entry of entries) {
    if (!configured.has(entry.method.toLowerCase())) {
      continue
    }
    operations.push(await buildOperation(entry, config, snapshot, sdk))
  }

  const securitySchemes = buildSecuritySchemes(config, operations, scopeFacts, undescribed)
  if (undescribed.size > 0) {
    warnings.push(
      `Slack publishes no reference page for these scopes, so they are emitted ` +
        `without a description: ${[...undescribed].toSorted().join(', ')}`,
    )
  }

  return {
    api: config.api,
    title: config.title,
    description: config.description,
    serverUrl: config.serverUrl,
    securitySchemes,
    operations: operations.toSorted((left, right) => left.id.localeCompare(right.id)),
    warnings,
  }
}

/** Scope reference pages, keyed by scope name. */
async function readScopeFacts(snapshot: Snapshot): Promise<Map<string, ScopeFacts>> {
  const facts = new Map<string, ScopeFacts>()
  for (const input of snapshot.list('scopes/')) {
    const parsed = parseScopePage(await snapshot.readText(input.path))
    facts.set(parsed.name, parsed)
  }
  return facts
}

/**
 * One scheme per configured token class, carrying the scopes in-scope
 * operations actually use.
 *
 * OpenAPI requires every scope named in a `security` requirement to be
 * declared by its scheme, so the scope map is built from the operations rather
 * than from the full published list.
 */
function buildSecuritySchemes(
  config: ForgeConfig,
  operations: readonly Operation[],
  scopeFacts: ReadonlyMap<string, ScopeFacts>,
  undescribed: Set<string>,
): SecurityScheme[] {
  return config.securitySchemes.map((declared) => {
    const scopes: Record<string, string> = {}
    for (const operation of operations) {
      for (const requirement of operation.security) {
        if (requirement.scheme !== declared.name) {
          continue
        }
        for (const scope of requirement.scopes) {
          scopes[scope] = scopeFacts.get(scope)?.description ?? ''
          if (!scopeFacts.has(scope)) {
            undescribed.add(scope)
          }
        }
      }
    }
    return {
      name: declared.name,
      description: declared.description,
      authorizationUrl: declared.authorizationUrl,
      tokenUrl: declared.tokenUrl,
      scopes: Object.fromEntries(
        Object.entries(scopes).toSorted(([left], [right]) => left.localeCompare(right)),
      ),
    }
  })
}

/**
 * Turn documented scopes into OpenAPI security requirements.
 *
 * Slack lists a method's scopes without saying how they relate, and the
 * relationship differs: `conversations.list` accepts *any* of
 * `channels:read`/`groups:read`/`im:read`/`mpim:read` — one per conversation
 * type — while `team.externalTeams.list` needs *both* `team:read` and
 * `conversations.connect:manage`. The two render identically, so the default is
 * `any` and the overlay declares the exceptions.
 */
export function buildSecurity(
  scopes: Scopes,
  config: ForgeConfig,
  relation: 'any' | 'all',
): SecurityRequirement[] {
  const requirements: SecurityRequirement[] = []
  for (const scheme of config.securitySchemes) {
    const granted = scheme.tokenClass === 'bot' ? scopes.bot : scopes.user
    if (granted.length === 0) {
      // A token class the operation does not accept at all — Slack omits the
      // section entirely rather than listing an empty one.
      continue
    }
    if (relation === 'all') {
      requirements.push({ scheme: scheme.name, scopes: [...granted] })
      continue
    }
    for (const scope of granted) {
      requirements.push({ scheme: scheme.name, scopes: [scope] })
    }
  }
  return requirements
}

/**
 * Pair each documentation page with its sample.
 *
 * Documentation slugs are lowercased upstream while sample filenames keep the
 * method's real casing, so the two are joined case-insensitively and the sample
 * name wins — it is the casing the request path needs.
 */
function indexSnapshot(snapshot: Snapshot): SnapshotEntry[] {
  const samples = new Map(
    snapshot.list('samples/').map((input) => {
      const method = input.path.slice('samples/'.length, -'.json'.length)
      return [method.toLowerCase(), { method, path: input.path }]
    }),
  )
  return snapshot.list('docs/').map((input) => {
    const slug = input.path.slice('docs/'.length, -'.md'.length)
    const sample = samples.get(slug.toLowerCase())
    return {
      method: sample?.method ?? slug,
      docsPath: input.path,
      ...(sample === undefined ? {} : { samplePath: sample.path }),
    }
  })
}

/** The SDK's request types, or an empty map when none were snapshotted. */
async function readSdkTypes(snapshot: Snapshot): Promise<Map<string, Set<string>>> {
  const files = new Map<string, string>()
  for (const input of snapshot.list('sdk/')) {
    files.set(input.path.slice('sdk/'.length), await snapshot.readText(input.path))
  }
  return files.size === 0 ? new Map() : parseSdkTypes(files)
}

async function buildOperation(
  entry: SnapshotEntry,
  config: ForgeConfig,
  snapshot: Snapshot,
  sdk: ReadonlyMap<string, Set<string>>,
): Promise<Operation> {
  const markdown = await snapshot.readText(entry.docsPath)
  const facts = parseDocsPage(markdown, config.serverUrl)
  const warnings = [...facts.warnings]

  if (facts.httpMethod !== 'get') {
    // Coral publishes GET operations only, so a non-GET method produces a
    // relation that exists in the catalog but can never be queried.
    warnings.push(
      `documented as ${facts.httpMethod.toUpperCase()}; Coral hides non-GET operations, ` +
        `so this will generate a relation nobody can query`,
    )
  }

  let response: SchemaNode = { kind: 'unknown' }
  if (entry.samplePath === undefined) {
    // Arguments are still usable without a sample; the response just has to be
    // treated as opaque rather than guessed at.
    warnings.push('no recorded response sample; the response is described as opaque')
  } else {
    response = nameRowComponents(inferSchema(await snapshot.readJson(entry.samplePath)))
  }

  warnings.push(...crossCheckArguments(facts.method, facts.documentedArguments, sdk))

  const { group, leaf } = splitMethod(facts.method)
  const docsUrl = parseSourceUrl(markdown)
  return {
    id: facts.method,
    operationId: `${group}/${leaf}`,
    group,
    path: facts.path,
    method: facts.httpMethod,
    summary: facts.description,
    description: facts.description,
    deprecated: false,
    ...(docsUrl === undefined ? {} : { docsUrl }),
    scopes: facts.scopes,
    security: buildSecurity(facts.scopes, config, 'any'),
    ...(facts.rateLimitTier === undefined ? {} : { rateLimitTier: facts.rateLimitTier }),
    parameters: facts.parameters,
    response,
    warnings,
  }
}

/**
 * Split a method into the group and leaf Coral derives SQL names from.
 *
 * The importer splits `operationId` on its slash, so only the first dot marks a
 * group: `admin.apps.approve` is one `admin` relation named `apps_approve`,
 * not a nested namespace.
 */
export function splitMethod(method: string): { group: string; leaf: string } {
  const [group = method, ...rest] = method.split('.')
  return { group, leaf: rest.length === 0 ? group : rest.join('_') }
}

/**
 * Name the object schemas that will become components.
 *
 * Slack envelopes hold their rows in an array named after the resource, so the
 * singular of that name is the natural component name — and a shared component
 * also gives Coral a better entity name than it would derive from the path.
 */
export function nameRowComponents(schema: SchemaNode): SchemaNode {
  if (schema.kind !== 'object') {
    return schema
  }
  const properties: Record<string, SchemaNode> = {}
  for (const [key, property] of Object.entries(schema.properties)) {
    if (property.kind === 'array' && property.items.kind === 'object') {
      const items: ObjectNode = { ...property.items, component: pascalCase(singularize(key)) }
      properties[key] = { ...property, items }
      continue
    }
    properties[key] = property
  }
  return { ...schema, properties }
}

export function singularize(word: string): string {
  if (word.endsWith('ies') && word.length > 3) {
    return `${word.slice(0, -3)}y`
  }
  if (/(ch|sh|ss|x|z)es$/.test(word)) {
    return word.slice(0, -2)
  }
  if (word.endsWith('s') && !word.endsWith('ss')) {
    return word.slice(0, -1)
  }
  return word
}

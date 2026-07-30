/**
 * Assembling the Slack {@link ApiModel} from the pinned snapshot.
 *
 * Reads only the snapshot, never the network, so a build is reproducible from
 * what is committed.
 */

import type { ApiModel, ObjectNode, Operation, SchemaNode } from '../../core/model.ts'
import type { ForgeConfig } from '../../core/config.ts'
import type { Snapshot } from '../../core/snapshot.ts'
import { inferSchema } from '../../core/infer.ts'
import { pascalCase } from '../../core/emit.ts'
import { parseDocsPage, parseSourceUrl } from './docs.ts'

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

  const operations: Operation[] = []
  for (const entry of entries) {
    if (!configured.has(entry.method.toLowerCase())) {
      continue
    }
    operations.push(await buildOperation(entry, config, snapshot))
  }

  return {
    api: config.api,
    title: config.title,
    description: config.description,
    serverUrl: config.serverUrl,
    operations: operations.toSorted((left, right) => left.id.localeCompare(right.id)),
    warnings,
  }
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

async function buildOperation(
  entry: SnapshotEntry,
  config: ForgeConfig,
  snapshot: Snapshot,
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

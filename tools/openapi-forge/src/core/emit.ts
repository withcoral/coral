/**
 * Emitting OpenAPI 3.0.3 from an {@link ApiModel}.
 *
 * The output has one consumer, Coral's DSL v4 importer, and that consumer is
 * strict in ways a general OpenAPI writer is not. The constraints below are
 * checked here rather than trusted, because every one of them fails quietly:
 * a rejected schema does not error, it produces a relation with a single opaque
 * column, which looks like a modelling choice rather than a bug.
 *
 * - Only OpenAPI `3.0.x` is accepted; 3.1 and 2.0 are rejected outright.
 * - Only local `#/` references resolve; external ones are dropped.
 * - `allOf`, `oneOf`, `anyOf` and `not` either break schema import or abort
 *   row-path inference. None is ever emitted.
 * - Operation identifiers must stay unique once normalized, since a collision
 *   is a hard error at import.
 */

import { stringify } from 'yaml'

import type { ApiModel, Operation, Parameter, SchemaNode } from './model.ts'

export const OPENAPI_VERSION = '3.0.3'

export class EmitError extends Error {}

interface JsonObject {
  [key: string]: unknown
}

export interface EmitOptions {
  /** Written to `info.version`; the snapshot date, so it tracks the inputs. */
  version: string
}

export function emitOpenApi(model: ApiModel, options: EmitOptions): string {
  const document = buildDocument(model, options)
  assertImportable(document)
  // Long descriptions are kept on one line: folding them would be a lossy
  // rewrite of prose that ends up in the SQL catalog.
  return stringify(document, { lineWidth: 0 })
}

function buildDocument(model: ApiModel, options: EmitOptions): JsonObject {
  const components = new ComponentRegistry()
  const paths: JsonObject = {}

  for (const operation of [...model.operations].toSorted((left, right) =>
    left.path.localeCompare(right.path),
  )) {
    const existing = paths[operation.path]
    const item: JsonObject = isJsonObject(existing) ? existing : {}
    if (item[operation.method] !== undefined) {
      throw new EmitError(
        `two operations claim ${operation.method.toUpperCase()} ${operation.path}`,
      )
    }
    item[operation.method] = buildOperation(operation, components)
    paths[operation.path] = item
  }

  const tags = [...new Set(model.operations.map((operation) => operation.group))]
    .toSorted((left, right) => left.localeCompare(right))
    .map((name) => ({ name }))

  const document: JsonObject = {
    openapi: OPENAPI_VERSION,
    info: {
      title: model.title,
      description: model.description,
      version: options.version,
    },
    servers: [{ url: model.serverUrl }],
    tags,
    paths,
  }
  const schemas = components.schemas()
  if (Object.keys(schemas).length > 0) {
    document.components = { schemas }
  }
  return document
}

function buildOperation(operation: Operation, components: ComponentRegistry): JsonObject {
  const emitted: JsonObject = {
    operationId: operation.operationId,
    tags: [operation.group],
    summary: operation.summary,
    description: buildDescription(operation),
  }
  if (operation.deprecated) {
    emitted.deprecated = true
  }
  if (operation.parameters.length > 0) {
    emitted.parameters = operation.parameters.map((parameter) => buildParameter(parameter))
  }
  emitted.responses = {
    '200': {
      description: 'Response envelope. Slack reports failures here with `ok: false`.',
      content: {
        'application/json': {
          schema: buildSchema(operation.response, components, operation),
        },
      },
    },
  }
  return emitted
}

/**
 * Prose plus the facts Coral has nowhere else to show.
 *
 * Scopes and rate-limit tier are not part of the descriptor's contract — Coral
 * ignores `securitySchemes` entirely and takes auth from the manifest — but
 * they are the two things a user hitting `missing_scope` or `ratelimited`
 * needs, and the operation description is where they will look.
 */
function buildDescription(operation: Operation): string {
  const lines = [operation.description]
  const scopes = operation.scopes
  if (scopes !== undefined && (scopes.bot.length > 0 || scopes.user.length > 0)) {
    const required = [...new Set([...scopes.bot, ...scopes.user])].toSorted((left, right) =>
      left.localeCompare(right),
    )
    lines.push(`Requires one of the scopes: ${required.join(', ')}.`)
  }
  if (operation.rateLimitTier !== undefined) {
    // Most tiers read `Tier 3: 50+ per minute`, but a few are a full sentence
    // that already ends in a full stop.
    lines.push(`Rate limit: ${operation.rateLimitTier.replace(/\.$/, '')}.`)
  }
  if (operation.docsUrl !== undefined) {
    lines.push(`See ${operation.docsUrl}.`)
  }
  return lines.filter((line) => line !== '').join(' ')
}

function buildParameter(parameter: Parameter): JsonObject {
  const schema: JsonObject = { type: parameter.schema.type }
  if (parameter.schema.enum !== undefined) {
    schema.enum = parameter.schema.enum
  }
  if (parameter.default !== undefined) {
    schema.default = parameter.default
  }
  const emitted: JsonObject = {
    name: parameter.name,
    in: parameter.in,
    required: parameter.required,
    schema,
  }
  if (parameter.description !== '') {
    emitted.description = parameter.description
  }
  if (parameter.example !== undefined) {
    emitted.example = parameter.example
  }
  return emitted
}

function buildSchema(
  node: SchemaNode,
  components: ComponentRegistry,
  operation: Operation,
): JsonObject {
  switch (node.kind) {
    case 'scalar': {
      const schema: JsonObject = { type: node.type }
      if (node.format !== undefined) {
        schema.format = node.format
      }
      if (node.enum !== undefined) {
        schema.enum = node.enum
      }
      return withDescription(schema, node.description)
    }
    case 'array':
      return withDescription(
        { type: 'array', items: buildSchema(node.items, components, operation) },
        node.description,
      )
    case 'object': {
      const properties: JsonObject = {}
      for (const key of Object.keys(node.properties).toSorted((left, right) =>
        left.localeCompare(right),
      )) {
        const property = node.properties[key]
        if (property !== undefined) {
          properties[key] = buildSchema(property, components, operation)
        }
      }
      // An object with no described properties is emitted bare, which Coral
      // reads as an opaque JSON value.
      const schema: JsonObject = withDescription(
        Object.keys(properties).length === 0 ? { type: 'object' } : { type: 'object', properties },
        node.description,
      )
      if (node.component === undefined || Object.keys(properties).length === 0) {
        return schema
      }
      return { $ref: components.register(node.component, schema, operation.group) }
    }
    case 'unknown':
      // Asserting no type at all is the honest description of a value whose
      // type could not be determined. Coral reads it as JSON either way.
      return withDescription({}, node.description)
  }
}

function withDescription(schema: JsonObject, description: string | undefined): JsonObject {
  if (description === undefined || description === '') {
    return schema
  }
  return { ...schema, description }
}

/**
 * Hoists named object schemas into `components/schemas`.
 *
 * Two operations that return the same shape share one component, which keeps
 * the descriptor small and gives Coral a better entity name than it derives
 * from a path. Two that only *want* the same name are disambiguated rather than
 * merged: silently unifying them would give one of them the wrong columns.
 */
class ComponentRegistry {
  readonly #byName = new Map<string, string>()
  readonly #byFingerprint = new Map<string, string>()
  readonly #schemas = new Map<string, JsonObject>()

  register(preferredName: string, schema: JsonObject, groupHint: string): string {
    const fingerprint = JSON.stringify(schema)
    const existing = this.#byFingerprint.get(fingerprint)
    if (existing !== undefined) {
      return reference(existing)
    }

    let name = preferredName
    if (this.#byName.has(name)) {
      name = `${pascalCase(groupHint)}${preferredName}`
    }
    for (let suffix = 2; this.#byName.has(name); suffix += 1) {
      name = `${pascalCase(groupHint)}${preferredName}${suffix}`
    }

    this.#byName.set(name, fingerprint)
    this.#byFingerprint.set(fingerprint, name)
    this.#schemas.set(name, schema)
    return reference(name)
  }

  schemas(): JsonObject {
    const ordered: JsonObject = {}
    for (const name of [...this.#schemas.keys()].toSorted((left, right) =>
      left.localeCompare(right),
    )) {
      const schema = this.#schemas.get(name)
      if (schema !== undefined) {
        ordered[name] = schema
      }
    }
    return ordered
  }
}

function reference(name: string): string {
  return `#/components/schemas/${name}`
}

export function pascalCase(value: string): string {
  return value
    .split(/[^A-Za-z0-9]+/)
    .filter((part) => part !== '')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join('')
}

/**
 * Verify the document against everything Coral's importer requires.
 *
 * @throws EmitError describing the first violation found.
 */
export function assertImportable(document: JsonObject): void {
  const version = document.openapi
  if (version !== OPENAPI_VERSION) {
    throw new EmitError(`openapi must be ${OPENAPI_VERSION}, got '${String(version)}'`)
  }

  const references: string[] = []
  walk(document, (node, path) => {
    for (const keyword of ['allOf', 'oneOf', 'anyOf', 'not']) {
      if (node[keyword] !== undefined) {
        throw new EmitError(
          `${path} uses '${keyword}', which Coral cannot import and which aborts row-path inference`,
        )
      }
    }
    const ref = node.$ref
    if (typeof ref === 'string') {
      if (!ref.startsWith('#/')) {
        throw new EmitError(`${path} uses external reference '${ref}'`)
      }
      references.push(ref)
    }
  })

  for (const ref of references) {
    if (resolve(document, ref) === undefined) {
      throw new EmitError(`reference '${ref}' does not resolve`)
    }
  }

  assertUniqueOperationIds(document)
}

function assertUniqueOperationIds(document: JsonObject): void {
  const seen = new Map<string, string>()
  const paths = document.paths
  if (!isJsonObject(paths)) {
    throw new EmitError('document has no paths')
  }
  for (const item of Object.values(paths)) {
    if (!isJsonObject(item)) {
      continue
    }
    for (const operation of Object.values(item)) {
      if (!isJsonObject(operation)) {
        continue
      }
      const id = operation.operationId
      if (typeof id !== 'string') {
        continue
      }
      // An approximation of Coral's identifier normalization. The importer is
      // the real authority and rejects collisions itself; this only catches
      // them earlier, where the message can name the generator.
      const normalized = id.toLowerCase().replaceAll(/[^a-z0-9]+/g, '_')
      const previous = seen.get(normalized)
      if (previous !== undefined) {
        throw new EmitError(`operationIds '${previous}' and '${id}' collide once normalized`)
      }
      seen.set(normalized, id)
    }
  }
}

function resolve(document: JsonObject, ref: string): unknown {
  let current: unknown = document
  for (const segment of ref.slice(2).split('/')) {
    if (!isJsonObject(current)) {
      return undefined
    }
    current = current[decodeURIComponent(segment.replaceAll('~1', '/').replaceAll('~0', '~'))]
  }
  return current
}

function walk(node: unknown, visit: (node: JsonObject, path: string) => void, path = '$'): void {
  if (Array.isArray(node)) {
    for (const [index, element] of node.entries()) {
      walk(element, visit, `${path}[${index}]`)
    }
    return
  }
  if (!isJsonObject(node)) {
    return
  }
  visit(node, path)
  for (const [key, value] of Object.entries(node)) {
    walk(value, visit, `${path}.${key}`)
  }
}

function isJsonObject(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

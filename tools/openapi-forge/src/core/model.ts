/**
 * `ApiModel` — the vendor-neutral description of an HTTP API.
 *
 * This is the pipeline's hinge. Adapters know how to read one provider's docs
 * and samples and produce an `ApiModel`; the emitter knows how to turn an
 * `ApiModel` into OpenAPI 3.0.3. Neither knows about the other, so supporting a
 * second API means writing a second adapter and nothing else.
 *
 * Two shaping decisions are deliberate:
 *
 * 1. Parameters carry only scalar schemas. Coral's v4 importer drops any
 *    parameter whose schema is not scalar, so an adapter that produced richer
 *    parameter types would only be producing detail that gets discarded.
 *
 * 2. `SchemaNode` is a small subset of JSON Schema rather than the real thing.
 *    It has no composition keywords, because `allOf`/`oneOf`/`anyOf` either
 *    break Coral's schema import or abort its row-path inference outright. A
 *    type that cannot express them cannot accidentally emit them.
 */

/** HTTP methods an operation may use. */
export type HttpMethod = 'get' | 'post' | 'put' | 'patch' | 'delete' | 'head' | 'options'

/** Scalar JSON types Coral maps onto SQL column types. */
export type ScalarType = 'string' | 'integer' | 'number' | 'boolean'

/**
 * A depth-limited JSON Schema subset.
 *
 * `unknown` is the honest representation of a value whose type could not be
 * determined — a field observed as a string in one sample and a number in
 * another, say. It emits a schema that asserts nothing, which Coral reads as a
 * JSON column. Claiming a type there would be worse than claiming none.
 */
export type SchemaNode = ScalarNode | ObjectNode | ArrayNode | UnknownNode

export interface ScalarNode {
  kind: 'scalar'
  type: ScalarType
  description?: string
  /** Emitted as an OpenAPI `format`; only `date-time` changes Coral's typing. */
  format?: 'date-time'
  enum?: string[]
}

export interface ObjectNode {
  kind: 'object'
  /** Empty for an object whose shape is unknown; Coral reads that as JSON. */
  properties: Record<string, SchemaNode>
  description?: string
  /**
   * When set, the emitter hoists this object into `components/schemas` under
   * this name and references it. Structurally identical objects that share a
   * name are emitted once.
   */
  component?: string
}

export interface ArrayNode {
  kind: 'array'
  items: SchemaNode
  description?: string
}

export interface UnknownNode {
  kind: 'unknown'
  description?: string
}

/** One request parameter. Only scalars survive Coral's importer. */
export interface Parameter {
  name: string
  in: 'query' | 'path'
  required: boolean
  description: string
  schema: ScalarNode
  /**
   * The provider's documented default.
   *
   * Worth care on page-size parameters: Coral derives a page-size maximum of
   * `max(declaredDefault, 100)`, so this value decides how many rows a single
   * request may fetch.
   */
  default?: string | number | boolean
  example?: string | number | boolean
}

/** Provider-documented auth scopes. Informational — Coral ignores them. */
export interface Scopes {
  bot: string[]
  user: string[]
}

export interface Operation {
  /** The provider's own identifier, e.g. `conversations.history`. */
  id: string
  /**
   * Emitted as the OpenAPI `operationId` in `group/leaf` form, e.g.
   * `conversations/history`. Coral splits on the slash to derive the SQL group
   * and relation name, so the shape matters.
   */
  operationId: string
  /** Emitted as the sole OpenAPI tag; Coral uses it as the projection group. */
  group: string
  /** Path relative to {@link ApiModel.serverUrl}, e.g. `/conversations.history`. */
  path: string
  method: HttpMethod
  summary: string
  description: string
  deprecated: boolean
  /** Where this operation was extracted from, for provenance. */
  docsUrl?: string
  scopes?: Scopes
  /** The provider's rate-limit tier label, if it publishes one. */
  rateLimitTier?: string
  parameters: Parameter[]
  /** Schema of the whole response body, envelope included. */
  response: SchemaNode
  /** Extraction problems a human should look at. Never silently dropped. */
  warnings: string[]
}

export interface ApiModel {
  /** Stable API identifier, matching the directory under `apis/`. */
  api: string
  title: string
  description: string
  /** Absolute URL every operation path is relative to. */
  serverUrl: string
  operations: Operation[]
  /** Problems that are not specific to one operation. */
  warnings: string[]
}

/** Every warning in the model, flattened for reporting. */
export function collectWarnings(model: ApiModel): string[] {
  return [
    ...model.warnings,
    ...model.operations.flatMap((operation) =>
      operation.warnings.map((warning) => `${operation.id}: ${warning}`),
    ),
  ]
}

// Writes recipes/gmail/openapi.json from Google's Gmail Discovery document.
// Google publishes Discovery documents rather than OpenAPI, so the Gmail recipe
// bundles this conversion. It is mechanical: every method, parameter and schema
// is carried over unchanged apart from what OpenAPI and Coral's importer need:
// - Discovery gives no status codes, so each method's response becomes `2XX`.
// - Coral requires every tool to share one credential, while Discovery
//   lists the scopes each method accepts. The per-method lists become one root
//   requirement: the smallest scope set that satisfies every method, which the
//   script fails on if it is not unique.
// - Discovery omits OAuth endpoints, so they come from Google's OpenID
//   configuration.
// Discovery's media-upload protocols (the /upload/ endpoints) are alternative
// transports for six methods, not methods of their own, and are not emitted.
// Nor are the global parameters a caller must not set: those that carry a
// credential, since Coral sends the account's own; those that make the response
// something other than JSON; and the upload protocol, which only /upload/ uses.
// Discovery serves its keys in a different order on each request, so the file
// sorts them and changes only when the API does.

import { writeFile } from 'node:fs/promises'

const DISCOVERY_URL = 'https://gmail.googleapis.com/$discovery/rest?version=v1'
const OPENID_CONFIGURATION_URL = 'https://accounts.google.com/.well-known/openid-configuration'
const OMITTED_PARAMETERS = new Set([
  // Credentials
  'access_token',
  'oauth_token',
  'key',
  // Protobuf and JSONP responses
  'alt',
  'callback',
  // Media upload
  'uploadType',
  'upload_protocol',
])
// Any other key means Discovery grew a construct this conversion does not handle yet.
const SCHEMA_KEYS = new Set([
  'id',
  'type',
  'format',
  'description',
  'properties',
  'items',
  'additionalProperties',
  '$ref',
  'enum',
  'enumDescriptions',
  'readOnly',
  'deprecated',
  'annotations',
])
const PARAMETER_KEYS = new Set([
  'location',
  'type',
  'format',
  'description',
  'default',
  'required',
  'repeated',
  'enum',
  'enumDescriptions',
])
const METHOD_KEYS = new Set([
  'id',
  'path',
  'flatPath',
  'httpMethod',
  'description',
  'parameters',
  'parameterOrder',
  'request',
  'response',
  'scopes',
  'supportsMediaUpload',
  'mediaUpload',
])

type Schema = { [key: string]: unknown }

interface Parameter {
  location: string
  type: string
  format?: string
  description?: string
  default?: string
  required?: boolean
  repeated?: boolean
  enum?: string[]
  enumDescriptions?: string[]
}

interface Method {
  id: string
  path: string
  flatPath: string
  httpMethod: string
  description: string
  parameters: Record<string, Parameter>
  parameterOrder: string[]
  request?: { $ref: string }
  response?: { $ref: string }
  scopes: string[]
}

interface Resource {
  methods?: Record<string, Method>
  resources?: Record<string, Resource>
}

interface Discovery {
  title: string
  version: string
  description: string
  revision: string
  documentationLink: string
  rootUrl: string
  servicePath: string
  parameters: Record<string, Parameter>
  resources: Record<string, Resource>
  schemas: Record<string, Schema>
  auth: { oauth2: { scopes: Record<string, { description: string }> } }
}

const byName = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0)

/** Rewrites every object with sorted keys; arrays keep their order. */
function stable(_key: string, value: unknown) {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return value
  return Object.fromEntries(Object.entries(value).toSorted(([a], [b]) => byName(a, b)))
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url, { signal: AbortSignal.timeout(30_000) })
  if (!response.ok) throw new Error(`${url} answered ${response.status}`)
  return (await response.json()) as T
}

function* methods(resources: Record<string, Resource>): Generator<Method> {
  for (const resource of Object.values(resources)) {
    yield* Object.values(resource.methods ?? {})
    yield* methods(resource.resources ?? {})
  }
}

function unexpected(kind: string, node: object, allowed: Set<string>) {
  const keys = Object.keys(node).filter((key) => !allowed.has(key))
  if (keys.length > 0) throw new Error(`Unhandled Discovery ${kind} keys: ${keys.toSorted()}`)
}

// Discovery schemas are already JSON Schema apart from bare-name references and
// `id`, which the component name replaces.
function schema(node: Schema): Schema {
  unexpected('schema', node, SCHEMA_KEYS)
  const converted: Schema = {}
  for (const [key, value] of Object.entries(node)) {
    if (key === '$ref') converted[key] = `#/components/schemas/${String(value)}`
    else if (key === 'properties')
      converted[key] = Object.fromEntries(
        Object.entries(value as Record<string, Schema>).map(([name, child]) => [
          name,
          schema(child),
        ]),
      )
    else if ((key === 'items' || key === 'additionalProperties') && typeof value === 'object')
      converted[key] = schema(value as Schema)
    else if (key !== 'id') converted[key] = value
  }
  return converted
}

function parameter(name: string, spec: Parameter) {
  unexpected('parameter', spec, PARAMETER_KEYS)
  let value: Schema = {}
  for (const key of ['type', 'format', 'enum', 'enumDescriptions'] as const)
    if (spec[key] !== undefined) value[key] = spec[key]
  // Discovery writes every default as a string; JSON Schema wants it in the parameter's type.
  if (spec.default !== undefined)
    value.default =
      spec.type === 'boolean'
        ? spec.default === 'true'
        : spec.type === 'integer'
          ? Number.parseInt(spec.default, 10)
          : spec.default
  if (spec.repeated) value = { type: 'array', items: value }
  return {
    name,
    in: spec.location,
    ...(spec.description === undefined ? {} : { description: spec.description }),
    ...(spec.required ? { required: true } : {}),
    schema: value,
  }
}

function* combinations<T>(items: readonly T[], size: number, start = 0): Generator<T[]> {
  if (size === 0) {
    yield []
    return
  }
  for (let index = start; index <= items.length - size; index++)
    for (const rest of combinations(items, size - 1, index + 1)) yield [items[index], ...rest]
}

const discovery = await fetchJson<Discovery>(DISCOVERY_URL)
const openid = await fetchJson<{ authorization_endpoint: string; token_endpoint: string }>(
  OPENID_CONFIGURATION_URL,
)
const everyMethod = [...methods(discovery.resources)]

// OpenAPI component names allow only [A-Za-z0-9._-], which `$.xgafv` is not.
const globalParameters = new Map(
  Object.entries(discovery.parameters)
    .filter(([name]) => !OMITTED_PARAMETERS.has(name))
    .toSorted(([a], [b]) => byName(a, b))
    .map(([name, spec]) => [name.replaceAll(/[^A-Za-z0-9._-]/g, '_'), { name, spec }]),
)
const globalNames = new Set([...globalParameters.values()].map(({ name }) => name))

const paths: Record<string, Record<string, unknown>> = {}
for (const method of everyMethod) {
  unexpected('method', method, METHOD_KEYS)
  if (method.path !== method.flatPath || method.path.includes('{+'))
    throw new Error(`${method.id} uses a reserved-expansion path, which Coral cannot bind`)
  const names = [
    ...method.parameterOrder,
    ...Object.keys(method.parameters)
      .filter((name) => !method.parameterOrder.includes(name))
      .toSorted(byName),
  ]
  if (names.some((name) => globalNames.has(name) || name === 'body'))
    throw new Error(`${method.id} has a parameter that collides with another input`)
  const response = {
    description: 'Successful response',
    ...(method.response && {
      content: {
        'application/json': { schema: { $ref: `#/components/schemas/${method.response.$ref}` } },
      },
    }),
  }
  paths[`/${method.path}`] ??= {}
  paths[`/${method.path}`][method.httpMethod.toLowerCase()] = {
    operationId: method.id,
    description: method.description,
    parameters: [
      ...names.map((name) => parameter(name, method.parameters[name])),
      ...[...globalParameters.keys()].map((key) => ({ $ref: `#/components/parameters/${key}` })),
    ],
    ...(method.request && {
      requestBody: {
        content: {
          'application/json': { schema: { $ref: `#/components/schemas/${method.request.$ref}` } },
        },
      },
    }),
    responses: { '2XX': response },
  }
}

const declaredScopes = discovery.auth.oauth2.scopes
const accepted = everyMethod.map((method) => new Set(method.scopes))
let covers: string[][] = []
for (let size = 1; size <= Object.keys(declaredScopes).length && covers.length === 0; size++) {
  covers = [...combinations(Object.keys(declaredScopes).toSorted(), size)].filter((combination) =>
    accepted.every((scopes) => combination.some((scope) => scopes.has(scope))),
  )
}
if (covers.length !== 1)
  throw new Error(`No unique smallest scope set satisfies every method: ${JSON.stringify(covers)}`)

const document = {
  openapi: '3.1.0',
  info: {
    title: discovery.title,
    version: discovery.version,
    description: discovery.description,
    'x-google-discovery-revision': discovery.revision,
  },
  externalDocs: { url: discovery.documentationLink },
  servers: [{ url: (discovery.rootUrl + discovery.servicePath).replace(/\/+$/, '') }],
  security: [{ oauth2: covers[0] }],
  paths,
  components: {
    schemas: Object.fromEntries(
      Object.entries(discovery.schemas).map(([name, node]) => [name, schema(node)]),
    ),
    parameters: Object.fromEntries(
      [...globalParameters].map(([key, { name, spec }]) => [key, parameter(name, spec)]),
    ),
    securitySchemes: {
      oauth2: {
        type: 'oauth2',
        flows: {
          authorizationCode: {
            authorizationUrl: openid.authorization_endpoint,
            tokenUrl: openid.token_endpoint,
            scopes: Object.fromEntries(
              Object.entries(declaredScopes).map(([scope, { description }]) => [
                scope,
                description,
              ]),
            ),
          },
        },
      },
    },
  },
}

const target = new URL('../recipes/gmail/openapi.json', import.meta.url)
await writeFile(target, `${JSON.stringify(document, stable, 2)}\n`)
console.log(
  `Wrote ${everyMethod.length} tools from Discovery revision ${discovery.revision} to ${target.pathname}`,
)

// Convert an entire official Google Discovery document to OpenAPI 3.1.
// Run: node scripts/google-discovery-openapi.ts <discovery-url> <recipe-id> [api-key-header]
// Carry every method, schema and documented JSON parameter into the snapshot.
// flatPath replaces reserved resource expansions with documented path segments;
// original paths and parameters remain in x-google-discovery-* annotations.
// Discovery has no status codes: successful responses use 2XX. Media transports
// are alternative upload/download protocols, not additional JSON API methods.
// Credential/format parameters are omitted because Coral owns these concerns.
// Google OAuth endpoints come from its public OpenID configuration.
// Fail on unknown Discovery constructs instead of silently dropping them.

import { writeFile } from 'node:fs/promises'

const [DISCOVERY_URL, RECIPE_ID, API_KEY_HEADER] = process.argv.slice(2)
if (!DISCOVERY_URL || !RECIPE_ID || !/^[a-z][a-z0-9_]*$/.test(RECIPE_ID))
  throw new Error(
    'Usage: node scripts/google-discovery-openapi.ts <discovery-url> <recipe-id> [api-key-header]',
  )
if (API_KEY_HEADER && !/^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/.test(API_KEY_HEADER))
  throw new Error('API key header must be a valid HTTP header name')
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
  'enumDeprecated',
  'readOnly',
  'deprecated',
  'annotations',
  'required',
  'minimum',
  'maximum',
  'pattern',
  'default',
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
  'enumDeprecated',
  'minimum',
  'maximum',
  'pattern',
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
  'supportsMediaDownload',
  'useMediaDownloadService',
  'mediaDownload',
  'deprecated',
  'annotations',
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
  enumDeprecated?: boolean[]
  minimum?: string | number
  maximum?: string | number
  pattern?: string
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
  deprecated?: boolean
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
  auth?: { oauth2: { scopes: Record<string, { description: string }> } }
  methods?: Record<string, Method>
}

const byName = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0)

/** Rewrites every object with sorted keys; arrays keep their order. */
function stable(_key: string, value: unknown) {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return value
  return Object.fromEntries(Object.entries(value).toSorted(([a], [b]) => byName(a, b)))
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url, { redirect: 'error', signal: AbortSignal.timeout(30_000) })
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
    if (key === 'type' && value === 'any') continue
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
  for (const key of [
    'type',
    'format',
    'enum',
    'enumDescriptions',
    'enumDeprecated',
    'pattern',
  ] as const)
    if (spec[key] !== undefined) value[key] = spec[key]
  for (const key of ['minimum', 'maximum'] as const) {
    if (spec[key] === undefined) continue
    const constraint = Number(spec[key])
    if (!Number.isFinite(constraint)) throw new Error(`Invalid Discovery ${key} for ${name}`)
    value[key] = constraint
  }
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

const discovery = await fetchJson<Discovery>(DISCOVERY_URL)
if (API_KEY_HEADER && !discovery.parameters.key)
  throw new Error('API key header requires a Discovery key credential parameter')
const openid = await fetchJson<{ authorization_endpoint: string; token_endpoint: string }>(
  OPENID_CONFIGURATION_URL,
)
const everyMethod = [
  ...Object.values(discovery.methods ?? {}),
  ...methods(discovery.resources ?? {}),
]

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
  const path = method.flatPath ?? method.path
  if (path.includes('{+')) throw new Error(`${method.id} has no segment-expanded flatPath`)
  const segments = [...path.matchAll(/\{([^}]+)\}/g)].map((match) => match[1])
  const parameters = Object.fromEntries(
    Object.entries(method.parameters ?? {}).filter(([, spec]) => spec.location !== 'path'),
  )
  for (const name of segments) {
    const original = method.parameters?.[name]
    parameters[name] = original ?? {
      location: 'path',
      type: 'string',
      required: true,
      description: `Resource path segment ${name}. See the original resource pattern in the tool description.`,
    }
  }
  const names = Object.keys(parameters).toSorted(byName)
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
  paths[`/${path}`] ??= {}
  paths[`/${path}`][method.httpMethod.toLowerCase()] = {
    operationId: method.id,
    ...(Object.hasOwn(method, 'deprecated') && { deprecated: method.deprecated }),
    description: method.description,
    'x-google-discovery-path': method.path,
    ...(method.scopes && { 'x-google-discovery-scopes': method.scopes }),
    ...(method.path !== path && { 'x-google-discovery-parameters': method.parameters }),
    parameters: [
      ...names.map((name) => parameter(name, parameters[name])),
      ...[...globalParameters.keys()].map((key) => ({ $ref: `#/components/parameters/${key}` })),
    ],
    ...(method.request && {
      requestBody: {
        required: true,
        content: {
          'application/json': { schema: { $ref: `#/components/schemas/${method.request.$ref}` } },
        },
      },
    }),
    ...(method.scopes && {
      security: [
        // Discovery lists alternative grants. Each one can authorize this method.
        ...method.scopes.map((scope) => ({ oauth2: [scope] })),
        ...(API_KEY_HEADER ? [{ apiKey: [] }] : []),
      ],
    }),
    responses: { '2XX': response },
  }
}

// Preserve every advertised scope as a choice, not a combined required grant.
// Some Business Profile Discovery documents omit OAuth metadata; their official
// auth guide defines business.manage. Keep that documented addition explicit.
const declaredScopes =
  discovery.auth?.oauth2.scopes ??
  (RECIPE_ID.startsWith('google_business_')
    ? {
        'https://www.googleapis.com/auth/business.manage': {
          description: 'Manage business accounts and locations.',
        },
      }
    : {})
const document = {
  openapi: '3.1.0',
  info: {
    title: discovery.title,
    version: discovery.version,
    description: discovery.description,
    'x-google-discovery-revision': discovery.revision,
    'x-google-discovery-url': DISCOVERY_URL,
  },
  externalDocs: { url: discovery.documentationLink },
  servers: [{ url: (discovery.rootUrl + discovery.servicePath).replace(/\/+$/, '') }],
  security: [
    {
      oauth2: RECIPE_ID.startsWith('google_business_')
        ? Object.keys(declaredScopes).toSorted()
        : [],
    },
    ...(API_KEY_HEADER ? [{ apiKey: [] }] : []),
  ],
  paths,
  components: {
    schemas: Object.fromEntries(
      Object.entries(discovery.schemas).map(([name, node]) => [name, schema(node)]),
    ),
    parameters: Object.fromEntries(
      [...globalParameters].map(([key, { name, spec }]) => [key, parameter(name, spec)]),
    ),
    securitySchemes: {
      // Discovery's global key parameter identifies the API-key credential.
      // The optional header spelling must come from the API's official docs.
      ...(API_KEY_HEADER && {
        apiKey: { type: 'apiKey', in: 'header', name: API_KEY_HEADER },
      }),
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

const target = new URL(`../recipes/${RECIPE_ID}/openapi.json`, import.meta.url)
await writeFile(target, `${JSON.stringify(document, stable, 2)}\n`)
console.log(
  `Wrote ${everyMethod.length} tools from Discovery revision ${discovery.revision} to ${target.pathname}`,
)

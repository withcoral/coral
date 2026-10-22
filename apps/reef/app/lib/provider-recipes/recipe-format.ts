// The Provider Recipe file format: the YAML files in apps/reef/recipes/. This
// schema is the only definition of the format. Types come from it, every object
// rejects unknown fields, and it is kept apart from Reef's form state and the
// management API types (ADR 0004).
//
// Rules use zod checks that also appear in the generated JSON Schema (regex,
// length), so an editor enforces them too. zod has no such check for entry
// counts, unique items or excluded names, so those helpers pair a refinement
// with metadata taken from the same values. Whether a URL parses, and rules
// that depend on another field, stay parse-time only.

import { z } from 'zod'

// Keep the .ts extension: scripts/recipe-schema.ts runs this file under plain node.
import { inputReference } from './input-reference.ts'

/** Entry limits for a map, checked when parsing and written into the JSON Schema. */
function withEntries<T extends z.ZodType<Record<string, unknown>>>(
  schema: T,
  { min = 0, max = Infinity }: { min?: number; max?: number },
  message: string,
): T {
  return schema
    .refine((value: Record<string, unknown>) => {
      const count = Object.keys(value).length
      return count >= min && count <= max
    }, message)
    .meta({
      ...(min > 0 && { minProperties: min }),
      ...(max < Infinity && { maxProperties: max }),
    })
}

/** Distinct items, checked when parsing and written into the JSON Schema. */
function withUniqueItems<T extends z.ZodType<readonly string[]>>(schema: T, message: string): T {
  return schema
    .refine((items: readonly string[]) => new Set(items).size === items.length, message)
    .meta({ uniqueItems: true })
}

/** Any value except these names, checked when parsing and written into the JSON Schema. */
function except<T extends z.ZodType<string>>(
  schema: T,
  names: readonly string[],
  message: string,
): T {
  return schema
    .refine((value: string) => !names.includes(value), message)
    .meta({ not: { enum: [...names] } })
}

// Written with plain escapes, not \p{Cc}, so every JSON Schema validator can compile it.
// oxlint-disable-next-line no-control-regex
const noControlCharacters = /^[^\x00-\x1f\x7f]*$/
const noControlCharactersMessage = 'must not contain control characters'
const noBraces = /^[^{}]*$/
const reference = inputReference.source

const name = z.string().regex(/^[a-z][a-z0-9_]{0,63}$/, 'must be a lowercase name')
const label = z.string().min(1).max(120).regex(noControlCharacters, noControlCharactersMessage)
const description = z.string().max(1000)

/** A plain value: no templates and no control characters. */
const literal = z
  .string()
  .min(1)
  .max(2048)
  .regex(noBraces, 'must not contain braces')
  .regex(noControlCharacters, noControlCharactersMessage)

const protocolLiteral = z
  .string()
  .max(2048)
  .regex(noBraces, 'must not contain braces')
  .regex(noControlCharacters, noControlCharactersMessage)
  .describe(
    'A non-secret protocol constant, never a credential or account id. Empty is allowed, as for a Basic password.',
  )

// Not z.url(): it trims the value and strips tabs and newlines before the checks after it.
const url = literal
  .regex(/^https?:\/\//, 'must be an http or https URL')
  .refine(URL.canParse, 'must be a valid URL')
  .describe('An http or https URL. It cannot reference inputs.')

const textTemplate = z
  .string()
  .min(1)
  .max(2048)
  .regex(new RegExp(`^(?:[^{}]|${reference})*$`), 'may only reference inputs as {{input.name}}')
  .regex(noControlCharacters, noControlCharactersMessage)
  .describe(
    'Literal text plus {{input.name}} references, and nothing else. Each reference must name a declared input.',
  )

const urlTemplate = z
  .union([
    url,
    textTemplate
      .regex(new RegExp(`^(?:https?://|${reference})`), 'must start with http(s):// or an input')
      .regex(new RegExp(reference), 'must be a URL or reference an input'),
  ])
  .describe(
    'A URL that may reference inputs as {{input.name}}. Coral fills in the inputs before it checks or fetches the URL.',
  )

const headerName = z
  .string()
  .min(1)
  .max(256)
  .regex(/^[a-z0-9!#$%&'*+.^_`|~-]+$/, 'must be a lowercase header name')
  .describe('A lowercase HTTP header name.')

const commonHostOwnedHeaders = [
  'connection',
  'content-length',
  'content-type',
  'cookie',
  'host',
  'keep-alive',
  'proxy-authorization',
  'proxy-connection',
  'set-cookie',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
  'user-agent',
]

const hostOwnedServiceHeaders = [...commonHostOwnedHeaders, 'authorization']

const hostOwnedCredentialHeaders = [
  ...commonHostOwnedHeaders,
  'accept',
  'last-event-id',
  'mcp-protocol-version',
  'mcp-session-id',
]

const serviceHeaderName = except(
  headerName,
  hostOwnedServiceHeaders,
  'is set by Coral, not by a recipe',
)
const credentialHeaderName = except(
  headerName,
  hostOwnedCredentialHeaders,
  'cannot carry a credential',
)
const queryParameterName = literal.max(256)

const presentation = {
  label,
  description: description.optional(),
  documentation_url: url.optional(),
}

// Credentials

const credentialFields = withEntries(
  z.record(
    name,
    z.strictObject({
      label,
      description: description.optional(),
      sensitive: z.boolean().default(true),
    }),
  ),
  { min: 1, max: 16 },
  'must declare between 1 and 16 fields',
).describe(
  'The fields of one credential. Every field must be used. Coral keeps every value private, even one that is not secret, such as an account id. sensitive only masks the value as the person types it.',
)

const credentialValue = z.union([
  z.strictObject({ field: name }),
  z.strictObject({ literal: protocolLiteral }),
])

/** A parameter sends a field or a value; an empty value says nothing, so Coral refuses it. */
const queryParameterValue = z.union([
  z.strictObject({ field: name }),
  z.strictObject({ literal: protocolLiteral.min(1) }),
])

/** Coral sends no password rather than a constant one: a secret belongs in a field. */
const passwordValue = z.union([
  z.strictObject({ field: name }),
  z.strictObject({ literal: z.literal('') }),
])

const headerCredentialValue = z.union([
  z.strictObject({ field: name, prefix: protocolLiteral.optional() }),
  // Coral refuses a header with an empty fixed value.
  z.strictObject({ literal: protocolLiteral.min(1) }),
])

// OAuth

const oauthClient = z
  .strictObject({
    id: textTemplate.max(1024).optional(),
    authentication: z.enum(['none', 'client_secret_basic', 'client_secret_post']).default('none'),
  })
  .prefault({})
  .describe(
    'The OAuth application. Set id to use your own application; without one, Coral identifies itself to the server where the server and the operator policy allow. client_secret_basic or client_secret_post make Coral ask for the application secret.',
  )

const scopes = withUniqueItems(
  z.array(literal.max(256).regex(/^[^ ]+$/, 'must not contain spaces')).max(100),
  'must not repeat a scope',
)

const scopeFormat = z
  .strictObject({
    request: z.enum(['space', 'comma']).default('space'),
  })
  .prefault({})
  .describe(
    'How scopes are separated in the authorization request. This never joins several scopes into one.',
  )

const hostOwnedAuthorizationParameters = [
  'client_id',
  'client_secret',
  'redirect_uri',
  'response_type',
  'state',
  'scope',
  'resource',
  'code',
  'grant_type',
  'code_challenge',
  'code_challenge_method',
  'code_verifier',
]

const authorizationParameters = withEntries(
  z.record(
    except(name, hostOwnedAuthorizationParameters, 'is set by Coral, not by a recipe'),
    protocolLiteral,
  ),
  { max: 32 },
  'must have at most 32 parameters',
).describe(
  'Extra non-secret parameters for the authorization request, such as access_type: offline. Coral sets the OAuth protocol parameters itself.',
)

const clientConfigHintGenerator = z
  .enum(['slack_app_create_link'])
  .describe(
    'A Reef helper that writes help below the client ID field, such as a link that creates the OAuth application.',
  )

export type ClientConfigHintGenerator = z.infer<typeof clientConfigHintGenerator>

function isConfidentialClient(client: z.infer<typeof oauthClient>) {
  return (
    client.authentication === 'client_secret_basic' ||
    client.authentication === 'client_secret_post'
  )
}

// Authentication options

const authenticationOption = z.discriminatedUnion('method', [
  z.strictObject({ method: z.literal('none'), ...presentation }),
  z.strictObject({ method: z.literal('bearer'), ...presentation }),
  z
    .strictObject({
      method: z.literal('header_credentials'),
      ...presentation,
      fields: credentialFields,
      headers: withEntries(
        z.record(credentialHeaderName, headerCredentialValue),
        { min: 1, max: 16 },
        'must have between 1 and 16 headers',
      ),
    })
    .describe(
      "Credential fields sent in named headers, each optionally after an exact prefix such as 'Token token='. A single API key is one field in one header. The person enters only the credential.",
    ),
  z
    .strictObject({
      method: z.literal('basic_auth'),
      ...presentation,
      fields: credentialFields,
      username: credentialValue,
      password: passwordValue,
    })
    .describe(
      'HTTP Basic authentication. At least one part uses a credential field; a literal username such as api needs no entry. A password is a field, or empty to send none.',
    ),
  z
    .strictObject({
      method: z.literal('query_credentials'),
      ...presentation,
      fields: credentialFields,
      parameters: withEntries(
        z.record(queryParameterName, queryParameterValue),
        { min: 1, max: 16 },
        'must have between 1 and 16 parameters',
      ),
    })
    .describe(
      'OpenAPI only. Coral adds each credential value to each request as a query parameter.',
    ),
  z
    .strictObject({
      method: z.literal('oauth_authorization_code'),
      ...presentation,
      authorization_url: urlTemplate,
      token_url: urlTemplate,
      client: oauthClient,
      pkce: z.enum(['required', 'disabled']).default('required'),
      scopes,
      scope_format: scopeFormat,
      authorization_parameters: authorizationParameters.optional(),
    })
    .refine((option) => option.pkce !== 'disabled' || isConfidentialClient(option.client), {
      message: 'pkce: disabled needs client_secret_basic or client_secret_post',
      path: ['pkce'],
    })
    .describe(
      'Authorization-code OAuth. PKCE uses S256; only a confidential client (client_secret_basic or client_secret_post) may disable it.',
    ),
  z
    .strictObject({
      method: z.literal('oauth_device_code'),
      ...presentation,
      device_authorization_url: urlTemplate,
      token_url: urlTemplate,
      client: oauthClient,
      scopes,
      scope_format: scopeFormat,
    })
    .describe('Device-code OAuth. Coral handles device authorization, polling and tokens.'),
  z
    .strictObject({
      method: z.literal('oauth_discovery'),
      ...presentation,
      flow: z.enum(['authorization_code', 'device_code']),
      client: oauthClient,
      scopes: scopes.optional(),
      scope_format: scopeFormat,
      authorization_parameters: authorizationParameters.optional(),
      client_config_hint_generator: clientConfigHintGenerator.optional(),
    })
    .refine(
      (option) => option.flow !== 'device_code' || option.authorization_parameters === undefined,
      {
        message: 'a device_code flow cannot set authorization_parameters',
        path: ['authorization_parameters'],
      },
    )
    .describe(
      'MCP only. Coral finds the issuer and endpoints through OAuth discovery. Choose the flow, and optionally the exact scopes to ask for.',
    ),
])

const authentication = z
  .strictObject({
    default: name.optional(),
    options: withEntries(
      z.record(name, authenticationOption),
      { min: 1, max: 16 },
      'must offer between 1 and 16 options',
    ),
  })
  .describe(
    'Named ways to authenticate. The person confirms one, and Coral never falls back to another. A single option fills itself in; of several, default names the one to fill in.',
  )

// Inputs

const hostname =
  /^(?=.{1,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$/i

const input = z
  .strictObject({
    type: z.enum(['string', 'url', 'hostname']),
    label,
    description: description.optional(),
    documentation_url: url.optional(),
    default: literal.optional(),
    enum: withUniqueItems(z.array(literal).min(1).max(100), 'must not repeat a value').optional(),
  })
  .refine(
    (declared) => {
      const values = [
        ...(declared.enum ?? []),
        ...(declared.default === undefined ? [] : [declared.default]),
      ]
      if (declared.type === 'url') return values.every((value) => url.safeParse(value).success)
      if (declared.type === 'hostname') return values.every((value) => hostname.test(value))
      return true
    },
    { message: 'default and enum values must match the input type' },
  )
  .describe(
    'A non-secret value the person gives when adding the provider. A default must be in enum when both are set. Never use an input for a credential.',
  )

// Source, servers and headers

const bundledDocument = except(
  z
    .string()
    .regex(
      /^[a-z0-9][a-z0-9_-]*\.(?:json|ya?ml)$/,
      'must be the name of a .json or .yaml file beside the recipe',
    )
    .describe(
      'The file name of an OpenAPI document beside recipe.yaml, for a service that publishes none at a URL. Reef sends its text to Coral.',
    ),
  ['recipe.yaml'],
  'must not be the recipe itself',
)

const source = z.union([
  z.strictObject({
    openapi: z.union([
      z.strictObject({ url: urlTemplate }),
      z.strictObject({ file: bundledDocument }),
    ]),
  }),
  z.strictObject({ mcp: z.strictObject({ url: urlTemplate }) }),
])

const serverDeclaration = z
  .string()
  .max(1024)
  .regex(
    /^#\/(?:servers|(?:paths|components\/pathItems)\/.+\/servers)$/,
    'must point at a servers declaration',
  )

const mappedInput = z
  .strictObject({
    input: name,
    values: withEntries(
      z.record(literal, literal),
      { min: 1, max: 100 },
      'must map between 1 and 100 values',
    ),
  })
  .describe(
    'Sets a server variable from an input by mapping each input value to a variable value. The input should declare enum, and values should cover it.',
  )

const serverSelection = z
  .union([
    z.strictObject({ base_url: urlTemplate }),
    z.strictObject({
      declared_url: z
        .string()
        .min(1)
        .max(2048)
        .regex(/^(?!.*\{\{)/, 'must be the exact declared template')
        .regex(noControlCharacters, noControlCharactersMessage)
        .describe(
          'An OpenAPI server URL exactly as the document declares it, not a recipe template.',
        ),
      variables: z
        .record(z.string().min(1).max(256), z.union([textTemplate, mappedInput]))
        .optional(),
    }),
  ])
  .describe(
    'The server for one servers declaration in the OpenAPI document: a base_url, or one of its declared URLs with values for its variables.',
  )

const fieldHelp = withEntries(
  z.strictObject({
    label: label.optional(),
    description: description.optional(),
    documentation_url: url.optional(),
  }),
  // The strict object already limits the fields, so only the minimum is needed.
  { min: 1 },
  'must not be empty',
)

const icon = z
  .string()
  .regex(
    /^[a-z0-9][a-z0-9_-]*\.(?:svg|png)$/,
    'must be the name of a .svg or .png file beside the recipe',
  )
  .describe('The file name of an image beside recipe.yaml, not a path or URL.')

export const recipeSchema = z.strictObject({
  schema_version: z.literal(1),
  id: name,
  service: name
    .describe(
      'The id of the recipe that describes the same service from another source, such as github for github_mcp. Without one, the recipe is its own service. Reef offers the recipes of one service together.',
    )
    .optional(),
  display_name: label,
  description: description.optional(),
  // `light` serves both themes unless `dark` names a logo that reads on a dark surface.
  icons: z.strictObject({ light: icon, dark: icon.optional() }).optional(),
  inputs: z.record(name, input).optional(),
  source,
  authentication: authentication.optional(),
  servers: z.record(serverDeclaration, serverSelection).optional(),
  request_headers: withEntries(
    z.record(serviceHeaderName, textTemplate),
    { max: 64 },
    'must have at most 64 headers',
  )
    .describe(
      'Non-secret headers sent to the service with each request. Coral sets credential and transport headers itself.',
    )
    .optional(),
  help: z.strictObject({ servers: z.record(serverDeclaration, fieldHelp).optional() }).optional(),
})

export type Recipe = z.infer<typeof recipeSchema>

/**
 * The format as JSON Schema, written to recipes/schema.json for editors. It
 * carries every rule about a single field except whether a URL parses. That
 * check, rules that depend on another field, and the references parseRecipe
 * checks are enforced only when parsing.
 */
export function recipeJsonSchema() {
  return {
    ...z.toJSONSchema(recipeSchema, {
      target: 'draft-2020-12',
      // Describe what an author writes, so fields with defaults stay optional.
      io: 'input',
      unrepresentable: 'any',
      reused: 'ref',
    }),
    title: 'Provider Recipe',
    description:
      'Generated from apps/reef/app/lib/provider-recipes/recipe-format.ts. Run npm run recipes:schema --prefix apps/reef to update it.',
  }
}
export type RecipeAuthenticationOption = z.infer<typeof authenticationOption>

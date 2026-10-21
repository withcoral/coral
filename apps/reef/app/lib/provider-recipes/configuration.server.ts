// Turns a recipe into what the configuration step fills in on a first
// configuration. Every value is checked against what Coral read, and anything
// that does not fit is logged and left for the person to fill in (ADR 0004).

import type {
  ConfigurationReviewView,
  CredentialFieldPresentation,
  CredentialMethod,
  CredentialSourceValue,
  CredentialBindingValue,
  RecipeConfigurationView,
  RecipeInputView,
  RecipeServerView,
  SetupView,
} from '@/views/providers/types'
import { CLIENT_CONFIG_HINT_GENERATORS, type ClientConfigHintContext } from './client-config-hints'
import { inputReference } from './input-reference'
import type { Recipe, RecipeAuthenticationOption } from './recipe-format'

type Application = NonNullable<RecipeConfigurationView['application']>
type Mapped = Pick<RecipeConfigurationView, 'values' | 'application'>
type OAuthClient = {
  id?: string
  authentication?: 'none' | 'client_secret_basic' | 'client_secret_post'
}

export function recipeConfiguration(
  recipe: Recipe,
  setup: SetupView,
  hintContext: ClientConfigHintContext,
): RecipeConfigurationView | null {
  const review = setup.configuration
  if (!review || review.confirmed) return null
  const mcp = setup.kind === 'mcp'
  const skipped: string[] = []
  let mapped: Mapped = { values: {} }

  const option = chosenOption(recipe, skipped)
  const authentication = option
    ? authenticationFrom(option, mcp, review.automaticClientAvailable, skipped)
    : null
  if (authentication) {
    const method = authentication.values.authentication
    if (method && review.authenticationMethods.includes(method)) mapped = authentication
    else {
      if (option?.method === 'oauth_discovery') mapped.values.authentication = ''
      const missing =
        option?.method === 'oauth_discovery' ? `oauth_discovery flow ${option.flow}` : method
      skipped.push(`authentication: Coral does not offer ${missing} for this source`)
    }
  }
  if (recipe.servers) {
    if (mcp) skipped.push('servers: an MCP server has no server declarations')
    else mapped.values.servers = serversFrom(recipe.servers, review, skipped)
  }
  if (mcp && review.values?.requestHeaders !== undefined)
    mapped.values.requestHeaders = review.values.requestHeaders
  else if (recipe.request_headers)
    mapped.values.requestHeaders = Object.entries(recipe.request_headers)
      .map(([name, value]) => ({ name, value }))
      .toSorted((left, right) => left.name.localeCompare(right.name))
  if (skipped.length > 0)
    console.warn(
      `Provider recipe ${recipe.id} does not fully fit this setup:\n  ${skipped.join('\n  ')}`,
    )
  const generator =
    option?.method === 'oauth_discovery' ? option.client_config_hint_generator : undefined
  return {
    id: recipe.id,
    inputs: inputsUsedBy(recipe, mapped),
    suggestedMethods: suggestedMethods(recipe, review),
    ...mapped,
    ...(generator && {
      clientConfigHintMarkdown: CLIENT_CONFIG_HINT_GENERATORS[generator](hintContext),
    }),
  }
}

/** The offered methods that some recipe option names, in Coral's order. */
function suggestedMethods(recipe: Recipe, review: ConfigurationReviewView) {
  const options = Object.values(recipe.authentication?.options ?? {})
  return review.authenticationMethods.filter((method) =>
    options.some((option) => recipeMethod(option) === method),
  )
}

function recipeMethod(option: RecipeAuthenticationOption): CredentialMethod {
  switch (option.method) {
    case 'oauth_discovery':
      return option.flow === 'device_code' ? 'device' : 'oauth'
    case 'oauth_authorization_code':
      return 'oauth'
    case 'oauth_device_code':
      return 'device'
    case 'header_credentials':
      return 'header'
    case 'basic_auth':
      return 'basic'
    case 'query_credentials':
      return 'query'
    case 'none':
      return 'none'
    case 'bearer':
      return 'bearer'
  }
}

/**
 * Where the recipe says to find the credentials for `method`; null when it names
 * nowhere. Of several options for the method, the recipe's default answers.
 */
export function recipeDocumentationUrl(recipe: Recipe, method: CredentialMethod | null) {
  if (!method) return null
  const { options = {}, default: preferred } = recipe.authentication ?? {}
  const matching = Object.entries(options).filter(([, option]) => recipeMethod(option) === method)
  const [, option] = matching.find(([name]) => name === preferred) ?? matching[0] ?? []
  return option?.documentation_url ?? null
}

/** One option pre-fills itself; of several, only the recipe's default does. */
function chosenOption(recipe: Recipe, skipped: string[]) {
  const options = recipe.authentication?.options ?? {}
  const names = Object.keys(options)
  if (names.length === 1) return options[names[0]]
  const chosen = recipe.authentication?.default
  if (chosen !== undefined) return options[chosen]
  if (names.length > 1) skipped.push('authentication: several options and no default')
  return undefined
}

function authenticationFrom(
  option: RecipeAuthenticationOption,
  mcp: boolean,
  automaticClient: boolean,
  skipped: string[],
): Mapped | null {
  switch (option.method) {
    case 'none':
    case 'bearer':
      return { values: { authentication: option.method } }
    case 'oauth_authorization_code': {
      if (mcp) {
        skipped.push('oauth_authorization_code: an MCP server discovers its own OAuth endpoints')
        return null
      }
      return {
        values: {
          authentication: 'oauth',
          authorizationUrl: option.authorization_url,
          tokenUrl: option.token_url,
          scopes: option.scopes,
          scopeFormat: option.scope_format.request,
        },
        application: applicationFrom(
          option.client,
          option.pkce,
          option.authorization_parameters,
          automaticClient,
        ),
      }
    }
    case 'oauth_discovery':
      if (!mcp) {
        skipped.push('oauth_discovery: use an MCP source')
        return null
      }
      return {
        values: {
          authentication: recipeMethod(option),
          ...(option.scopes === undefined ? {} : { scopes: option.scopes }),
          ...(option.scope_format.request === 'comma' || option.scopes !== undefined
            ? { scopeFormat: option.scope_format.request }
            : {}),
        },
        application: applicationFrom(
          option.client,
          undefined,
          option.authorization_parameters,
          option.flow === 'device_code' ? false : automaticClient,
        ),
      }
    case 'header_credentials': {
      if (mcp) {
        skipped.push('header_credentials: Coral sends credential headers to OpenAPI providers only')
        return null
      }
      return {
        values: {
          authentication: 'header',
          headers: Object.entries(option.headers)
            .map(([header, binding]): CredentialBindingValue => {
              const name = header.toLowerCase()
              return 'literal' in binding
                ? { name, source: 'literal', value: binding.literal }
                : { name, source: 'field', field: binding.field, prefix: binding.prefix ?? '' }
            })
            .toSorted((left, right) => left.name.localeCompare(right.name)),
          credentialFields: presentation(option.fields),
        },
      }
    }
    case 'basic_auth': {
      if (mcp) {
        skipped.push('basic_auth: Coral sends Basic credentials to OpenAPI providers only')
        return null
      }
      return {
        values: {
          authentication: 'basic',
          basic: { username: source(option.username), password: source(option.password) },
          credentialFields: presentation(option.fields),
        },
      }
    }
    case 'query_credentials': {
      if (mcp) {
        skipped.push('query_credentials: Coral sends query credentials to OpenAPI providers only')
        return null
      }
      return {
        values: {
          authentication: 'query',
          parameters: Object.entries(option.parameters)
            .map(([name, value]) => ({ name, ...source(value) }))
            .toSorted((left, right) => left.name.localeCompare(right.name)),
          credentialFields: presentation(option.fields),
        },
      }
    }
    // Listed rather than a default, so a new recipe method fails to build until it is mapped.
    case 'oauth_device_code': {
      if (mcp) {
        skipped.push('oauth_device_code: use an OpenAPI source')
        return null
      }
      return {
        values: {
          authentication: 'device',
          authorizationUrl: option.device_authorization_url,
          tokenUrl: option.token_url,
          scopes: option.scopes,
          scopeFormat: option.scope_format.request,
        },
        application: applicationFrom(option.client, undefined, undefined, false),
      }
    }
  }
}

/** How each of the recipe's Credential Fields is asked for. */
function presentation(
  fields: Extract<RecipeAuthenticationOption, { fields: unknown }>['fields'],
): Record<string, CredentialFieldPresentation> {
  return Object.fromEntries(
    Object.entries(fields).map(([name, field]) => [
      name,
      { label: field.label, description: field.description ?? '', sensitive: field.sensitive },
    ]),
  )
}

function source(value: { field: string } | { literal: string }): CredentialSourceValue {
  return 'field' in value
    ? { source: 'field', field: value.field }
    : { source: 'literal', value: value.literal }
}

/**
 * Application settings, or nothing when the recipe leaves Coral's own choice in charge.
 * Only a client id asks for your own application; without one, Coral identifies itself
 * to the server where the server and the operator policy allow.
 */
function applicationFrom(
  client: OAuthClient | undefined,
  pkce: 'required' | 'disabled' | undefined,
  parameters: Record<string, string> | undefined,
  automaticClient: boolean,
): Application | undefined {
  if (client?.id === undefined && automaticClient) {
    const encoded = new URLSearchParams(parameters ?? {}).toString()
    return encoded
      ? {
          mode: 'automatic',
          clientId: '',
          authentication: 'none',
          pkce: 's256',
          parameters: encoded,
        }
      : undefined
  }
  const authentication =
    client?.authentication === 'client_secret_basic'
      ? 'basic'
      : client?.authentication === 'client_secret_post'
        ? 'post'
        : 'none'
  if (client?.id === undefined && authentication === 'none' && pkce !== 'disabled' && !parameters)
    return undefined
  return {
    mode: 'manual',
    clientId: client?.id ?? '',
    authentication,
    pkce: pkce === 'disabled' ? 'disabled' : 's256',
    parameters: new URLSearchParams(parameters ?? {}).toString(),
  }
}

function serversFrom(
  servers: NonNullable<Recipe['servers']>,
  review: ConfigurationReviewView,
  skipped: string[],
): RecipeServerView[] {
  return Object.entries(servers).flatMap(([declaration, selection]): RecipeServerView[] => {
    const group = review.servers.find((candidate) => candidate.declaration === declaration)
    if (!group) {
      skipped.push(`servers.${declaration}: the document has no such server declaration`)
      return []
    }
    if ('base_url' in selection)
      return [{ declaration, url: selection.base_url, custom: true, variables: {} }]
    const option = group.options.find((candidate) => candidate.url === selection.declared_url)
    if (!option) {
      skipped.push(
        `servers.${declaration}: the document does not declare ${selection.declared_url}`,
      )
      return []
    }
    const variables: RecipeServerView['variables'] = Object.fromEntries(
      option.variables.map((variable) => [variable.name, variable.defaultValue]),
    )
    for (const [name, value] of Object.entries(selection.variables ?? {})) {
      if (option.variables.some((variable) => variable.name === name)) variables[name] = value
      else skipped.push(`servers.${declaration}: the document declares no variable "${name}"`)
    }
    return [{ declaration, url: option.url, custom: false, variables }]
  })
}

/** The recipe's inputs that a mapped value refers to, in the recipe's order. */
function inputsUsedBy(recipe: Recipe, mapped: Mapped): RecipeInputView[] {
  const used = new Set<string>()
  const visit = (value: unknown) => {
    if (typeof value === 'string') {
      for (const match of value.matchAll(inputReference)) used.add(match[1])
    } else if (Array.isArray(value)) {
      for (const item of value) visit(item)
    } else if (typeof value === 'object' && value !== null) {
      if ('input' in value && typeof value.input === 'string') used.add(value.input)
      for (const item of Object.values(value)) visit(item)
    }
  }
  visit(mapped)
  return Object.entries(recipe.inputs ?? {})
    .filter(([name]) => used.has(name))
    .map(([name, input]) => ({
      name,
      label: input.label,
      description: input.description ?? '',
      type: input.type,
      defaultValue: input.default ?? '',
      choices: input.enum ?? [],
    }))
}

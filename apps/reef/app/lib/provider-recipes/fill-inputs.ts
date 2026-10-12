// Fills a recipe's {{input.name}} references from the answers given in the
// configuration step. A value waiting for an unanswered input stays empty rather
// than becoming a half-built URL, and the server's own value is kept instead.

import type {
  ConfigurationValues,
  RecipeConfigurationView,
  RecipeServerView,
} from '@/views/providers/types'
import { inputReference } from './input-reference'

export type RecipeAnswers = Record<string, string>
type Server = ConfigurationValues['servers'][number]
type Filled = {
  values: Partial<ConfigurationValues>
  application?: RecipeConfigurationView['application']
}

export function defaultAnswers(recipe: RecipeConfigurationView): RecipeAnswers {
  return Object.fromEntries(recipe.inputs.map((input) => [input.name, input.defaultValue]))
}

/** The text with every reference filled in, or undefined while an answer is missing. */
function fill(text: string, answers: RecipeAnswers) {
  let missing = false
  const filled = text.replace(inputReference, (_reference, name: string) => {
    const answer = answers[name]?.trim() ?? ''
    if (!answer) missing = true
    return answer
  })
  return missing ? undefined : filled
}

/** The inputs a recipe server's URL and variables use, in the order they appear. */
export function serverInputs(server: RecipeServerView): string[] {
  const texts = [
    server.url,
    ...Object.values(server.variables).map((variable) =>
      typeof variable === 'string' ? variable : `{{input.${variable.input}}}`,
    ),
  ]
  const names = texts.flatMap((text) => [...text.matchAll(inputReference)].map((match) => match[1]))
  return [...new Set(names)]
}

/** A server missing any answer has no URL, so the document's own server stays. */
function filledServer(server: RecipeServerView, answers: RecipeAnswers): Server {
  const variables = Object.entries(server.variables).map(
    ([name, variable]) =>
      [
        name,
        typeof variable === 'string'
          ? fill(variable, answers)
          : variable.values[answers[variable.input]?.trim() ?? ''],
      ] as const,
  )
  const url = fill(server.url, answers)
  const complete = url !== undefined && variables.every(([, value]) => value !== undefined)
  return {
    declaration: server.declaration,
    custom: server.custom,
    url: complete ? url : '',
    variables: Object.fromEntries(variables.map(([name, value]) => [name, value ?? ''])),
  }
}

/** The recipe's values with every input answer filled in, in Reef's configuration shape. */
export function filledRecipe(recipe: RecipeConfigurationView, answers: RecipeAnswers): Filled {
  const { servers, authorizationUrl, tokenUrl, requestHeaders, ...plain } = recipe.values
  return {
    values: {
      ...plain,
      ...(requestHeaders === undefined
        ? {}
        : {
            requestHeaders: requestHeaders.map(({ name, value }) => ({
              name,
              value: fill(value, answers) ?? '',
            })),
          }),
      ...(authorizationUrl === undefined
        ? {}
        : { authorizationUrl: fill(authorizationUrl, answers) ?? '' }),
      ...(tokenUrl === undefined ? {} : { tokenUrl: fill(tokenUrl, answers) ?? '' }),
      ...(servers === undefined
        ? {}
        : { servers: servers.map((server) => filledServer(server, answers)) }),
    },
    application: recipe.application && {
      ...recipe.application,
      clientId: fill(recipe.application.clientId, answers) ?? '',
    },
  }
}

function sortedVariables(server: Server) {
  return JSON.stringify(Object.entries(server.variables).toSorted(([x], [y]) => x.localeCompare(y)))
}

export function sameServer(a: Server | undefined, b: Server | undefined) {
  if (!a || !b) return a === b
  return a.url === b.url && a.custom === b.custom && sortedVariables(a) === sortedVariables(b)
}

/**
 * Moves each value that still holds the recipe's earlier fill to its new fill.
 * A value the person changed stays as they left it.
 */
export function refilled(
  current: ConfigurationValues,
  base: ConfigurationValues,
  before: Partial<ConfigurationValues>,
  after: Partial<ConfigurationValues>,
): ConfigurationValues {
  const next = { ...current }
  for (const field of ['authorizationUrl', 'tokenUrl'] as const) {
    if (current[field] === (before[field] || base[field])) next[field] = after[field] || base[field]
  }
  next.requestHeaders = current.requestHeaders?.map((header) => {
    const previous = (before.requestHeaders ?? base.requestHeaders)?.find(
      ({ name }) => name === header.name,
    )
    const replacement = (after.requestHeaders ?? base.requestHeaders)?.find(
      ({ name }) => name === header.name,
    )
    return previous?.value === header.value && replacement ? replacement : header
  })
  const declarations = new Set(
    [...(before.servers ?? []), ...(after.servers ?? [])].map((server) => server.declaration),
  )
  for (const declaration of declarations) {
    const filledIn = (servers: readonly Server[] | undefined) =>
      servers?.find((server) => server.declaration === declaration && server.url)
    const original = base.servers.find((server) => server.declaration === declaration)
    const existing = next.servers.find((server) => server.declaration === declaration)
    if (!sameServer(existing, filledIn(before.servers) ?? original)) continue
    const replacement = filledIn(after.servers) ?? original
    next.servers = [
      ...next.servers.filter((server) => server.declaration !== declaration),
      ...(replacement ? [replacement] : []),
    ]
  }
  return next
}

/** The server's values with the recipe laid over them, before anyone edits them. */
export function withRecipe(
  base: ConfigurationValues,
  filled: Partial<ConfigurationValues>,
): ConfigurationValues {
  return refilled(
    {
      ...base,
      authentication: filled.authentication ?? base.authentication,
      scopes: filled.scopes ?? base.scopes,
      scopeFormat: filled.scopeFormat ?? base.scopeFormat,
      requestHeaders: filled.requestHeaders ?? base.requestHeaders,
      headers: filled.headers ?? base.headers,
      basic: filled.basic ?? base.basic,
      parameters: filled.parameters ?? base.parameters,
      credentialFields: filled.credentialFields ?? base.credentialFields,
    },
    base,
    {},
    filled,
  )
}

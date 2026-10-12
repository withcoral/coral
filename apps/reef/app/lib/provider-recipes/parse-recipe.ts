// Turns recipe YAML into a checked Recipe, or a list of problems. It never
// throws, so a caller can skip a bad recipe rather than fail (ADR 0004).

import { parse } from 'yaml'

import { inputReference } from './input-reference'
import { recipeSchema, type Recipe, type RecipeAuthenticationOption } from './recipe-format'

export type RecipeParseResult = { ok: true; recipe: Recipe } | { ok: false; problems: string[] }

export function parseRecipe(text: string): RecipeParseResult {
  let document: unknown
  try {
    // Duplicate keys are an error rather than last-one-wins.
    document = parse(text, { uniqueKeys: true })
  } catch (error) {
    return {
      ok: false,
      problems: [`YAML: ${error instanceof Error ? error.message : String(error)}`],
    }
  }

  const parsed = recipeSchema.safeParse(document)
  if (!parsed.success) {
    return {
      ok: false,
      problems: parsed.error.issues.flatMap((issue) =>
        // zod reports a bad record key as "Invalid key in record" and keeps the rule's reason inside.
        (issue.code === 'invalid_key' && issue.issues.length > 0 ? issue.issues : [issue]).map(
          ({ message }) => `${formatPath(issue.path)}: ${message}`,
        ),
      ),
    }
  }

  const problems = referenceProblems(parsed.data)
  return problems.length === 0 ? { ok: true, recipe: parsed.data } : { ok: false, problems }
}

function formatPath(path: readonly PropertyKey[]) {
  return path.length === 0 ? 'recipe' : path.map(String).join('.')
}

/** Methods an MCP recipe cannot use. oauth_discovery is the reverse: MCP only. */
const openApiOnlyMethods = new Set<RecipeAuthenticationOption['method']>([
  'query_credentials',
  'oauth_authorization_code',
  'oauth_device_code',
])

/** Checks between fields that the schema cannot express. */
function referenceProblems(recipe: Recipe): string[] {
  const problems: string[] = []
  const inputs = recipe.inputs ?? {}
  const options = recipe.authentication?.options ?? {}

  const defaultOption = recipe.authentication?.default
  if (defaultOption !== undefined && !Object.hasOwn(options, defaultOption)) {
    problems.push(`authentication.default: option "${defaultOption}" is not declared`)
  }

  for (const reference of new Set(inputReferences(recipe))) {
    if (!Object.hasOwn(inputs, reference)) {
      problems.push(`input "${reference}" is referenced but not declared`)
    }
  }

  for (const [key, input] of Object.entries(inputs)) {
    if (
      input.default !== undefined &&
      input.enum !== undefined &&
      !input.enum.includes(input.default)
    ) {
      problems.push(`inputs.${key}: default "${input.default}" is not in its enum`)
    }
  }

  const mcp = 'mcp' in recipe.source
  if (mcp && recipe.servers !== undefined) {
    problems.push('servers: an MCP recipe cannot set servers')
  }

  for (const [key, option] of Object.entries(options)) {
    const path = `authentication.options.${key}`
    if (mcp ? openApiOnlyMethods.has(option.method) : option.method === 'oauth_discovery') {
      problems.push(`${path}: ${option.method} needs ${mcp ? 'an OpenAPI' : 'an MCP'} source`)
    }
    problems.push(...credentialFieldProblems(path, option))
  }

  return problems
}

/** Every input a recipe references: {{input.name}} in any string, and mapped server variables. */
function inputReferences(value: unknown): string[] {
  if (typeof value === 'string') {
    return [...value.matchAll(inputReference)].map((match) => match[1])
  }
  if (Array.isArray(value)) return value.flatMap(inputReferences)
  if (typeof value === 'object' && value !== null) {
    const mapped =
      'input' in value && 'values' in value && typeof value.input === 'string' ? [value.input] : []
    return [...mapped, ...Object.values(value).flatMap(inputReferences)]
  }
  return []
}

function credentialFieldProblems(path: string, option: RecipeAuthenticationOption): string[] {
  if (!('fields' in option)) return []

  const referenced = credentialValues(option).flatMap((value) =>
    'field' in value ? [value.field] : [],
  )
  const declared = Object.keys(option.fields)
  return [
    ...referenced
      .filter((field) => !declared.includes(field))
      .map((field) => `${path}: field "${field}" is referenced but not declared`),
    ...declared
      .filter((field) => !referenced.includes(field))
      .map((field) => `${path}: field "${field}" is declared but never used`),
  ]
}

function credentialValues(option: RecipeAuthenticationOption) {
  switch (option.method) {
    case 'basic_auth':
      return [option.username, option.password]
    case 'header_credentials':
      return Object.values(option.headers)
    case 'query_credentials':
      return Object.values(option.parameters)
    default:
      return []
  }
}

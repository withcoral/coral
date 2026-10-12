import type { RecipeInputView } from '@/views/providers/types'
import { inputReference } from './input-reference'

// What Add provider offers before a source is read: a recipe from the library, or
// a URL someone typed, shaped alike so the add page reads either the same way.
// The server builds source choices from recipes; page code never sees the recipe
// format itself (ADR 0004).

/** Where the source is read from: a URL, or a document bundled beside the recipe. */
export type SourceLocation =
  | {
      /** Which of the two the source is; the person states it for a typed URL. */
      kind: 'openapi' | 'mcp'
      /** URL of the OpenAPI document or the MCP server endpoint. */
      url: string
    }
  | {
      kind: 'openapi'
      /** File name of the OpenAPI document bundled beside the recipe, read in place of a URL. */
      document: string
    }

/** A recipe's logo URLs. `light` serves both themes when `dark` is absent. */
export type RecipeIcons = { light: string; dark?: string }

export type SourceChoice = SourceLocation & {
  /** The recipe's id; empty for a typed URL. */
  id: string
  /**
   * The service whose catalog card offers this recipe, also the suggested
   * namespace; the recipe's own id when absent.
   */
  service?: string
  displayName: string
  /** One line on the catalog row. */
  description: string
  /** URLs of the logos bundled beside the recipe; a lettermark when absent. */
  icons?: RecipeIcons
  /** Non-secret service headers needed before reading the MCP catalog. */
  requestHeaders?: { name: string; value: string }[]
  /** Non-secret answers needed to locate this source before discovery. */
  sourceInputs?: RecipeInputView[]
}

/** The service a recipe belongs to: the one it names, or itself. */
function serviceOf(choice: SourceChoice): string {
  return choice.service ?? choice.id
}

/** The namespace Configuration first offers for a recipe: its service. */
export function recipeNamespace(choice: SourceChoice): string {
  return serviceOf(choice)
}

/** The recipes of one service, which the catalog offers as one card. */
export type ServiceChoices = {
  service: string
  /** The recipe that names and pictures the card: the one named after the service, else the first. */
  shown: SourceChoice
  choices: readonly SourceChoice[]
}

/** Recipes grouped by service, in the order of the names their cards show. */
export function byService(choices: readonly SourceChoice[]): ServiceChoices[] {
  const services = new Map<string, SourceChoice[]>()
  for (const choice of choices) {
    const service = serviceOf(choice)
    const group = services.get(service)
    if (group) group.push(choice)
    else services.set(service, [choice])
  }
  return [...services]
    .map(([service, group]) => ({
      service,
      shown: group.find((choice) => choice.id === service) ?? group[0],
      choices: group,
    }))
    .toSorted((a, b) =>
      a.shown.displayName.localeCompare(b.shown.displayName, 'en', { sensitivity: 'base' }),
    )
}

/** The query parameter that opens Add provider with a recipe's source filled in. */
export const RECIPE_PARAM = 'recipe'
/** Query parameters that open Add provider reading a typed URL, as a recipe would. */
export const SOURCE_PARAM = 'source'
export const URL_PARAM = 'url'

/**
 * A typed URL as a source choice, so the add page reads it as it reads a recipe.
 * The kind travels with it: a refusal looks the same from a document host and
 * from a server awaiting a token, so only the person who holds the URL knows
 * which it is.
 */
export function adHocSource(kind: string | null, url: string | null): SourceChoice | null {
  if ((kind !== 'mcp' && kind !== 'openapi') || !url) return null
  try {
    const parsed = new URL(url)
    if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') return null
    return { id: '', displayName: parsed.hostname, description: url, kind, url }
  } catch {
    return null
  }
}

/**
 * The `source` field a chosen source posts, in the words the server reads. A
 * bundled document posts `recipe`, and the server reads the text from its own
 * library by the recipe's id, never from the browser.
 */
export function recipeSource(choice: SourceChoice): 'mcp' | 'url' | 'recipe' {
  if ('document' in choice) return 'recipe'
  return choice.kind === 'mcp' ? 'mcp' : 'url'
}

/** What the source field shows: the URL, or the file name of a bundled document. */
export function sourceAddress(choice: SourceChoice): string {
  return 'document' in choice ? choice.document : choice.url
}

/** Where a recipe or typed URL reopens the add page with its source known. */
export function sourceSearch(choice: SourceChoice) {
  const params = new URLSearchParams()
  if (choice.id) params.set(RECIPE_PARAM, choice.id)
  else if ('url' in choice) {
    params.set(SOURCE_PARAM, choice.kind)
    params.set(URL_PARAM, choice.url)
  }
  return params.toString()
}

/** Resolve a source address only after every required input has a valid answer. */
export function filledSourceUrl(choice: SourceChoice, answers: Record<string, string>): string {
  if (!('url' in choice)) return ''
  for (const input of choice.sourceInputs ?? []) {
    const answer = answers[input.name]?.trim() ?? ''
    if (!answer || (input.choices.length > 0 && !input.choices.includes(answer))) return ''
    if (input.type === 'url' && !adHocSource('mcp', answer)) return ''
    if (
      input.type === 'hostname' &&
      !/^(?=.{1,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$/i.test(
        answer,
      )
    )
      return ''
  }
  let missing = false
  const inputs = new Map(choice.sourceInputs?.map((input) => [input.name, input]))
  const url = choice.url.replace(inputReference, (reference, name: string, offset: number) => {
    const answer = answers[name]?.trim() ?? ''
    if (!answer) missing = true
    const type = inputs.get(name)?.type
    // Strip only the join separator. A complete endpoint may require its final slash.
    if (type === 'url' && choice.url[offset + reference.length] === '/')
      return answer.replace(/\/+$/, '')
    return answer
  })
  return missing ? '' : url
}

import { inputReference } from './input-reference'
// The checked-in recipe library. Vite reads recipes/*/recipe.yaml when Reef is
// built and bundles the text, so the Reef image and Desktop carry the library in
// the server build and nothing reads recipe files at run time (ADR 0004). Each
// recipe's icon sits beside it and ships as a client asset. An OpenAPI document
// a recipe bundles sits beside it too and stays in the server build. A recipe
// that fails to parse is logged and left out rather than failing Reef.

import type { SourceChoice, SourceLocation } from './choice'
import { parseRecipe } from './parse-recipe'
import type { Recipe } from './recipe-format'

const recipeTexts = import.meta.glob<string>('../../../recipes/*/recipe.yaml', {
  query: '?raw',
  import: 'default',
  eager: true,
})

const iconUrls = import.meta.glob<string>('../../../recipes/*/*.{svg,png}', {
  query: '?url',
  import: 'default',
  eager: true,
})

const documentTexts = import.meta.glob<string>(
  ['../../../recipes/*/*.{json,yaml,yml}', '!../../../recipes/*/recipe.yaml'],
  { query: '?raw', import: 'default', eager: true },
)

function loadRecipes(texts: Record<string, string>): readonly Recipe[] {
  const recipes: Recipe[] = []
  for (const [file, text] of Object.entries(texts).toSorted(([a], [b]) => a.localeCompare(b))) {
    const result = parseRecipe(text)
    if (result.ok) recipes.push(result.recipe)
    else console.warn(`Skipping provider recipe ${file}:\n  ${result.problems.join('\n  ')}`)
  }
  return recipes
}

export const PROVIDER_RECIPES = loadRecipes(recipeTexts)

export function recipeById(id: string | undefined): Recipe | null {
  if (!id) return null
  const recipe = PROVIDER_RECIPES.find((candidate) => candidate.id === id)
  if (!recipe) console.warn(`Provider recipe ${id} is not in the library, so nothing is filled in`)
  return recipe ?? null
}

/** Where the browser loads one of a recipe's icons; none when the file is missing. */
function iconUrl(recipe: Recipe, file: string | undefined): string | undefined {
  if (file === undefined) return undefined
  const url = iconUrls[`../../../recipes/${recipe.id}/${file}`]
  if (url === undefined) console.warn(`Provider recipe ${recipe.id} has no icon file ${file}`)
  return url
}

/**
 * The text of the OpenAPI document a recipe bundles; null when the recipe reads
 * its source from a URL, or is not in the library, or its file is missing.
 */
export function recipeDocument(id: string): string | null {
  const recipe = PROVIDER_RECIPES.find((candidate) => candidate.id === id)
  if (!recipe || !('openapi' in recipe.source) || !('file' in recipe.source.openapi)) return null
  const text = documentTexts[`../../../recipes/${recipe.id}/${recipe.source.openapi.file}`]
  if (text === undefined)
    console.warn(`Provider recipe ${recipe.id} has no document file ${recipe.source.openapi.file}`)
  return text ?? null
}

/** A recipe's source and the non-secret questions needed before discovery. */
export function recipeChoice(recipe: Recipe): SourceChoice | null {
  const source: SourceLocation =
    'mcp' in recipe.source
      ? { kind: 'mcp', url: recipe.source.mcp.url }
      : 'file' in recipe.source.openapi
        ? { kind: 'openapi', document: recipe.source.openapi.file }
        : { kind: 'openapi', url: recipe.source.openapi.url }
  const used = new Set(
    'url' in source ? [...source.url.matchAll(inputReference)].map((match) => match[1]) : [],
  )
  const sourceInputs = Object.entries(recipe.inputs ?? {})
    .filter(([name]) => used.has(name))
    .map(([name, input]) => ({
      name,
      label: input.label,
      description: input.description ?? '',
      type: input.type,
      defaultValue: input.default ?? '',
      choices: input.enum ?? [],
    }))
  const light = iconUrl(recipe, recipe.icons?.light)
  const dark = iconUrl(recipe, recipe.icons?.dark)
  return {
    id: recipe.id,
    ...(recipe.service === undefined ? {} : { service: recipe.service }),
    displayName: recipe.display_name,
    description: recipe.description ?? '',
    ...source,
    ...(sourceInputs.length > 0 ? { sourceInputs } : {}),
    ...(source.kind === 'mcp' && recipe.request_headers
      ? {
          requestHeaders: Object.entries(recipe.request_headers).map(([name, template]) => {
            let missing = false
            const value = template.replace(inputReference, (_reference, input: string) => {
              const answer = recipe.inputs?.[input]?.default?.trim() ?? ''
              if (!answer) missing = true
              return answer
            })
            return { name, value: missing ? '' : value }
          }),
        }
      : {}),
    // Without the light logo, a dark one alone would leave the light theme blank.
    ...(light === undefined ? {} : { icons: dark === undefined ? { light } : { light, dark } }),
  }
}

export const RECIPE_CHOICES: readonly SourceChoice[] = PROVIDER_RECIPES.flatMap((recipe) => {
  const choice = recipeChoice(recipe)
  return choice ? [choice] : []
})

export function recipeChoiceById(id: string | null): SourceChoice | null {
  if (!id) return null
  return RECIPE_CHOICES.find((choice) => choice.id === id) ?? null
}

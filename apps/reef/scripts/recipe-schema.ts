// Writes recipes/schema.json from the zod recipe schema, so editors can
// complete and check recipes. The recipe test fails when the file is stale.

import { writeFile } from 'node:fs/promises'

import { recipeJsonSchema } from '../app/lib/provider-recipes/recipe-format.ts'

const target = new URL('../recipes/schema.json', import.meta.url)
await writeFile(target, `${JSON.stringify(recipeJsonSchema(), null, 2)}\n`)
console.log(`Wrote ${target.pathname}`)

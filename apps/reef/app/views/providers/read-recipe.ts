import { useCallback, useEffect, useRef, useState } from 'react'
import { useFetcher } from 'react-router'
import { addToast } from '@/wax/components/toast'
import {
  recipeSource,
  sourceSearch,
  recipeNamespace,
  type SourceChoice,
} from '@/lib/provider-recipes/choice'
import type { FormContext, StepperResult } from './types'

/**
 * Reads a recipe's source where it was chosen, posting the same prepare request
 * the source form would. A read that succeeds redirects to Configuration; a
 * refusal stays put and says why, so the person can choose another at once.
 */
export function useReadRecipe(form: FormContext, basePath: string) {
  const fetcher = useFetcher<StepperResult>()
  const [chosen, setChosen] = useState<SourceChoice | null>(null)
  const announced = useRef<StepperResult>(undefined)
  useEffect(() => {
    if (!fetcher.data?.error || announced.current === fetcher.data) return
    announced.current = fetcher.data
    addToast('error', {
      title: `Coral could not read ${chosen?.displayName ?? 'the source'}`,
      description: fetcher.data.error,
    })
  }, [fetcher.data, chosen])
  const { submit } = fetcher
  const read = useCallback(
    (recipe: SourceChoice) => {
      const body = new FormData()
      body.set('csrf', form.csrf)
      body.set('requestId', form.requestId)
      body.set('intent', 'prepare')
      body.set('source', recipeSource(recipe))
      if ('url' in recipe) body.set('sourceUrl', recipe.url)
      if (recipe.kind === 'mcp')
        body.set('requestHeaders', JSON.stringify(recipe.requestHeaders ?? []))
      // A recipe's service is its suggested namespace, so Configuration offers
      // "github" rather than the first label of the host.
      if (recipe.id) {
        body.set('namespace', recipeNamespace(recipe))
        body.set('displayName', recipe.displayName)
        body.set('recipe', recipe.id)
      }
      setChosen(recipe)
      void submit(body, {
        method: 'post',
        // A typed URL can hold a token, so it stays out of the address and its logs.
        action: recipe.id ? `${basePath}?${sourceSearch(recipe)}` : basePath,
        encType: 'multipart/form-data',
      })
    },
    [submit, form.csrf, form.requestId, basePath],
  )
  // The redirect that follows a successful read keeps the fetcher busy until
  // Configuration loads, so the chosen recipe shows as reading until then.
  const idle = fetcher.state === 'idle'
  return { read, reading: idle ? null : chosen, failed: idle && Boolean(fetcher.data?.error) }
}

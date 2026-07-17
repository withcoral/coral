import type { ShouldRevalidateFunctionArgs } from 'react-router'

import type { Route } from './+types/traces'

import { TracesIndex } from '@/views/traces/traces-index'

export { loader } from './traces-loader'

export const middleware: Route.MiddlewareFunction[] = [
  async (_, next) => {
    const response = await next()
    response.headers.set('Cache-Control', 'private, no-store')
    return response
  },
]

export function shouldRevalidate({
  currentParams,
  defaultShouldRevalidate,
  formMethod,
  nextParams,
}: ShouldRevalidateFunctionArgs) {
  if (formMethod && formMethod.toUpperCase() !== 'GET') return true
  if (currentParams.workspaceId !== nextParams.workspaceId) return true
  if (currentParams.traceId !== nextParams.traceId) return nextParams.traceId === undefined
  return defaultShouldRevalidate
}

export default function TracesRoute({ loaderData, params }: Route.ComponentProps) {
  return <TracesIndex {...loaderData} workspaceId={params.workspaceId} />
}

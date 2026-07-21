import type { ShouldRevalidateFunctionArgs } from 'react-router'

import type { Route } from './+types/traces'

import { ROOT_SPAN_ID_PARAM } from '@/views/traces/trace-location'
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
  currentUrl,
  defaultShouldRevalidate,
  formMethod,
  nextParams,
  nextUrl,
}: ShouldRevalidateFunctionArgs) {
  if (formMethod && formMethod.toUpperCase() !== 'GET') return true
  if (currentParams.workspaceId !== nextParams.workspaceId) return true
  if (currentParams.traceId !== nextParams.traceId) return nextParams.traceId === undefined
  if (
    currentParams.traceId &&
    currentUrl.searchParams.get(ROOT_SPAN_ID_PARAM) !== nextUrl.searchParams.get(ROOT_SPAN_ID_PARAM)
  ) {
    return false
  }
  return defaultShouldRevalidate
}

export default function TracesRoute({ loaderData, params }: Route.ComponentProps) {
  return <TracesIndex {...loaderData} workspaceId={params.workspaceId} />
}

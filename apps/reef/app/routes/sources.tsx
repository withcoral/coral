import { Outlet, type ShouldRevalidateFunctionArgs } from 'react-router'

import type { Route } from './+types/sources'

import { SourcesIndex } from '@/views/sources/sources-index'

export { action } from './sources-action'
export { loader } from './sources-loader'

export function shouldRevalidate({
  currentUrl,
  defaultShouldRevalidate,
  formMethod,
  nextUrl,
}: ShouldRevalidateFunctionArgs) {
  if (formMethod && formMethod.toUpperCase() !== 'GET') return true
  if (
    currentUrl.pathname !== nextUrl.pathname &&
    isSourcesDetailNavigation(currentUrl.pathname, nextUrl.pathname)
  ) {
    return false
  }
  return defaultShouldRevalidate
}

function isSourcesDetailNavigation(currentPath: string, nextPath: string) {
  const sourcesPath = '/sources'
  const sourceDetailPath = /^\/sources\/[^/]+$/
  const currentIsSources = currentPath === sourcesPath || sourceDetailPath.test(currentPath)
  const nextIsSources = nextPath === sourcesPath || sourceDetailPath.test(nextPath)
  return currentIsSources && nextIsSources
}

export default function SourcesRoute({ loaderData }: Route.ComponentProps) {
  return (
    <>
      <SourcesIndex entries={loaderData.entries} loadError={loaderData.loadError} />
      <Outlet />
    </>
  )
}

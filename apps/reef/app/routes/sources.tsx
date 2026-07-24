import { matchPath, Outlet, type ShouldRevalidateFunctionArgs } from 'react-router'

import type { Route } from './+types/sources'

import { SourcesIndex } from '@/views/sources/sources-index'
import { routePattern } from '@/routing/routemap'

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
  const currentWorkspace = sourcesWorkspaceId(currentPath)
  const nextWorkspace = sourcesWorkspaceId(nextPath)
  return currentWorkspace !== undefined && currentWorkspace === nextWorkspace
}

function sourcesWorkspaceId(pathname: string): string | undefined {
  return (
    matchPath(routePattern('workspaceSource'), pathname) ??
    matchPath(routePattern('workspaceSources'), pathname)
  )?.params.workspaceId
}

export default function SourcesRoute({ loaderData, params }: Route.ComponentProps) {
  return (
    <>
      <SourcesIndex
        entries={loaderData.entries}
        loadError={loaderData.loadError}
        workspaceId={params.workspaceId}
      />
      <Outlet />
    </>
  )
}

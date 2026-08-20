import { create } from '@bufbuild/protobuf'
import { matchPath } from 'react-router'

import { WorkspaceSchema, type Workspace } from '@/generated/coral/v1/resources_pb'
import { routePath, routePattern } from '@/routing/routemap'

export function workspaceFromParams(params: { workspaceId?: string }): Workspace {
  const workspaceId = params.workspaceId
  if (!workspaceId) {
    throw new Response('Missing workspace route parameter.', {
      status: 400,
      statusText: 'Invalid Workspace',
    })
  }
  return create(WorkspaceSchema, { name: workspaceId })
}

export function workspacePathForCurrentSection(workspaceId: string, pathname: string): string {
  const isFunctionsSection = matchPath(
    { end: false, path: routePattern('workspaceFunctions') },
    pathname,
  )
  if (isFunctionsSection) return routePath('workspaceFunctions', { workspaceId })

  const isSchemaSection = matchPath({ end: false, path: routePattern('workspaceSchema') }, pathname)
  if (isSchemaSection) return routePath('workspaceSchema', { workspaceId })

  const isTracesSection = matchPath({ end: false, path: routePattern('workspaceTraces') }, pathname)
  return isTracesSection
    ? routePath('workspaceTraces', { workspaceId })
    : routePath('workspaceSources', { workspaceId })
}

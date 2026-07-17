import { generatePath } from 'react-router'
import type { RouteObject } from 'react-router'

const HOME_PATH = '/'
const WORKSPACES_PATH = '/workspaces'
const WORKSPACE_SCHEMA_PATH = '/workspaces/:workspaceId/schema'
const WORKSPACE_SCHEMA_TABLE_PATH = '/workspaces/:workspaceId/schema/:schemaName/:tableName'
const WORKSPACE_SOURCE_DISCOVERY_PATH = '/workspaces/:workspaceId/sources/discover'
const WORKSPACE_SOURCE_INSTALL_PATH = '/workspaces/:workspaceId/sources/install'
const WORKSPACE_SOURCES_PATH = '/workspaces/:workspaceId/sources'
const WORKSPACE_SOURCE_PATH = '/workspaces/:workspaceId/sources/:sourceName'
const WORKSPACE_TRACES_PATH = '/workspaces/:workspaceId/traces'
const WORKSPACE_TRACE_PATH = '/workspaces/:workspaceId/traces/:traceId'
const SETTINGS_PATH = '/settings'

export const routeDefinitions = {
  home: {
    path: HOME_PATH,
    toPath: () => HOME_PATH,
  },
  settings: {
    path: SETTINGS_PATH,
    toPath: () => SETTINGS_PATH,
  },
  workspaces: {
    path: WORKSPACES_PATH,
    toPath: () => WORKSPACES_PATH,
  },
  workspaceSchema: {
    path: WORKSPACE_SCHEMA_PATH,
    toPath: (params: { workspaceId: string }) =>
      generatePath(WORKSPACE_SCHEMA_PATH, {
        workspaceId: params.workspaceId,
      }),
  },
  workspaceSchemaTable: {
    path: WORKSPACE_SCHEMA_TABLE_PATH,
    toPath: (params: { schemaName: string; tableName: string; workspaceId: string }) =>
      generatePath(WORKSPACE_SCHEMA_TABLE_PATH, {
        schemaName: params.schemaName,
        tableName: params.tableName,
        workspaceId: params.workspaceId,
      }),
  },
  workspaceSource: {
    path: WORKSPACE_SOURCE_PATH,
    toPath: (params: { sourceName: string; workspaceId: string }) =>
      generatePath(WORKSPACE_SOURCE_PATH, {
        sourceName: params.sourceName,
        workspaceId: params.workspaceId,
      }),
  },
  workspaceSourceDiscovery: {
    path: WORKSPACE_SOURCE_DISCOVERY_PATH,
    toPath: (params: { workspaceId: string }) =>
      generatePath(WORKSPACE_SOURCE_DISCOVERY_PATH, {
        workspaceId: params.workspaceId,
      }),
  },
  workspaceSourceInstall: {
    path: WORKSPACE_SOURCE_INSTALL_PATH,
    toPath: (params: { workspaceId: string }) =>
      generatePath(WORKSPACE_SOURCE_INSTALL_PATH, {
        workspaceId: params.workspaceId,
      }),
  },
  workspaceSources: {
    path: WORKSPACE_SOURCES_PATH,
    toPath: (params: { workspaceId: string }) =>
      generatePath(WORKSPACE_SOURCES_PATH, {
        workspaceId: params.workspaceId,
      }),
  },
  workspaceTrace: {
    path: WORKSPACE_TRACE_PATH,
    toPath: (params: { traceId: string; workspaceId: string }) =>
      generatePath(WORKSPACE_TRACE_PATH, {
        traceId: params.traceId,
        workspaceId: params.workspaceId,
      }),
  },
  workspaceTraces: {
    path: WORKSPACE_TRACES_PATH,
    toPath: (params: { workspaceId: string }) =>
      generatePath(WORKSPACE_TRACES_PATH, {
        workspaceId: params.workspaceId,
      }),
  },
} as const

export type AppRouteId = keyof typeof routeDefinitions
export type AppRoute = (typeof routeDefinitions)[AppRouteId]

export const routeMap: ReadonlyMap<AppRouteId, AppRoute> = new Map(
  Object.entries(routeDefinitions) as [AppRouteId, AppRoute][],
)

export type RoutePathArgs<RouteId extends AppRouteId> = Parameters<
  (typeof routeDefinitions)[RouteId]['toPath']
>

export function routePattern(routeId: AppRouteId): string {
  const route = routeMap.get(routeId)
  if (!route) throw new Error(`Unknown route id: ${routeId}`)
  return route.path
}

export function routePath<RouteId extends AppRouteId>(
  routeId: RouteId,
  ...args: RoutePathArgs<RouteId>
): string {
  const route = routeDefinitions[routeId]
  return (route.toPath as (...params: RoutePathArgs<RouteId>) => string)(...args)
}

export function routeObjects(
  routeIds: readonly AppRouteId[] = [...routeMap.keys()],
): RouteObject[] {
  return routeIds.map((routeId) => ({
    id: routeId,
    path: routePattern(routeId),
  }))
}

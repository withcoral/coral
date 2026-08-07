import { generatePath } from 'react-router'
import type { RouteObject } from 'react-router'

const HOME_PATH = '/'
const LOGIN_PATH = '/login'
const ONBOARDING_PATH = '/onboarding'
const WORKSPACES_PATH = '/workspaces'
const WORKSPACE_FUNCTIONS_PATH = '/workspaces/:workspaceId/functions'
const WORKSPACE_SCHEMA_PATH = '/workspaces/:workspaceId/schema'
const WORKSPACE_CATALOG_SCHEMA_TABLE_PATH =
  '/workspaces/:workspaceId/schema/catalogs/:catalogName/:schemaName/:tableName'
const WORKSPACE_CATALOG_SCHEMA_TABLE_FUNCTION_PATH =
  '/workspaces/:workspaceId/schema/catalogs/:catalogName/:schemaName/functions/:functionName'
const WORKSPACE_SCHEMA_TABLE_PATH = '/workspaces/:workspaceId/schema/:schemaName/:tableName'
const WORKSPACE_SCHEMA_TABLE_FUNCTION_PATH =
  '/workspaces/:workspaceId/schema/:schemaName/functions/:functionName'
const WORKSPACE_SOURCE_DISCOVERY_PATH = '/workspaces/:workspaceId/sources/discover'
const WORKSPACE_SOURCE_INSTALL_PATH = '/workspaces/:workspaceId/sources/install'
const WORKSPACE_SOURCES_PATH = '/workspaces/:workspaceId/sources'
const WORKSPACE_SOURCE_PATH = '/workspaces/:workspaceId/sources/:sourceName'
const WORKSPACE_TRACES_PATH = '/workspaces/:workspaceId/traces'
const WORKSPACE_TRACE_PATH = '/workspaces/:workspaceId/traces/:traceId'
const SETTINGS_PATH = '/settings'
const SETTINGS_MCP_CLIENTS_PATH = '/settings/mcp-clients'
const SETTINGS_RUNTIME_FEATURES_PATH = '/settings/runtime-features'

export const routeDefinitions = {
  home: {
    path: HOME_PATH,
    toPath: () => HOME_PATH,
  },
  // The one public route with call sites spread across the app: the interstitial
  // form, the callback error boundary, and the redirect every expired session
  // produces all name it, and two of those live outside `routes/`. Callers add
  // their own query, as they do for every other route here.
  login: {
    path: LOGIN_PATH,
    toPath: () => LOGIN_PATH,
  },
  onboarding: {
    path: ONBOARDING_PATH,
    toPath: () => ONBOARDING_PATH,
  },
  settings: {
    path: SETTINGS_PATH,
    toPath: () => SETTINGS_PATH,
  },
  settingsMcpClients: {
    path: SETTINGS_MCP_CLIENTS_PATH,
    toPath: () => SETTINGS_MCP_CLIENTS_PATH,
  },
  settingsRuntimeFeatures: {
    path: SETTINGS_RUNTIME_FEATURES_PATH,
    toPath: () => SETTINGS_RUNTIME_FEATURES_PATH,
  },
  workspaces: {
    path: WORKSPACES_PATH,
    toPath: () => WORKSPACES_PATH,
  },
  workspaceFunctions: {
    path: WORKSPACE_FUNCTIONS_PATH,
    toPath: (params: { workspaceId: string }) =>
      generatePath(WORKSPACE_FUNCTIONS_PATH, {
        workspaceId: params.workspaceId,
      }),
  },
  workspaceSchema: {
    path: WORKSPACE_SCHEMA_PATH,
    toPath: (params: { workspaceId: string }) =>
      generatePath(WORKSPACE_SCHEMA_PATH, {
        workspaceId: params.workspaceId,
      }),
  },
  workspaceCatalogSchemaTable: {
    path: WORKSPACE_CATALOG_SCHEMA_TABLE_PATH,
    toPath: (params: {
      catalogName: string
      schemaName: string
      tableName: string
      workspaceId: string
    }) =>
      generatePath(WORKSPACE_CATALOG_SCHEMA_TABLE_PATH, {
        catalogName: params.catalogName,
        schemaName: params.schemaName,
        tableName: params.tableName,
        workspaceId: params.workspaceId,
      }),
  },
  workspaceCatalogSchemaTableFunction: {
    path: WORKSPACE_CATALOG_SCHEMA_TABLE_FUNCTION_PATH,
    toPath: (params: {
      catalogName: string
      functionName: string
      schemaName: string
      workspaceId: string
    }) =>
      generatePath(WORKSPACE_CATALOG_SCHEMA_TABLE_FUNCTION_PATH, {
        catalogName: params.catalogName,
        functionName: params.functionName,
        schemaName: params.schemaName,
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
  workspaceSchemaTableFunction: {
    path: WORKSPACE_SCHEMA_TABLE_FUNCTION_PATH,
    toPath: (params: { functionName: string; schemaName: string; workspaceId: string }) =>
      generatePath(WORKSPACE_SCHEMA_TABLE_FUNCTION_PATH, {
        functionName: params.functionName,
        schemaName: params.schemaName,
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

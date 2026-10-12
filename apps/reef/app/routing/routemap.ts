import type { RouteObject } from 'react-router'

const HOME_PATH = '/'
const LOGIN_PATH = '/login'

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

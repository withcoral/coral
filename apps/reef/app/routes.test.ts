import type { RouteConfigEntry } from '@react-router/dev/routes'
import { describe, expect, it } from 'vitest'

import routes from './routes'

const protectedBoundaryFile = 'routes/_protected.tsx'
const publicRouteFiles = new Set([
  'routes/healthz.ts',
  'routes/readyz.ts',
  'routes/oauth-client-metadata.ts',
  'routes/login.tsx',
  'routes/auth.callback.tsx',
  'routes/logout.tsx',
])

function visitRoutes(
  entries: readonly RouteConfigEntry[],
  visitor: (entry: RouteConfigEntry, ancestors: readonly RouteConfigEntry[]) => void,
  ancestors: readonly RouteConfigEntry[] = [],
): void {
  for (const entry of entries) {
    visitor(entry, ancestors)
    if (entry.children) visitRoutes(entry.children, visitor, [...ancestors, entry])
  }
}

describe('route authentication boundary', () => {
  it('places every non-public route below the protected boundary', async () => {
    const routeConfig = await routes
    const publicRoutesOutsideBoundary: string[] = []
    const publicRoutesInsideBoundary: string[] = []
    const unprotectedRoutes: string[] = []
    let protectedBoundaryCount = 0

    visitRoutes(routeConfig, (entry, ancestors) => {
      const isProtectedBoundary = entry.file === protectedBoundaryFile
      if (isProtectedBoundary) protectedBoundaryCount += 1
      const isProtected =
        isProtectedBoundary || ancestors.some(({ file }) => file === protectedBoundaryFile)

      if (publicRouteFiles.has(entry.file)) {
        const destination = isProtected ? publicRoutesInsideBoundary : publicRoutesOutsideBoundary
        destination.push(entry.file)
      } else if (!isProtected) {
        unprotectedRoutes.push(entry.file)
      }
    })

    expect(protectedBoundaryCount).toBe(1)
    expect(unprotectedRoutes).toEqual([])
    expect(publicRoutesInsideBoundary).toEqual([])
    expect(new Set(publicRoutesOutsideBoundary)).toEqual(publicRouteFiles)
    expect(publicRoutesOutsideBoundary).toHaveLength(publicRouteFiles.size)
  })

  it('registers catalog-qualified schema routes', async () => {
    const routeConfig = await routes
    const registeredRoutes = new Map<string | undefined, string>()

    visitRoutes(routeConfig, (entry) => registeredRoutes.set(entry.path, entry.file))

    expect(registeredRoutes.get('catalogs/:catalogName/:schemaName/:tableName')).toBe(
      'routes/schema-catalog-table.tsx',
    )
    expect(registeredRoutes.get('catalogs/:catalogName/:schemaName/functions/:functionName')).toBe(
      'routes/schema-catalog-table-function.tsx',
    )
  })
})

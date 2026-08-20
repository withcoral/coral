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

describe('route authentication boundary', () => {
  it('places every non-public route below the protected boundary', async () => {
    const routeConfig = await routes
    const publicRoutesOutsideBoundary: string[] = []
    const publicRoutesInsideBoundary: string[] = []
    const unprotectedRoutes: string[] = []
    let protectedBoundaryCount = 0

    function visit(entries: RouteConfigEntry[], hasProtectedAncestor: boolean): void {
      for (const entry of entries) {
        const isProtectedBoundary = entry.file === protectedBoundaryFile
        if (isProtectedBoundary) protectedBoundaryCount += 1
        const isProtected = hasProtectedAncestor || isProtectedBoundary

        if (publicRouteFiles.has(entry.file)) {
          const destination = isProtected ? publicRoutesInsideBoundary : publicRoutesOutsideBoundary
          destination.push(entry.file)
        } else if (!isProtected) {
          unprotectedRoutes.push(entry.file)
        }

        if (entry.children) visit(entry.children, isProtected)
      }
    }

    visit(routeConfig, false)

    expect(protectedBoundaryCount).toBe(1)
    expect(unprotectedRoutes).toEqual([])
    expect(publicRoutesInsideBoundary).toEqual([])
    expect(new Set(publicRoutesOutsideBoundary)).toEqual(publicRouteFiles)
    expect(publicRoutesOutsideBoundary).toHaveLength(publicRouteFiles.size)
  })
})

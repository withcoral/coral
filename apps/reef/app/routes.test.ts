import type { RouteConfigEntry } from '@react-router/dev/routes'
import { describe, expect, it } from 'vitest'

import { settingsPath } from './components/settings/settings-nav'
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

// Catches: a section of the settings nav that leads to no page, or to a page
// that shows the workspace's sidebar because it left the settings layout.
it('puts every section of the settings nav under the settings layout', async () => {
  const settings = (entries: RouteConfigEntry[]): RouteConfigEntry | undefined =>
    entries
      .flatMap((entry) => [entry, settings(entry.children ?? [])])
      .find((entry) => entry?.file === 'routes/settings.tsx')
  const pages = settings(await routes)?.children?.map((entry) => `/${entry.path}`)
  // The nav does not lead to the sub-agent page yet (#3444).
  expect(pages?.toSorted()).toEqual(
    [...Object.values(settingsPath), '/organization/sub-agent'].toSorted(),
  )
})

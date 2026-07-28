import { describe, expect, it } from 'vitest'

import type { AppRouteId } from './routemap'
import { routeMap, routeObjects, routePath, routePattern } from './routemap'

const CANONICAL_PATTERNS = {
  home: '/',
  settings: '/settings',
  workspaces: '/workspaces',
  workspaceFunctions: '/workspaces/:workspaceId/functions',
  workspaceSchema: '/workspaces/:workspaceId/schema',
  workspaceSchemaTable: '/workspaces/:workspaceId/schema/:schemaName/:tableName',
  workspaceSchemaTableFunction:
    '/workspaces/:workspaceId/schema/:schemaName/functions/:functionName',
  workspaceSource: '/workspaces/:workspaceId/sources/:sourceName',
  workspaceSourceDiscovery: '/workspaces/:workspaceId/sources/discover',
  workspaceSourceInstall: '/workspaces/:workspaceId/sources/install',
  workspaceSources: '/workspaces/:workspaceId/sources',
  workspaceTrace: '/workspaces/:workspaceId/traces/:traceId',
  workspaceTraces: '/workspaces/:workspaceId/traces',
} satisfies Record<AppRouteId, string>

describe('route map', () => {
  it('exposes canonical route patterns only', () => {
    expect(Object.fromEntries([...routeMap].map(([id]) => [id, routePattern(id)]))).toEqual(
      CANONICAL_PATTERNS,
    )
  })

  it('creates React Router route objects from mapped routes', () => {
    expect(routeObjects(['home', 'workspaceSource'])).toEqual([
      { id: 'home', path: '/' },
      { id: 'workspaceSource', path: '/workspaces/:workspaceId/sources/:sourceName' },
    ])
  })

  it('generates canonical URLs for every mapped route', () => {
    const paths = {
      home: routePath('home'),
      settings: routePath('settings'),
      workspaces: routePath('workspaces'),
      workspaceFunctions: routePath('workspaceFunctions', { workspaceId: 'analytics' }),
      workspaceSchema: routePath('workspaceSchema', { workspaceId: 'analytics' }),
      workspaceSchemaTable: routePath('workspaceSchemaTable', {
        schemaName: 'github',
        tableName: 'issues',
        workspaceId: 'analytics',
      }),
      workspaceSchemaTableFunction: routePath('workspaceSchemaTableFunction', {
        functionName: 'search_issues',
        schemaName: 'github',
        workspaceId: 'analytics',
      }),
      workspaceSource: routePath('workspaceSource', {
        sourceName: 'github',
        workspaceId: 'analytics',
      }),
      workspaceSourceDiscovery: routePath('workspaceSourceDiscovery', {
        workspaceId: 'analytics',
      }),
      workspaceSourceInstall: routePath('workspaceSourceInstall', { workspaceId: 'analytics' }),
      workspaceSources: routePath('workspaceSources', { workspaceId: 'analytics' }),
      workspaceTrace: routePath('workspaceTrace', {
        traceId: 'trace_123',
        workspaceId: 'analytics',
      }),
      workspaceTraces: routePath('workspaceTraces', { workspaceId: 'analytics' }),
    } satisfies Record<AppRouteId, string>

    expect(paths).toEqual({
      home: '/',
      settings: '/settings',
      workspaces: '/workspaces',
      workspaceFunctions: '/workspaces/analytics/functions',
      workspaceSchema: '/workspaces/analytics/schema',
      workspaceSchemaTable: '/workspaces/analytics/schema/github/issues',
      workspaceSchemaTableFunction: '/workspaces/analytics/schema/github/functions/search_issues',
      workspaceSource: '/workspaces/analytics/sources/github',
      workspaceSourceDiscovery: '/workspaces/analytics/sources/discover',
      workspaceSourceInstall: '/workspaces/analytics/sources/install',
      workspaceSources: '/workspaces/analytics/sources',
      workspaceTrace: '/workspaces/analytics/traces/trace_123',
      workspaceTraces: '/workspaces/analytics/traces',
    })
  })
})

import { describe, expect, it } from 'vitest'

import { workspaceFromParams, workspacePathForCurrentSection } from './workspace-routing'

describe('workspace routing', () => {
  it('creates the route workspace and rejects a missing workspace parameter', () => {
    expect(workspaceFromParams({ workspaceId: 'analytics' }).name).toBe('analytics')

    let thrown: unknown
    try {
      workspaceFromParams({})
    } catch (error) {
      thrown = error
    }

    expect(thrown).toBeInstanceOf(Response)
    expect(thrown).toMatchObject({ status: 400, statusText: 'Invalid Workspace' })
  })

  it.each([
    ['/workspaces/default/sources/github', '/workspaces/team%20alpha/sources'],
    ['/workspaces/default/schema', '/workspaces/team%20alpha/schema'],
    ['/workspaces/default/schema/github/issues', '/workspaces/team%20alpha/schema'],
    ['/workspaces/default/functions', '/workspaces/team%20alpha/functions'],
    ['/workspaces/default/traces/trace-1', '/workspaces/team%20alpha/traces'],
    ['/settings', '/workspaces/team%20alpha/sources'],
  ])('targets the corresponding section for %s', (pathname, expectedPath) => {
    expect(workspacePathForCurrentSection('team alpha', pathname)).toBe(expectedPath)
  })
})

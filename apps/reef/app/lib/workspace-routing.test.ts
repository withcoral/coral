import { describe, expect, it } from 'vitest'

import { workspaceFromParams, workspacePathForCurrentSection } from './workspace-routing'

describe('workspace routing', () => {
  it('treats the workspace route parameter as the local workspace name', () => {
    expect(workspaceFromParams({ workspaceId: 'analytics' }).name).toBe('analytics')
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

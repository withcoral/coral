import { describe, expect, it } from 'vitest'

import { workspaceFromParams } from './workspace-routing'

describe('workspace routing', () => {
  it('treats the workspace route parameter as the local workspace name', () => {
    expect(workspaceFromParams({ workspaceId: 'analytics' }).name).toBe('analytics')
  })
})

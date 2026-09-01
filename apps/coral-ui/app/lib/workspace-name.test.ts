import { describe, expect, it } from 'vitest'

import { validateWorkspaceName } from './workspace-name'

describe('validateWorkspaceName', () => {
  it.each([
    ['', 'Workspace name is required'],
    ['team_name', 'Workspace name may only contain letters, numbers, and hyphens'],
    ['-team', 'Workspace name must not start or end with a hyphen'],
    ['team-', 'Workspace name must not start or end with a hyphen'],
    ['a'.repeat(64), 'Workspace name must be 63 characters or fewer'],
  ])('rejects invalid workspace name %j', (name, error) => {
    expect(validateWorkspaceName(name)).toBe(error)
  })
})

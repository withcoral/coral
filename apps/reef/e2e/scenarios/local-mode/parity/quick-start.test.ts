import { test } from 'vitest'
import { workspaceQuickStart } from '../../main/workspaces/quick-start.ts'

test(
  'A new local workspace opens its overview with no dialog, and its quick start copies its whole prompt',
  workspaceQuickStart,
)

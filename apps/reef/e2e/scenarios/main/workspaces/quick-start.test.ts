import { test } from 'vitest'
import { workspaceQuickStart } from './quick-start.ts'

test(
  'A new workspace opens its overview with no dialog, and its quick start copies its whole prompt',
  workspaceQuickStart,
)

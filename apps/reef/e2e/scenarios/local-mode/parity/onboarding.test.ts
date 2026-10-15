import { test } from 'vitest'
import { workspaceOnboarding } from '../../main/workspaces/onboarding.ts'

test(
  'A new local workspace opens onboarding once, copies its whole prompt, and stays closed after Dismiss',
  workspaceOnboarding,
)

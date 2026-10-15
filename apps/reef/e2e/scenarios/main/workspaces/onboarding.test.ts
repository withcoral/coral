import { test } from 'vitest'
import { workspaceOnboarding } from './onboarding.ts'

test(
  'A new workspace opens onboarding once, copies its whole prompt, and stays closed after Dismiss',
  workspaceOnboarding,
)

import { test } from 'vitest'
import { sharedAccountAtomicity } from './shared-account-atomicity.ts'

test(
  'Failed incompatible shared updates preserve the active configuration and account',
  sharedAccountAtomicity,
)

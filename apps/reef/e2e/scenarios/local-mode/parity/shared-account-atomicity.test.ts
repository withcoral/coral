import { test } from 'vitest'
import { sharedAccountAtomicity } from '../../main/providers/updates/shared-account-atomicity.ts'

test(
  'Local failed incompatible shared updates preserve the active configuration and account',
  sharedAccountAtomicity,
)

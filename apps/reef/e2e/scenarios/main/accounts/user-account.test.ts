import { test } from 'vitest'
import { separateUserAccount } from './user-account.ts'

test.for(['connect', 'retry'] as const)(
  'Separate User account: %s discovery authorization and execute once',
  separateUserAccount,
)

import { test } from 'vitest'
import { separateUserAccount } from '../../main/accounts/user-account.ts'

test.for(['connect', 'retry'] as const)(
  'Local separate User account: %s discovery authorization and execute once',
  separateUserAccount,
)

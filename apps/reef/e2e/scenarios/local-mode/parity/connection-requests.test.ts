import { test } from 'vitest'
import { connectionRequests } from '../../main/accounts/connection-requests.ts'

test(
  'Local: connect your account from Accounts to continue a waiting run, or decline on the run',
  connectionRequests,
)

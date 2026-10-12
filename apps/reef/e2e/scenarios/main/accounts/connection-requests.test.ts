import { test } from 'vitest'
import { connectionRequests } from './connection-requests.ts'

test(
  'Connect your account from Accounts to continue a waiting run, or decline on the run',
  connectionRequests,
)

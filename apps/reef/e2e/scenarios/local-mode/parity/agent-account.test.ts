import { test } from 'vitest'
import { agentAccount } from '../../main/accounts/agent-account.ts'

test(
  'Local Owner provisions the exact Agent Account; its real bearer discovers and executes',
  agentAccount,
)

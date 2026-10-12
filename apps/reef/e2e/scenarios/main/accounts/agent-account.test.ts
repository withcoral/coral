import { test } from 'vitest'
import { agentAccount } from './agent-account.ts'

test(
  'Owner provisions the exact Agent Account; its real bearer discovers and executes',
  agentAccount,
)

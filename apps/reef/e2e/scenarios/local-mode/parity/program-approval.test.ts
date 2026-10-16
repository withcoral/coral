import { test } from 'vitest'
import { approvedProgramProvider } from '../../main/providers/program-approval.ts'

test(
  "Local: a workspace owner approves their Program's new provider, then signs in at its link",
  approvedProgramProvider,
)

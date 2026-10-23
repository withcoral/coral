import { test } from 'vitest'
import { liveActions } from './live-actions.ts'

test(
  'Live mode reviews the oldest approval of a Run first, and each decision advances the review',
  liveActions,
)

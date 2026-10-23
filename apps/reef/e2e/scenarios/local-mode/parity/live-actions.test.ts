import { test } from 'vitest'
import { liveActions } from '../../main/activity/live-actions.ts'

test(
  'Local live mode reviews the oldest approval of a Run first, and each decision advances the review',
  liveActions,
)

import { test } from 'vitest'
import { sourceLocations } from '../../main/activity/source-locations.ts'

test(
  'Local Activity highlights the source line Coral stored for each tool call, log and emission',
  sourceLocations,
)

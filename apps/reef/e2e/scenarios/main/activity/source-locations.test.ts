import { test } from 'vitest'
import { sourceLocations } from './source-locations.ts'

test(
  'Activity highlights the source line Coral stored for each tool call, log and emission',
  sourceLocations,
)

import { test } from 'vitest'
import { sourceLocations } from './source-locations.ts'

test(
  'Activity highlights the source line Coral stored for each operation call, log and emission',
  sourceLocations,
)

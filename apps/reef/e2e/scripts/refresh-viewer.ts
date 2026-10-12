import { readdir } from 'node:fs/promises'
import { join } from 'node:path'

import { refreshRunPages } from '../support/artifacts.ts'
import { runsRoot } from '../support/config.ts'

// Explicitly refresh saved presentation, never browser recordings or test source.
for (const entry of await readdir(runsRoot, { withFileTypes: true })) {
  if (!entry.isDirectory()) continue
  await refreshRunPages(join(runsRoot, entry.name))
  console.log(`Refreshed Reel: ${entry.name}`)
}

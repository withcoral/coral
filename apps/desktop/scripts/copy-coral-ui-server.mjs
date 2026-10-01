import { cp, mkdir, rm } from 'node:fs/promises'
import { resolve } from 'node:path'

const desktopRoot = resolve(import.meta.dirname, '..')
const repoRoot = resolve(desktopRoot, '..', '..')
const coralUIServerBuild = resolve(repoRoot, 'apps', 'coral-ui', 'build', 'server')
const outputDir = resolve(desktopRoot, 'out', 'coral-ui-server')

await rm(outputDir, { recursive: true, force: true })
await mkdir(outputDir, { recursive: true })
await cp(coralUIServerBuild, outputDir, { recursive: true })

console.log(`[copy-coral-ui-server] staged ${coralUIServerBuild} -> ${outputDir}`)

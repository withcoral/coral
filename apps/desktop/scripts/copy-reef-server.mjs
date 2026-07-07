import { cp, mkdir, rm } from 'node:fs/promises'
import { resolve } from 'node:path'

const desktopRoot = resolve(import.meta.dirname, '..')
const repoRoot = resolve(desktopRoot, '..', '..')
const reefServerBuild = resolve(repoRoot, 'apps', 'reef', 'build', 'server')
const outputDir = resolve(desktopRoot, 'out', 'reef-server')

await rm(outputDir, { recursive: true, force: true })
await mkdir(outputDir, { recursive: true })
await cp(reefServerBuild, outputDir, { recursive: true })

console.log(`[copy-reef-server] staged ${reefServerBuild} -> ${outputDir}`)

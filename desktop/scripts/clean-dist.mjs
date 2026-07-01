import { rm } from 'node:fs/promises'
import { resolve } from 'node:path'

const distDir = resolve(import.meta.dirname, '..', 'dist')

await rm(distDir, { force: true, recursive: true })
console.log(`[clean-dist] removed ${distDir}`)

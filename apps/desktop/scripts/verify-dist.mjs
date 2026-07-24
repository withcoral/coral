#!/usr/bin/env node
// Verifies a release-shaped desktop dist directory. Shared by the Validate
// workflow's Desktop macOS package job and the release workflow so the two
// checks cannot drift: exactly one DMG and one ZIP, non-empty update
// metadata, every file latest-mac.yml references present, and a blockmap
// next to the ZIP (electron-updater fetches <file>.blockmap for
// differential updates).
import { existsSync, readFileSync, readdirSync } from 'node:fs'
import { join, resolve } from 'node:path'

const distDir = resolve(process.argv[2] ?? 'apps/desktop/dist')

function fail(message) {
  console.error(`[verify-dist] ${message}`)
  process.exit(1)
}

if (!existsSync(distDir)) fail(`missing dist directory: ${distDir}`)
const entries = readdirSync(distDir)

const dmgs = entries.filter((f) => f.startsWith('coral-desktop-') && f.endsWith('.dmg'))
const zips = entries.filter((f) => f.startsWith('coral-desktop-') && f.endsWith('.zip'))
if (dmgs.length !== 1 || zips.length !== 1) {
  fail(
    `expected exactly one desktop DMG and one desktop ZIP, got DMGs: [${dmgs}] ZIPs: [${zips}]`,
  )
}

const metadataPath = join(distDir, 'latest-mac.yml')
if (!existsSync(metadataPath)) fail('missing latest-mac.yml')
const metadata = readFileSync(metadataPath, 'utf8')
if (!metadata.trim()) fail('latest-mac.yml is empty')

// latest-mac.yml lists its update assets as `url:` entries plus a legacy
// top-level `path:`; a file the metadata references but the dist lacks would
// 404 for every updater in the wild.
const referenced = [
  ...new Set(
    [...metadata.matchAll(/^\s*(?:-\s*)?(?:url|path):\s*(\S+)\s*$/gm)].map((match) => match[1]),
  ),
]
if (referenced.length === 0) fail('latest-mac.yml references no update assets')
if (!referenced.includes(zips[0])) fail(`latest-mac.yml does not reference ${zips[0]}`)
for (const file of referenced) {
  if (!entries.includes(file)) fail(`latest-mac.yml references a missing file: ${file}`)
}

const zipBlockmap = `${zips[0]}.blockmap`
if (!entries.includes(zipBlockmap)) {
  fail(`missing ${zipBlockmap}; differential updates need the ZIP blockmap`)
}

console.log(
  `[verify-dist] ok: ${dmgs[0]}, ${zips[0]} (+${zipBlockmap}), latest-mac.yml references ${referenced.join(', ')}`,
)

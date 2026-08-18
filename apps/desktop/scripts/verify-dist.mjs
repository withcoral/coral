#!/usr/bin/env node
// Verifies a release-shaped desktop dist directory. Shared by the Desktop
// package workflow and the release workflow so the two checks cannot drift.
//
// The artifact shape is per platform, and only macOS publishes an update feed
// (see desktopUpdatesSupported in src/main/auto-update.ts):
//
//   mac    exactly one DMG and one ZIP, a blockmap next to the ZIP
//          (electron-updater fetches <file>.blockmap for differential
//          updates), and a non-empty latest-mac.yml whose every referenced
//          file is present.
//   linux  at least one AppImage and one deb, and no update feed.
import { existsSync, readFileSync, readdirSync } from 'node:fs'
import { join, resolve } from 'node:path'

const PLATFORMS = ['mac', 'linux']

const distDir = resolve(process.argv[2] ?? 'apps/desktop/dist')
const platform = process.argv[3] ?? 'mac'

function fail(message) {
  console.error(`[verify-dist] ${message}`)
  process.exit(1)
}

if (!PLATFORMS.includes(platform)) {
  fail(`unknown platform '${platform}'; expected one of ${PLATFORMS.join(', ')}`)
}
if (!existsSync(distDir)) fail(`missing dist directory: ${distDir}`)
const entries = readdirSync(distDir)

function artifacts(extension) {
  return entries.filter((f) => f.startsWith('coral-desktop-') && f.endsWith(extension))
}

function verifyMac() {
  const dmgs = artifacts('.dmg')
  const zips = artifacts('.zip')
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

  return `${dmgs[0]}, ${zips[0]} (+${zipBlockmap}), latest-mac.yml references ${referenced.join(', ')}`
}

function verifyLinux() {
  const appImages = artifacts('.AppImage')
  const debs = artifacts('.deb')
  if (appImages.length === 0 || debs.length === 0) {
    fail(
      `expected at least one desktop AppImage and one desktop deb, got AppImages: [${appImages}] debs: [${debs}]`,
    )
  }
  // Linux has no updater, so a feed here would be published and then served to
  // nobody. Treat one as a packaging mistake.
  const feeds = entries.filter((f) => /^latest(-\w+)?\.yml$/.test(f))
  if (feeds.length > 0) {
    fail(`linux ships no updater, but the build produced update metadata: ${feeds.join(', ')}`)
  }
  return [...appImages, ...debs].join(', ')
}

const summary = { mac: verifyMac, linux: verifyLinux }[platform]()

console.log(`[verify-dist] ok (${platform}): ${summary}`)

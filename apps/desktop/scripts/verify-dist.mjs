#!/usr/bin/env node
// Verifies a release-shaped desktop dist directory. Shared by the Desktop
// package workflow and the release workflow so the two checks cannot drift.
//
// The artifact shape is per platform, and each platform publishes the update
// feed its updater reads (see desktopUpdatesSupported in
// src/main/auto-update.ts):
//
//   mac    exactly one DMG and one ZIP, a blockmap next to the ZIP
//          (electron-updater fetches <file>.blockmap for differential
//          updates), and a non-empty latest-mac.yml whose every referenced
//          file is present.
//   linux  exactly one AppImage and one deb, and a non-empty latest-linux.yml
//          that references the AppImage. The deb has no updater, and the
//          AppImage carries its blockmap inside the image, so no separate
//          blockmap file exists here.
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

// Returns the files the feed lists, after checking that the dist holds each of
// them and that `required` is among them. An asset the feed names but the
// release lacks would 404 for every updater in the wild.
function verifyFeed(feedName, required) {
  const metadataPath = join(distDir, feedName)
  if (!existsSync(metadataPath)) fail(`missing ${feedName}`)
  const metadata = readFileSync(metadataPath, 'utf8')
  if (!metadata.trim()) fail(`${feedName} is empty`)

  // A feed lists its update assets as `url:` entries plus a legacy top-level
  // `path:`.
  const referenced = [
    ...new Set(
      [...metadata.matchAll(/^\s*(?:-\s*)?(?:url|path):\s*(\S+)\s*$/gm)].map((match) => match[1]),
    ),
  ]
  if (referenced.length === 0) fail(`${feedName} references no update assets`)
  if (!referenced.includes(required)) fail(`${feedName} does not reference ${required}`)
  for (const file of referenced) {
    if (!entries.includes(file)) fail(`${feedName} references a missing file: ${file}`)
  }
  return referenced
}

function verifyMac() {
  const dmgs = artifacts('.dmg')
  const zips = artifacts('.zip')
  if (dmgs.length !== 1 || zips.length !== 1) {
    fail(
      `expected exactly one desktop DMG and one desktop ZIP, got DMGs: [${dmgs}] ZIPs: [${zips}]`,
    )
  }

  const referenced = verifyFeed('latest-mac.yml', zips[0])

  const zipBlockmap = `${zips[0]}.blockmap`
  if (!entries.includes(zipBlockmap)) {
    fail(`missing ${zipBlockmap}; differential updates need the ZIP blockmap`)
  }

  return `${dmgs[0]}, ${zips[0]} (+${zipBlockmap}), latest-mac.yml references ${referenced.join(', ')}`
}

function verifyLinux() {
  const appImages = artifacts('.AppImage')
  const debs = artifacts('.deb')
  if (appImages.length !== 1 || debs.length !== 1) {
    fail(
      `expected exactly one desktop AppImage and one desktop deb, got AppImages: [${appImages}] debs: [${debs}]`,
    )
  }

  const referenced = verifyFeed('latest-linux.yml', appImages[0])

  return `${appImages[0]}, ${debs[0]}, latest-linux.yml references ${referenced.join(', ')}`
}

const summary = { mac: verifyMac, linux: verifyLinux }[platform]()

console.log(`[verify-dist] ok (${platform}): ${summary}`)

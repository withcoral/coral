#!/usr/bin/env node
// Verifies a release-shaped desktop dist directory. Shared by the Desktop
// package workflow and the release workflow so the two checks cannot drift.
//
//   mac    one DMG, one ZIP, the ZIP's blockmap (electron-updater fetches it for
//          differential updates), and latest-mac.yml.
//   linux  one AppImage, one deb, and latest-linux.yml. The deb has no updater
//          and must stay out of the feed, and the AppImage carries its blockmap
//          inside the image.
//   win    one NSIS installer .exe, and neither a feed nor a blockmap, because
//          Windows ships no updater.
import { existsSync, readFileSync, readdirSync } from 'node:fs'
import { join, resolve } from 'node:path'

const PLATFORMS = ['mac', 'linux', 'win']

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

// Returns the files the feed lists, after three checks: the dist holds each of
// them, `required` is among them, and the top-level `path:` names `required`.
//
// A feed may legitimately list more than one asset — writeUpdateInfoFiles()
// merges every target that reports update info into the one file, so
// latest-mac.yml carries the DMG next to the ZIP. Which one an updater installs
// comes from its own findFile() preference, except for the top-level `path:`,
// the field electron-updater 1.x fell back to and app-builder-lib still fills
// from whichever task sorted first. That sort breaks ties by zip-vs-non-zip and
// then arch, so two same-arch non-zip artifacts in one feed leave it up to
// whichever hashed first. Pinning it here is what makes the feed deterministic.
//
// `forbidden` extensions are packages that must never appear at all, because no
// updater in the fleet can install them.
function verifyFeed(feedName, required, forbidden = []) {
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
  const banned = referenced.filter((file) => forbidden.some((ext) => file.endsWith(ext)))
  if (banned.length > 0) {
    fail(`${feedName} references packages no updater can install: [${banned}]`)
  }

  // Unindented, so a `url:` inside `files:` cannot satisfy it.
  const legacyPath = metadata.match(/^path:\s*(\S+)\s*$/m)?.[1]
  if (legacyPath !== required) {
    fail(`${feedName} top-level path is '${legacyPath ?? '<missing>'}', expected ${required}`)
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

  // The deb belongs to dpkg: a DebUpdater would run `dpkg -i` under pkexec and
  // leave the running AppImage in place. `deb.publish: null` keeps it out of the
  // feed; this fails the build if that ever stops working.
  const referenced = verifyFeed('latest-linux.yml', appImages[0], ['.deb'])

  return `${appImages[0]}, ${debs[0]}, latest-linux.yml references ${referenced.join(', ')}`
}

// Windows has no updater, so a feed or a blockmap here is a packaging mistake:
// both would be published and then served to nobody. NSIS gates the blockmap on
// `differentialPackage`, not on `publish`, so `publish: null` alone does not
// suppress it — see the nsis block in electron-builder.config.ts.
function verifyWindows() {
  const installers = artifacts('.exe')
  if (installers.length !== 1) {
    fail(`expected exactly one desktop installer .exe, got [${installers}]`)
  }

  const feeds = entries.filter((f) => /^latest(-\w+)?\.yml$/.test(f))
  if (feeds.length > 0) {
    fail(`windows ships no updater, but the build produced update metadata: ${feeds.join(', ')}`)
  }
  const blockmaps = entries.filter((f) => f.endsWith('.blockmap'))
  if (blockmaps.length > 0) {
    fail(`windows ships no updater, but the build produced blockmaps: ${blockmaps.join(', ')}`)
  }

  return installers[0]
}

const summary = { mac: verifyMac, linux: verifyLinux, win: verifyWindows }[platform]()

console.log(`[verify-dist] ok (${platform}): ${summary}`)

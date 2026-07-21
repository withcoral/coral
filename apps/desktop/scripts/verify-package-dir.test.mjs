import assert from 'node:assert/strict'
import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test, { afterEach } from 'node:test'

import { createPackage } from '@electron/asar'

import { verifyPackageDir } from './verify-package-dir.mjs'

const fixtures = new Set()

afterEach(async () => {
  await Promise.all(
    [...fixtures].map((fixture) => rm(fixture, { recursive: true, force: true })),
  )
  fixtures.clear()
})

function thinMachO(cpuType = 0x0100000c) {
  const header = Buffer.alloc(32)
  header.writeUInt32LE(0xfeedfacf, 0)
  header.writeUInt32LE(cpuType, 4)
  return header
}

async function createFixture(options = {}) {
  const root = await mkdtemp(join(tmpdir(), 'coral-package-dir-fixture-'))
  fixtures.add(root)
  const distDir = join(root, 'dist')
  await mkdir(distDir, { recursive: true })
  if (options.missingApp) return { root, distDir }

  const appPath = join(distDir, 'mac-arm64', 'Coral.app')
  const contentsPath = join(appPath, 'Contents')
  const resourcesPath = join(contentsPath, 'Resources')
  const electronExecutablePath = join(contentsPath, 'MacOS', 'Coral')
  const sidecarPath = join(resourcesPath, 'coral', 'coral')
  const reefAssetsPath = join(resourcesPath, 'app', 'assets')
  await Promise.all([
    mkdir(join(contentsPath, 'Frameworks'), { recursive: true }),
    mkdir(join(contentsPath, 'MacOS'), { recursive: true }),
    mkdir(join(resourcesPath, 'coral'), { recursive: true }),
    mkdir(reefAssetsPath, { recursive: true }),
  ])
  await Promise.all([
    writeFile(join(contentsPath, 'Info.plist'), '<plist/>'),
    writeFile(electronExecutablePath, 'electron'),
    writeFile(sidecarPath, thinMachO(options.cpuType)),
    writeFile(join(reefAssetsPath, 'entry.js'), 'reef client'),
  ])
  await Promise.all([chmod(electronExecutablePath, 0o755), chmod(sidecarPath, 0o755)])

  const archiveSource = join(root, 'archive-source')
  const archiveEntries = {
    'out/main/index.js': 'main',
    'out/preload/index.cjs': 'preload',
    'out/reef-server/index.js': 'reef server',
  }
  for (const [entry, contents] of Object.entries(archiveEntries)) {
    if (options.omitArchiveEntry === entry) continue
    const path = join(archiveSource, entry)
    if (options.directoryArchiveEntry === entry) {
      await mkdir(path, { recursive: true })
      continue
    }
    await mkdir(join(path, '..'), { recursive: true })
    await writeFile(path, contents)
  }
  await createPackage(archiveSource, join(resourcesPath, 'app.asar'))

  return {
    root,
    distDir,
    appPath,
    electronExecutablePath,
    reefAssetsPath,
    resourcesPath,
    sidecarPath,
  }
}

test('accepts an unpacked app with Electron, Reef, and a thin arm64 sidecar', async () => {
  const fixture = await createFixture()

  const result = await verifyPackageDir(fixture.distDir)

  assert.equal(result.appPath, fixture.appPath)
  assert.equal(result.sidecarPath, fixture.sidecarPath)
  assert.equal(result.architecture, 'arm64')
})

test('rejects missing app output', async () => {
  const fixture = await createFixture({ missingApp: true })

  await assert.rejects(() => verifyPackageDir(fixture.distDir), /expected exactly one Coral\.app/)
})

test('rejects a missing packaged sidecar', async () => {
  const fixture = await createFixture()
  await rm(fixture.sidecarPath)

  await assert.rejects(() => verifyPackageDir(fixture.distDir), /missing packaged Coral sidecar/)
})

test('rejects a sidecar without executable permission', async () => {
  const fixture = await createFixture()
  await chmod(fixture.sidecarPath, 0o644)

  await assert.rejects(() => verifyPackageDir(fixture.distDir), /sidecar is not executable/)
})

test('rejects missing Electron main and preload outputs', async (t) => {
  for (const [entry, description] of [
    ['out/main/index.js', 'Electron main output'],
    ['out/preload/index.cjs', 'Electron preload output'],
  ]) {
    await t.test(description, async () => {
      const fixture = await createFixture({ omitArchiveEntry: entry })
      await assert.rejects(() => verifyPackageDir(fixture.distDir), new RegExp(description))
    })
  }
})

test('rejects an ASAR directory masquerading as an Electron output file', async () => {
  const fixture = await createFixture({ directoryArchiveEntry: 'out/main/index.js' })

  await assert.rejects(
    () => verifyPackageDir(fixture.distDir),
    /Electron main output must be a non-empty file/,
  )
})

test('rejects missing Reef client assets', async () => {
  const fixture = await createFixture()
  await rm(fixture.reefAssetsPath, { recursive: true })

  await assert.rejects(() => verifyPackageDir(fixture.distDir), /missing Reef client assets directory/)
})

test('rejects missing Reef server assets', async () => {
  const fixture = await createFixture({ omitArchiveEntry: 'out/reef-server/index.js' })

  await assert.rejects(() => verifyPackageDir(fixture.distDir), /missing Reef server output/)
})

test('rejects a non-arm64 sidecar', async () => {
  const fixture = await createFixture({ cpuType: 0x01000007 })

  await assert.rejects(() => verifyPackageDir(fixture.distDir), /must be thin arm64; found x86_64/)
})

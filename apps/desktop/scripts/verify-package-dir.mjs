#!/usr/bin/env node
import { statFile } from '@electron/asar'
import { constants } from 'node:fs'
import { access, open, readdir, stat } from 'node:fs/promises'
import { basename, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const MACH_64_MAGIC = 0xfeedfacf
const MACH_32_MAGIC = 0xfeedface
const FAT_MAGICS = new Set([0xcafebabe, 0xcafebabf, 0xbebafeca, 0xbfbafeca])
const CPU_TYPE_ARM64 = 0x0100000c
const CPU_TYPE_X86_64 = 0x01000007

async function requireDirectory(path, description) {
  let metadata
  try {
    metadata = await stat(path)
  } catch {
    throw new Error(`missing ${description}: ${path}`)
  }
  if (!metadata.isDirectory()) throw new Error(`${description} is not a directory: ${path}`)
}

async function requireNonEmptyFile(path, description) {
  let metadata
  try {
    metadata = await stat(path)
  } catch {
    throw new Error(`missing ${description}: ${path}`)
  }
  if (!metadata.isFile() || metadata.size === 0) {
    throw new Error(`${description} must be a non-empty regular file: ${path}`)
  }
  return metadata
}

async function requireExecutable(path, description, metadata) {
  if ((metadata.mode & 0o111) === 0) throw new Error(`${description} is not executable: ${path}`)
  try {
    await access(path, constants.X_OK)
  } catch {
    throw new Error(`${description} is not executable: ${path}`)
  }
}

async function findTopLevelAppBundles(directory) {
  await requireDirectory(directory, 'unpacked package directory')
  const found = []

  async function visit(current) {
    for (const entry of await readdir(current, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue
      const path = join(current, entry.name)
      if (entry.name.endsWith('.app')) {
        found.push(path)
      } else {
        await visit(path)
      }
    }
  }

  await visit(directory)
  return found
}

function requireArchiveFile(archivePath, entry, description) {
  let metadata
  try {
    metadata = statFile(archivePath, entry)
  } catch {
    throw new Error(`missing ${description} in app.asar: ${entry}`)
  }
  if (!Number.isSafeInteger(metadata.size) || metadata.size <= 0) {
    throw new Error(`${description} must be a non-empty file in app.asar: ${entry}`)
  }
}

async function containsNonEmptyFile(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name)
    if (entry.isDirectory() && (await containsNonEmptyFile(path))) return true
    if (entry.isFile() && (await stat(path)).size > 0) return true
  }
  return false
}

export async function readThinMachOArchitecture(path) {
  const handle = await open(path, 'r')
  try {
    const header = Buffer.alloc(8)
    const { bytesRead } = await handle.read(header, 0, header.length, 0)
    if (bytesRead < header.length) throw new Error('file is too small to contain a Mach-O header')

    const magicLittleEndian = header.readUInt32LE(0)
    const magicBigEndian = header.readUInt32BE(0)
    if (FAT_MAGICS.has(magicLittleEndian) || FAT_MAGICS.has(magicBigEndian)) {
      throw new Error('binary is universal; expected a thin arm64 Mach-O executable')
    }

    let cpuType
    if (magicLittleEndian === MACH_64_MAGIC || magicLittleEndian === MACH_32_MAGIC) {
      cpuType = header.readUInt32LE(4)
    } else if (magicBigEndian === MACH_64_MAGIC || magicBigEndian === MACH_32_MAGIC) {
      cpuType = header.readUInt32BE(4)
    } else {
      throw new Error('file is not a Mach-O executable')
    }

    if (cpuType === CPU_TYPE_ARM64) return 'arm64'
    if (cpuType === CPU_TYPE_X86_64) return 'x86_64'
    return `unknown CPU type 0x${cpuType.toString(16)}`
  } finally {
    await handle.close()
  }
}

export async function verifyPackageDir(packageDirectory) {
  const distDir = resolve(packageDirectory)
  const appBundles = await findTopLevelAppBundles(distDir)
  if (appBundles.length !== 1 || basename(appBundles[0]) !== 'Coral.app') {
    const found = appBundles.length === 0 ? 'none' : appBundles.map((path) => basename(path)).join(', ')
    throw new Error(`expected exactly one Coral.app in ${distDir}; found ${found}`)
  }

  const appPath = appBundles[0]
  const contentsPath = join(appPath, 'Contents')
  const resourcesPath = join(contentsPath, 'Resources')
  await requireDirectory(contentsPath, 'Electron app Contents directory')
  await requireDirectory(join(contentsPath, 'Frameworks'), 'Electron app Frameworks directory')
  await requireDirectory(resourcesPath, 'Electron app Resources directory')
  await requireNonEmptyFile(join(contentsPath, 'Info.plist'), 'Electron app Info.plist')

  const electronExecutablePath = join(contentsPath, 'MacOS', 'Coral')
  const electronExecutable = await requireNonEmptyFile(
    electronExecutablePath,
    'Electron app executable',
  )
  await requireExecutable(electronExecutablePath, 'Electron app executable', electronExecutable)

  const archivePath = join(resourcesPath, 'app.asar')
  await requireNonEmptyFile(archivePath, 'packaged app archive')
  requireArchiveFile(archivePath, 'out/main/index.js', 'Electron main output')
  requireArchiveFile(archivePath, 'out/preload/index.cjs', 'Electron preload output')
  requireArchiveFile(archivePath, 'out/reef-server/index.js', 'Reef server output')

  const reefAssetsPath = join(resourcesPath, 'app', 'assets')
  await requireDirectory(reefAssetsPath, 'Reef client assets directory')
  if (!(await containsNonEmptyFile(reefAssetsPath))) {
    throw new Error(`Reef client assets directory contains no non-empty files: ${reefAssetsPath}`)
  }

  const sidecarPath = join(resourcesPath, 'coral', 'coral')
  const sidecar = await requireNonEmptyFile(sidecarPath, 'packaged Coral sidecar')
  await requireExecutable(sidecarPath, 'packaged Coral sidecar', sidecar)
  let architecture
  try {
    architecture = await readThinMachOArchitecture(sidecarPath)
  } catch (error) {
    throw new Error(`packaged Coral sidecar is not thin arm64: ${error.message}`)
  }
  if (architecture !== 'arm64') {
    throw new Error(`packaged Coral sidecar must be thin arm64; found ${architecture}`)
  }

  return { appPath, archivePath, sidecarPath, architecture }
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : undefined
if (invokedPath === fileURLToPath(import.meta.url)) {
  const packageDirectory = process.argv[2] ?? fileURLToPath(new URL('../dist', import.meta.url))
  try {
    const result = await verifyPackageDir(packageDirectory)
    console.log(
      `[verify-package-dir] ok: ${result.appPath}; main, preload, Reef client/server, and ${result.architecture} Coral sidecar verified`,
    )
  } catch (error) {
    console.error(`[verify-package-dir] ${error.message}`)
    process.exitCode = 1
  }
}

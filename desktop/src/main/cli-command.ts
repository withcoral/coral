import { execFile } from 'node:child_process'
import { constants } from 'node:fs'
import { access, lstat, mkdir, readlink, rm, symlink } from 'node:fs/promises'
import { delimiter, join } from 'node:path'
import { promisify } from 'node:util'
import { app } from 'electron'
import type { CliInstallResult } from '../shared/types'
import { externalCoralCommandPath } from './sidecar'

const execFileAsync = promisify(execFile)
const COMMAND_NAME = 'coral'
const READ_ONLY_PATH_DIRS = new Set(['/bin', '/sbin', '/usr/bin', '/usr/sbin'])

function isNotFound(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false
  return 'code' in error && (error as NodeJS.ErrnoException).code === 'ENOENT'
}

function splitPath(value: string | undefined): string[] {
  return (value ?? '')
    .split(delimiter)
    .map((entry) => entry.trim())
    .filter(Boolean)
}

async function detectedShellPathDirs(): Promise<string[]> {
  const shell = process.env.SHELL || '/bin/zsh'
  try {
    const { stdout } = await execFileAsync(shell, ['-lc', 'printf "%s" "$PATH"'], {
      timeout: 2_000,
    })
    return splitPath(stdout)
  } catch {
    return []
  }
}

function unique(values: string[]): string[] {
  return [...new Set(values)]
}

function hasPathDir(pathDirs: string[], dir: string): boolean {
  return pathDirs.includes(dir)
}

function candidateCommandDirs(home: string, pathDirs: string[]): string[] {
  const homeLocalBin = join(home, '.local', 'bin')
  const homeBin = join(home, 'bin')
  const candidates = [
    '/opt/homebrew/bin',
    '/usr/local/bin',
    ...(hasPathDir(pathDirs, homeLocalBin) ? [homeLocalBin] : []),
    ...(hasPathDir(pathDirs, homeBin) ? [homeBin] : []),
  ]
  return unique(
    candidates.filter((dir) => hasPathDir(pathDirs, dir) && !READ_ONLY_PATH_DIRS.has(dir)),
  )
}

async function canWriteCommandDir(dir: string): Promise<boolean> {
  try {
    if (dir.startsWith(`${app.getPath('home')}/`)) await mkdir(dir, { recursive: true })
    await access(dir, constants.W_OK)
    return true
  } catch {
    return false
  }
}

async function installSymlink(commandPath: string, targetPath: string): Promise<void> {
  try {
    const stat = await lstat(commandPath)
    if (!stat.isSymbolicLink()) {
      throw new Error(`${commandPath} already exists and is not managed by Coral Desktop.`)
    }
    const currentTarget = await readlink(commandPath)
    if (currentTarget === targetPath) return
    await rm(commandPath)
  } catch (error) {
    if (!isNotFound(error)) throw error
  }

  await symlink(targetPath, commandPath)
}

export async function installCliCommand(): Promise<CliInstallResult> {
  if (process.platform !== 'darwin') {
    throw new Error('Installing the Coral command from the desktop app is currently supported on macOS only.')
  }

  const targetPath = await externalCoralCommandPath()
  const home = app.getPath('home')
  const pathDirs = unique([...splitPath(process.env.PATH), ...(await detectedShellPathDirs())])

  for (const dir of candidateCommandDirs(home, pathDirs)) {
    if (!(await canWriteCommandDir(dir))) continue

    const commandPath = join(dir, COMMAND_NAME)
    await installSymlink(commandPath, targetPath)
    return {
      commandPath,
      installKind: 'symlink',
      onPath: true,
      targetPath,
    }
  }

  throw new Error(
    'No writable command directory was found on PATH. Add a writable directory such as /opt/homebrew/bin, /usr/local/bin, ~/.local/bin, or ~/bin to PATH, then try again.',
  )
}

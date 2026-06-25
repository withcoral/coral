import { execFile } from 'node:child_process'
import { constants } from 'node:fs'
import {
  access,
  lstat,
  mkdir,
  readFile,
  readlink,
  rm,
  symlink,
  writeFile,
} from 'node:fs/promises'
import { delimiter, dirname, join } from 'node:path'
import { promisify } from 'node:util'
import { app } from 'electron'
import type { CliInstallResult } from '../shared/types'
import { externalCoralPath } from './sidecar'

const execFileAsync = promisify(execFile)
const COMMAND_NAME = process.platform === 'win32' ? 'coral.cmd' : 'coral'
const ALIAS_START = '# >>> Coral Desktop CLI alias >>>'
const ALIAS_END = '# <<< Coral Desktop CLI alias <<<'
const PATH_START = '# >>> Coral Desktop CLI path >>>'
const PATH_END = '# <<< Coral Desktop CLI path <<<'
const READ_ONLY_PATH_DIRS = new Set(['/bin', '/sbin', '/usr/bin', '/usr/sbin'])

function isNotFound(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false
  return 'code' in error && (error as NodeJS.ErrnoException).code === 'ENOENT'
}

async function readText(path: string): Promise<string> {
  try {
    return await readFile(path, 'utf8')
  } catch (error) {
    if (isNotFound(error)) return ''
    throw error
  }
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`
}

function pathBlock(commandDir: string): string {
  return [
    PATH_START,
    `coral_desktop_bin=${shellQuote(commandDir)}`,
    'case ":$PATH:" in',
    '  *":$coral_desktop_bin:"*) ;;',
    '  *) export PATH="$coral_desktop_bin:$PATH" ;;',
    'esac',
    'unset coral_desktop_bin',
    PATH_END,
  ].join('\n')
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function removeManagedBlock(raw: string, start: string, end: string): string {
  const pattern = new RegExp(`${escapeRegExp(start)}[\\s\\S]*?${escapeRegExp(end)}\\n?`, 'm')
  return raw.replace(pattern, '').trimEnd()
}

function replaceManagedBlock(raw: string, block: string, start: string, end: string): string {
  const withoutBlock = removeManagedBlock(raw, start, end)
  return `${withoutBlock}${withoutBlock ? '\n\n' : ''}${block}\n`
}

async function removeManagedShellBlocks(path: string): Promise<void> {
  const raw = await readText(path)
  if (!raw) return
  const next = removeManagedBlock(
    removeManagedBlock(raw, ALIAS_START, ALIAS_END),
    PATH_START,
    PATH_END,
  )
  if (next === raw.trimEnd()) return
  await writeFile(path, `${next}${next ? '\n' : ''}`)
}

async function writeManagedPath(path: string, commandDir: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true })
  const withoutAlias = removeManagedBlock(await readText(path), ALIAS_START, ALIAS_END)
  await writeFile(path, replaceManagedBlock(withoutAlias, pathBlock(commandDir), PATH_START, PATH_END))
}

function shellConfigPaths(home: string): string[] {
  const paths = [join(home, '.zshrc')]
  if (process.platform !== 'darwin') {
    paths.push(join(home, '.bashrc'), join(home, '.profile'))
  }
  return paths
}

function splitPath(value: string | undefined): string[] {
  return (value ?? '')
    .split(delimiter)
    .map((entry) => entry.trim())
    .filter(Boolean)
}

async function detectedShellPathDirs(): Promise<string[]> {
  const shell = process.env.SHELL || (process.platform === 'darwin' ? '/bin/zsh' : '/bin/sh')
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
    ...(hasPathDir(pathDirs, homeLocalBin) ? [homeLocalBin] : []),
    ...(hasPathDir(pathDirs, homeBin) ? [homeBin] : []),
    '/usr/local/bin',
    ...(hasPathDir(pathDirs, '/opt/homebrew/bin') ? ['/opt/homebrew/bin'] : []),
  ]
  return unique(candidates.filter((dir) => !READ_ONLY_PATH_DIRS.has(dir)))
}

async function canWriteCommandDir(dir: string, create: boolean): Promise<boolean> {
  try {
    if (create) await mkdir(dir, { recursive: true })
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

async function installUnixCliCommand(targetPath: string): Promise<CliInstallResult> {
  const home = app.getPath('home')
  const shellPathDirs = unique([...splitPath(process.env.PATH), ...(await detectedShellPathDirs())])

  for (const dir of candidateCommandDirs(home, shellPathDirs)) {
    const create = dir.startsWith(`${home}/`)
    if (!(await canWriteCommandDir(dir, create))) continue

    const commandPath = join(dir, COMMAND_NAME)
    await installSymlink(commandPath, targetPath)
    await Promise.all(shellConfigPaths(home).map(removeManagedShellBlocks))
    return {
      commandPath,
      installKind: 'symlink',
      onPath: hasPathDir(shellPathDirs, dir),
      targetPath,
    }
  }

  const commandDir = join(home, '.local', 'bin')
  const commandPath = join(commandDir, COMMAND_NAME)
  const shellConfigPath = join(home, '.zshrc')

  await mkdir(commandDir, { recursive: true })
  await installSymlink(commandPath, targetPath)
  await writeManagedPath(shellConfigPath, commandDir)

  return {
    commandPath,
    installKind: 'path',
    onPath: true,
    shellConfigPath,
    targetPath,
  }
}

async function ensureWindowsUserPath(dir: string): Promise<void> {
  const script = [
    '$dir = [System.IO.Path]::GetFullPath($args[0])',
    '$current = [Environment]::GetEnvironmentVariable("Path", "User")',
    'if ([string]::IsNullOrWhiteSpace($current)) { $current = "" }',
    '$parts = $current -split ";" | Where-Object { $_ -and $_.Trim().Length -gt 0 }',
    'if (-not ($parts | Where-Object { [System.IO.Path]::GetFullPath($_) -eq $dir })) {',
    '  [Environment]::SetEnvironmentVariable("Path", "$dir;$current", "User")',
    '}',
  ].join('\n')

  await execFileAsync('powershell.exe', ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-Command', script, dir])
}

async function installWindowsCli(targetPath: string): Promise<CliInstallResult> {
  const dir = join(app.getPath('home'), '.local', 'bin')
  await mkdir(dir, { recursive: true })
  const commandPath = join(dir, COMMAND_NAME)
  await writeFile(commandPath, `@echo off\r\n"${targetPath}" %*\r\n`)
  await ensureWindowsUserPath(dir)
  return {
    commandPath,
    installKind: 'cmd',
    onPath: true,
    targetPath,
  }
}

export async function installCliCommand(): Promise<CliInstallResult> {
  const targetPath = await externalCoralPath()
  return process.platform === 'win32' ? installWindowsCli(targetPath) : installUnixCliCommand(targetPath)
}

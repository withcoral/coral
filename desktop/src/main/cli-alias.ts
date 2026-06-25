import { execFile } from 'node:child_process'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { promisify } from 'node:util'
import { app } from 'electron'
import type { CliInstallResult } from '../shared/types'
import { externalCoralPath } from './sidecar'

const execFileAsync = promisify(execFile)
const ALIAS_START = '# >>> Coral Desktop CLI alias >>>'
const ALIAS_END = '# <<< Coral Desktop CLI alias <<<'

async function readText(path: string): Promise<string> {
  try {
    return await readFile(path, 'utf8')
  } catch {
    return ''
  }
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`
}

function aliasBlock(targetPath: string): string {
  return [
    ALIAS_START,
    `alias coral=${shellQuote(targetPath)}`,
    ALIAS_END,
  ].join('\n')
}

function replaceManagedBlock(raw: string, block: string): string {
  const pattern = new RegExp(
    `${escapeRegExp(ALIAS_START)}[\\s\\S]*?${escapeRegExp(ALIAS_END)}`,
    'm',
  )
  if (pattern.test(raw)) return `${raw.replace(pattern, block).trimEnd()}\n`
  return `${raw.trimEnd()}${raw.trimEnd() ? '\n\n' : ''}${block}\n`
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

async function writeManagedAlias(path: string, targetPath: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true })
  await writeFile(path, replaceManagedBlock(await readText(path), aliasBlock(targetPath)))
}

async function installUnixShellAlias(targetPath: string): Promise<CliInstallResult> {
  const home = app.getPath('home')
  const primaryConfigPath = join(home, '.zshrc')

  await writeManagedAlias(primaryConfigPath, targetPath)

  if (process.platform !== 'darwin') {
    await writeManagedAlias(join(home, '.bashrc'), targetPath)
  }

  return {
    commandPath: 'coral',
    installKind: 'alias',
    onPath: true,
    shellConfigPath: primaryConfigPath,
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
  const commandPath = join(dir, 'coral.cmd')
  await writeFile(commandPath, `@echo off\r\n"${targetPath}" %*\r\n`)
  await ensureWindowsUserPath(dir)
  return {
    commandPath,
    installKind: 'cmd',
    onPath: true,
    targetPath,
  }
}

export async function installCliAlias(): Promise<CliInstallResult> {
  const targetPath = await externalCoralPath()
  return process.platform === 'win32' ? installWindowsCli(targetPath) : installUnixShellAlias(targetPath)
}

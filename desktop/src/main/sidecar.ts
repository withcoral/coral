import { spawn, type ChildProcess } from 'node:child_process'
import { access } from 'node:fs/promises'
import { constants } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { app } from 'electron'

export interface CoralSidecar {
  url: string
  child: ChildProcess
  commandPath: string
  packaged: boolean
  stop(): Promise<void>
}

const READY_RE = /Coral UI listening on (http:\/\/(?:127\.0\.0\.1|localhost|\[[^\]]+\])(?::\d+)?|http:\/\/[^\s]+)/

function moduleDir(): string {
  return dirname(fileURLToPath(import.meta.url))
}

export function desktopRoot(): string {
  if (app.isPackaged) return process.resourcesPath
  return resolve(moduleDir(), '..', '..')
}

export function repoRoot(): string {
  return resolve(moduleDir(), '..', '..', '..')
}

export function bundledCoralPath(): string {
  const binary = process.platform === 'win32' ? 'coral.exe' : 'coral'
  return app.isPackaged
    ? join(process.resourcesPath, 'coral', binary)
    : resolve(desktopRoot(), 'resources', 'coral', binary)
}

export function bundledCoralCommandPath(): string {
  const command = process.platform === 'win32' ? 'coral.cmd' : 'coral'
  return app.isPackaged
    ? join(process.resourcesPath, 'bin', command)
    : resolve(desktopRoot(), 'resources', 'bin', command)
}

function releaseCoralPath(): string {
  return resolve(repoRoot(), 'target', 'release', process.platform === 'win32' ? 'coral.exe' : 'coral')
}

export async function externalCoralPath(): Promise<string> {
  const candidates = [
    bundledCoralPath(),
    releaseCoralPath(),
  ]

  for (const candidate of candidates) {
    try {
      await access(candidate, constants.X_OK)
      return candidate
    } catch {
      // Try the next candidate.
    }
  }

  throw new Error('No Coral binary is available yet. Run `npm run stage:coral --prefix desktop` first.')
}

export async function externalCoralCommandPath(): Promise<string> {
  const candidates = [bundledCoralCommandPath(), bundledCoralPath(), releaseCoralPath()]

  for (const candidate of candidates) {
    try {
      await access(candidate, constants.X_OK)
      return candidate
    } catch {
      // Try the next candidate.
    }
  }

  throw new Error('No Coral command is available yet. Run `npm run stage:coral --prefix desktop` first.')
}

function devSidecarCommand(): { command: string; args: string[]; cwd: string } {
  return {
    command: 'cargo',
    args: [
      'run',
      '--manifest-path',
      resolve(repoRoot(), 'Cargo.toml'),
      '--locked',
      '-p',
      'coral-cli',
      '--',
      'ui',
      '--no-open',
      '--port',
      '0',
    ],
    cwd: repoRoot(),
  }
}

function packagedSidecarCommand(): { command: string; args: string[]; cwd: string } {
  return {
    command: bundledCoralPath(),
    args: ['ui', '--no-open', '--port', '0'],
    cwd: process.resourcesPath,
  }
}

function sidecarCommand(): { command: string; args: string[]; cwd: string } {
  return app.isPackaged ? packagedSidecarCommand() : devSidecarCommand()
}

function envWithLoopbackNoProxy(): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = { ...process.env, CORAL_DESKTOP: '1' }
  for (const key of ['NO_PROXY', 'no_proxy']) {
    const existing = env[key]
      ?.split(',')
      .map((item: string) => item.trim())
      .filter(Boolean) ?? []
    for (const host of ['127.0.0.1', 'localhost', '::1']) {
      if (!existing.some((item: string) => item.toLowerCase() === host)) existing.push(host)
    }
    env[key] = existing.join(',')
  }
  return env
}

export function startCoralSidecar(): Promise<CoralSidecar> {
  const command = sidecarCommand()
  const child = spawn(command.command, command.args, {
    cwd: command.cwd,
    stdio: ['ignore', 'pipe', 'pipe'],
    env: envWithLoopbackNoProxy(),
  })

  return new Promise((resolveStart, rejectStart) => {
    let stdoutBuffer = ''
    let stderrTail = ''
    let resolved = false

    const reject = (error: Error) => {
      if (resolved) return
      rejectStart(error)
    }

    const resolveReady = (url: string) => {
      if (resolved) return
      resolved = true
      resolveStart({
        url,
        child,
        commandPath: command.command,
        packaged: app.isPackaged,
        stop: () => stopChild(child),
      })
    }

    child.on('error', reject)
    child.stdout?.on('data', (chunk: Buffer) => {
      const text = chunk.toString('utf8')
      process.stdout.write(`[coral-sidecar] ${text}`)
      stdoutBuffer += text
      const match = stdoutBuffer.match(READY_RE)
      if (match?.[1]) resolveReady(match[1])
    })
    child.stderr?.on('data', (chunk: Buffer) => {
      const text = chunk.toString('utf8')
      process.stderr.write(`[coral-sidecar] ${text}`)
      stderrTail = (stderrTail + text).slice(-8000)
    })
    child.once('exit', (code, signal) => {
      if (resolved) return
      reject(new Error(`Coral runtime exited before ready (code=${code}, signal=${signal}). ${stderrTail}`))
    })
  })
}

export function stopChild(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.killed) return Promise.resolve()

  return new Promise((resolveStop) => {
    const timeout = setTimeout(() => {
      child.kill('SIGKILL')
      resolveStop()
    }, 5000)
    child.once('exit', () => {
      clearTimeout(timeout)
      resolveStop()
    })
    child.kill('SIGTERM')
  })
}

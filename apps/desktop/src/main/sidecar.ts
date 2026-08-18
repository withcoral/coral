import { spawn, type ChildProcess } from 'node:child_process'
import { access } from 'node:fs/promises'
import { constants } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { app } from 'electron'
import { ensureDesktopCoralConfig } from './coral-config'

export interface CoralSidecar {
  url: string
  child: ChildProcess
  stop(): Promise<void>
}

// Only trust a loopback endpoint — the sidecar is a local process, so a
// non-loopback URL in its output should not be adopted as the runtime address.
const READY_RE = /Coral gRPC server listening on (http:\/\/(?:127\.0\.0\.1|localhost|\[::1\])(?::\d+)?)/
const PACKAGED_STARTUP_TIMEOUT_MS = 30_000
const OUTPUT_TAIL_LIMIT = 8000

function moduleDir(): string {
  return dirname(fileURLToPath(import.meta.url))
}

export function desktopRoot(): string {
  if (app.isPackaged) return process.resourcesPath
  return resolve(moduleDir(), '..', '..')
}

export function repoRoot(): string {
  return resolve(moduleDir(), '..', '..', '..', '..')
}

export function bundledCoralPath(): string {
  const binary = process.platform === 'win32' ? 'coral.exe' : 'coral'
  return app.isPackaged
    ? join(process.resourcesPath, 'coral', binary)
    : resolve(desktopRoot(), 'resources', 'coral', binary)
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

  throw new Error('No Coral binary is available yet. Run `npm run stage:coral --prefix apps/desktop` first.')
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
      'server',
    ],
    cwd: repoRoot(),
  }
}

function packagedSidecarCommand(): { command: string; args: string[]; cwd: string } {
  return {
    command: bundledCoralPath(),
    args: ['server'],
    // The binary is addressed absolutely and its state directory arrives as
    // CORAL_CONFIG_DIR, so cwd only decides where stray relative work lands.
    // `resourcesPath` is a read-only squashfs mount inside an AppImage;
    // userData is writable on every platform.
    cwd: app.getPath('userData'),
  }
}

function sidecarCommand(): { command: string; args: string[]; cwd: string } {
  return app.isPackaged ? packagedSidecarCommand() : devSidecarCommand()
}

function startupTimeoutMs(): number | null {
  return app.isPackaged ? PACKAGED_STARTUP_TIMEOUT_MS : null
}

function envWithLoopbackNoProxy(configDir: string): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = { ...process.env, CORAL_CONFIG_DIR: configDir, CORAL_DESKTOP: '1' }
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

// Every spawned sidecar process, tracked from spawn time so teardown can
// force-kill even one that is still starting up — its handle is otherwise only
// exposed once the start promise resolves.
const liveChildren = new Set<ChildProcess>()

export function killAllTrackedChildren(): void {
  for (const child of liveChildren) {
    child.kill('SIGKILL')
  }
}

export async function startCoralSidecar(): Promise<CoralSidecar> {
  const devPort = process.env.CORAL_DEV_SIDECAR_PORT || '8778'
  const configDir = app.isPackaged
    ? await ensureDesktopCoralConfig(app.getPath('userData'))
    : await ensureDesktopCoralConfig(app.getPath('userData'), {
        bindAddr: `127.0.0.1:${devPort}`,
        directory: `coral-dev-${devPort}`,
      })
  const command = sidecarCommand()
  const child = spawn(command.command, command.args, {
    cwd: command.cwd,
    stdio: ['ignore', 'pipe', 'pipe'],
    env: envWithLoopbackNoProxy(configDir),
  })
  liveChildren.add(child)
  child.once('exit', () => liveChildren.delete(child))

  return new Promise((resolveStart, rejectStart) => {
    let stdoutBuffer = ''
    let stderrTail = ''
    let settled = false
    let startupTimeout: ReturnType<typeof setTimeout> | null = null
    const timeoutMs = startupTimeoutMs()

    const reject = (error: Error) => {
      if (settled) return
      settled = true
      if (startupTimeout) clearTimeout(startupTimeout)
      rejectStart(error)
    }

    const resolveReady = (url: string) => {
      if (settled) return
      settled = true
      if (startupTimeout) clearTimeout(startupTimeout)
      stdoutBuffer = ''
      resolveStart({
        url,
        child,
        stop: () => stopChild(child),
      })
    }

    if (timeoutMs !== null) {
      startupTimeout = setTimeout(() => {
        reject(new Error(`Coral runtime did not become ready within ${timeoutMs / 1000}s. ${stderrTail}`))
        void stopChild(child)
      }, timeoutMs)
    }

    child.on('error', reject)
    child.stdout?.on('data', (chunk: Buffer) => {
      const text = chunk.toString('utf8')
      process.stdout.write(`[coral-sidecar] ${text}`)
      stdoutBuffer = (stdoutBuffer + text).slice(-OUTPUT_TAIL_LIMIT)
      const match = stdoutBuffer.match(READY_RE)
      if (match?.[1]) resolveReady(match[1])
    })
    child.stderr?.on('data', (chunk: Buffer) => {
      const text = chunk.toString('utf8')
      process.stderr.write(`[coral-sidecar] ${text}`)
      stderrTail = (stderrTail + text).slice(-OUTPUT_TAIL_LIMIT)
    })
    child.once('exit', (code, signal) => {
      if (settled) return
      reject(new Error(`Coral runtime exited before ready (code=${code}, signal=${signal}). ${stderrTail}`))
    })
  })
}

export function stopChild(child: ChildProcess): Promise<void> {
  // `killed` only means a signal was delivered, not that the process exited.
  // Wait for an actual exit/signal code before treating it as terminated.
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve()

  return new Promise((resolveStop) => {
    let killTimer: ReturnType<typeof setTimeout>
    let reapTimer: ReturnType<typeof setTimeout>
    const finish = () => {
      clearTimeout(killTimer)
      clearTimeout(reapTimer)
      // Drop the exit listener so the reap-timeout path can't leave it dangling.
      child.removeListener('exit', finish)
      resolveStop()
    }
    // Resolve on the actual exit — including the exit that SIGKILL triggers —
    // rather than immediately after sending the signal.
    child.once('exit', finish)
    killTimer = setTimeout(() => {
      child.kill('SIGKILL')
      // Last resort: give the OS a moment to reap, then stop waiting.
      reapTimer = setTimeout(finish, 1000)
    }, 5000)
    child.kill('SIGTERM')
  })
}

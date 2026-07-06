import { spawn } from 'node:child_process'
import { resolve } from 'node:path'

const desktopRoot = resolve(import.meta.dirname, '..')
const repoRoot = resolve(desktopRoot, '..', '..')
const electronViteBin = resolve(
  desktopRoot,
  'node_modules',
  '.bin',
  process.platform === 'win32' ? 'electron-vite.cmd' : 'electron-vite',
)

const children = new Set()
let shuttingDown = false

function spawnChild(command, args, options) {
  const child = spawn(command, args, {
    shell: process.platform === 'win32',
    ...options,
  })
  children.add(child)
  child.once('exit', () => children.delete(child))
  child.once('error', (error) => {
    children.delete(child)
    console.error(`[desktop-dev] failed to run ${command}: ${error.message}`)
    if (!shuttingDown) {
      shutdown()
      process.exitCode = 1
    }
  })
  return child
}

function shutdown(signal = 'SIGTERM') {
  if (shuttingDown) return
  shuttingDown = true
  for (const child of children) {
    if (!child.killed) child.kill(signal)
  }
}

function waitForAppUrl(child) {
  return new Promise((resolveWait, rejectWait) => {
    const urlPattern = /https?:\/\/(?:localhost|127\.0\.0\.1):\d+\/?/
    let buffer = ''
    let matched = false
    const timeout = setTimeout(() => {
      rejectWait(new Error('Timed out waiting for the app dev server URL.'))
    }, 30_000)

    function inspectOutput(chunk) {
      const text = chunk.toString('utf8')
      process.stdout.write(text)
      // Keep forwarding output, but stop accumulating once matched so the
      // detection buffer can't grow for the life of the dev session.
      if (matched) return
      // Accumulate so a URL split across chunks is still matched.
      buffer += text
      const match = buffer.match(urlPattern)
      if (match?.[0]) {
        matched = true
        buffer = ''
        clearTimeout(timeout)
        resolveWait(match[0])
      }
    }

    child.stdout?.on('data', inspectOutput)
    child.stderr?.on('data', inspectOutput)
    // A failed spawn emits only 'error' (no 'exit'); reject immediately instead
    // of hanging until the timeout.
    child.once('error', (error) => {
      clearTimeout(timeout)
      rejectWait(error)
    })
    child.once('exit', (code, signal) => {
      clearTimeout(timeout)
      rejectWait(new Error(`App dev server exited before ready (code=${code}, signal=${signal}).`))
    })
  })
}

process.once('SIGINT', () => shutdown('SIGINT'))
process.once('SIGTERM', () => shutdown('SIGTERM'))

const appDevServer = spawnChild('npm', ['run', 'dev', '--prefix', 'apps/reef'], {
  cwd: repoRoot,
  env: {
    ...process.env,
    CORAL_DESKTOP_APP: '1',
  },
  stdio: ['ignore', 'pipe', 'pipe'],
})

try {
  const appUrl = await waitForAppUrl(appDevServer)

  // If Reef exits after startup, tear down Electron rather than leaving it
  // pointed at a dead renderer endpoint.
  appDevServer.once('exit', (code, signal) => {
    if (shuttingDown) return
    console.error(`[desktop-dev] Reef dev server exited (code=${code}, signal=${signal}).`)
    shutdown()
    process.exitCode = code ?? 1
  })

  const electron = spawnChild(electronViteBin, ['dev', '--ignoreConfigWarning'], {
    cwd: desktopRoot,
    env: {
      ...process.env,
      ELECTRON_RENDERER_URL: appUrl,
    },
    stdio: 'inherit',
  })

  electron.once('exit', (code) => {
    shutdown()
    process.exitCode = code ?? 1
  })
} catch (error) {
  shutdown()
  console.error(error instanceof Error ? error.message : String(error))
  process.exitCode = 1
}

import { spawn } from 'node:child_process'
import { resolve } from 'node:path'

const desktopRoot = resolve(import.meta.dirname, '..')
const repoRoot = resolve(desktopRoot, '..')
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
    const timeout = setTimeout(() => {
      rejectWait(new Error('Timed out waiting for the app dev server URL.'))
    }, 30_000)

    function inspectOutput(chunk) {
      const text = chunk.toString('utf8')
      process.stdout.write(text)
      const match = text.match(urlPattern)
      if (match?.[0]) {
        clearTimeout(timeout)
        resolveWait(match[0])
      }
    }

    child.stdout?.on('data', inspectOutput)
    child.stderr?.on('data', inspectOutput)
    child.once('exit', (code, signal) => {
      clearTimeout(timeout)
      rejectWait(new Error(`App dev server exited before ready (code=${code}, signal=${signal}).`))
    })
  })
}

process.once('SIGINT', () => shutdown('SIGINT'))
process.once('SIGTERM', () => shutdown('SIGTERM'))

const appDevServer = spawnChild('npm', ['run', 'dev', '--prefix', 'reef'], {
  cwd: repoRoot,
  env: {
    ...process.env,
    CORAL_DESKTOP_APP: '1',
  },
  stdio: ['ignore', 'pipe', 'pipe'],
})

try {
  const appUrl = await waitForAppUrl(appDevServer)
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
    process.exitCode = code ?? 0
  })
} catch (error) {
  shutdown()
  console.error(error instanceof Error ? error.message : String(error))
  process.exitCode = 1
}

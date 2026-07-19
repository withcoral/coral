import { spawn, spawnSync } from 'node:child_process'
import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'

const desktopRoot = resolve(import.meta.dirname, '..')
const repoRoot = resolve(desktopRoot, '..', '..')
const electronDir = resolve(desktopRoot, 'node_modules', 'electron')
const electronViteBin = resolve(
  desktopRoot,
  'node_modules',
  '.bin',
  process.platform === 'win32' ? 'electron-vite.cmd' : 'electron-vite',
)

// Shared between the Reef React Router server (through CORAL_ENDPOINT) and the
// sidecar spawn in the Electron main process. The fixed port lets the server
// know its endpoint before Electron finishes starting the sidecar.
// `||` (not `??`) so an empty CORAL_DEV_SIDECAR_PORT also falls back to the
// default rather than propagating an empty port to either process.
const sidecarPort = process.env.CORAL_DEV_SIDECAR_PORT || '8778'

// A missing electron/path.txt (skipped/interrupted postinstall binary download)
// makes electron-vite die with a cryptic "Electron uninstall". Re-run the
// downloader once to self-heal before starting the dev servers.
function electronBinaryReady() {
  // Mirror electron-vite's resolver: an explicit ELECTRON_EXEC_PATH wins and
  // needs no downloaded binary. (It ignores ELECTRON_OVERRIDE_DIST_PATH.)
  if (process.env.ELECTRON_EXEC_PATH) return true
  const pathFile = resolve(electronDir, 'path.txt')
  if (!existsSync(pathFile)) return false
  const binary = readFileSync(pathFile, 'utf8').trim()
  return binary !== '' && existsSync(resolve(electronDir, 'dist', binary))
}

function ensureElectronBinary() {
  if (electronBinaryReady()) return
  console.error('[desktop-dev] Electron binary missing — downloading it…')
  spawnSync(process.execPath, [resolve(electronDir, 'install.js')], { cwd: electronDir, stdio: 'inherit' })
  if (!electronBinaryReady()) {
    console.error('[desktop-dev] Electron binary still unavailable. Run `node node_modules/electron/install.js` in apps/desktop; if it keeps failing the download is likely blocked or install ran with --ignore-scripts.')
    process.exit(1)
  }
}

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

// Fix a missing Electron binary before starting the (slow) dev servers, so the
// run doesn't get most of the way up only to die on "Electron uninstall".
ensureElectronBinary()

const appDevServer = spawnChild('npm', ['run', 'dev', '--prefix', 'apps/reef'], {
  cwd: repoRoot,
  env: {
    ...process.env,
    CORAL_DESKTOP_APP: '1',
    CORAL_ENDPOINT: `http://127.0.0.1:${sidecarPort}`,
    VITE_CORAL_DESKTOP_APP: '1',
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
      CORAL_DEV_SIDECAR_PORT: sidecarPort,
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

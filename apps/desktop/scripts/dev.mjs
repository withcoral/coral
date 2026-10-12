// Runs Desktop from the checkout: builds the runtime and the main process, then
// starts Electron. Electron's supervisor runs that runtime and Reef's Vite dev
// server, as a packaged app runs its bundled ones.
import { spawn, spawnSync } from 'node:child_process'
import { existsSync, readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { resolve } from 'node:path'

import { buildRuntime } from './runtime.mjs'

const desktopRoot = resolve(import.meta.dirname, '..')
const electronDir = resolve(desktopRoot, 'node_modules', 'electron')

// A skipped or interrupted postinstall leaves Electron without its binary, and
// the launch then fails with a cryptic message. Download it once to recover.
function ensureElectronBinary() {
  const ready = () => {
    const pathFile = resolve(electronDir, 'path.txt')
    if (!existsSync(pathFile)) return false
    const binary = readFileSync(pathFile, 'utf8').trim()
    return binary !== '' && existsSync(resolve(electronDir, 'dist', binary))
  }
  if (ready()) return
  console.error('[desktop-dev] Electron binary missing; downloading it')
  spawnSync(process.execPath, [resolve(electronDir, 'install.js')], {
    cwd: electronDir,
    stdio: 'inherit',
  })
  if (!ready()) {
    console.error(
      '[desktop-dev] Run `node node_modules/electron/install.js` in apps/desktop and retry.',
    )
    process.exit(1)
  }
}

ensureElectronBinary()
const runtime = buildRuntime()
const built = spawnSync(process.execPath, [resolve(desktopRoot, 'scripts/build.mjs')], {
  stdio: 'inherit',
})
if (built.status !== 0) process.exit(built.status ?? 1)

const electron = spawn(createRequire(import.meta.url)('electron'), [desktopRoot], {
  cwd: desktopRoot,
  env: { ...process.env, CORAL_DESKTOP_DEV_RUNTIME: runtime },
  stdio: 'inherit',
})
// Electron quits through its own shutdown, which stops the runtime and Reef.
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => electron.kill(signal))
electron.once('exit', (code) => process.exit(code ?? 1))

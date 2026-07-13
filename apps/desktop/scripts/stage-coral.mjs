import { chmod, copyFile, mkdir, rm } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { spawn } from 'node:child_process'

const desktopRoot = resolve(import.meta.dirname, '..')
const repoRoot = resolve(desktopRoot, '..', '..')
const outputDir = resolve(desktopRoot, 'resources', 'coral')
const binaryName = process.platform === 'win32' ? 'coral.exe' : 'coral'
const targetBinary = resolve(repoRoot, 'target', 'release', binaryName)
const universalMacBinary = resolve(repoRoot, 'target', 'release', 'coral-universal')
const universalMac = process.env.CORAL_DESKTOP_UNIVERSAL === '1'
const macTargets = ['x86_64-apple-darwin', 'aarch64-apple-darwin']

function run(command, args, options = {}) {
  return new Promise((resolveRun, rejectRun) => {
    const child = spawn(command, args, {
      cwd: repoRoot,
      stdio: 'inherit',
      shell: process.platform === 'win32',
      ...options,
    })
    child.on('error', rejectRun)
    child.on('exit', (code) => {
      if (code === 0) {
        resolveRun()
        return
      }
      rejectRun(new Error(`${command} ${args.join(' ')} exited with ${code}`))
    })
  })
}

await run('npm', ['ci', '--prefix', 'apps/ui'])
await run('npm', ['run', 'build', '--prefix', 'apps/ui'])
await run('npm', ['ci', '--prefix', 'apps/reef'])
await run('npm', ['run', 'build', '--prefix', 'apps/reef'], {
  env: {
    ...process.env,
    CORAL_DESKTOP_APP: '1',
    VITE_CORAL_DESKTOP_APP: '1',
  },
})

async function buildCoralCli() {
  if (!universalMac) {
    await run('cargo', ['build', '--locked', '-p', 'coral-cli', '--release'])
    return targetBinary
  }

  if (process.platform !== 'darwin') {
    throw new Error('CORAL_DESKTOP_UNIVERSAL=1 is only supported on macOS.')
  }

  await run('rustup', ['target', 'add', ...macTargets])
  for (const target of macTargets) {
    await run('cargo', ['build', '--locked', '-p', 'coral-cli', '--release', '--target', target])
  }
  await run('lipo', [
    '-create',
    ...macTargets.map((target) => resolve(repoRoot, 'target', target, 'release', binaryName)),
    '-output',
    universalMacBinary,
  ])
  return universalMacBinary
}

const builtBinary = await buildCoralCli()

await rm(outputDir, { recursive: true, force: true })
await mkdir(outputDir, { recursive: true })
await copyFile(builtBinary, join(outputDir, binaryName))

if (process.platform !== 'win32') {
  await chmod(join(outputDir, binaryName), 0o755)
}

console.log(`[stage-coral] staged ${builtBinary} -> ${join(outputDir, binaryName)}`)

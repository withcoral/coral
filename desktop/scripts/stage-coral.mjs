import { chmod, copyFile, mkdir, rm } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { spawn } from 'node:child_process'

const desktopRoot = resolve(import.meta.dirname, '..')
const repoRoot = resolve(desktopRoot, '..')
const outputDir = resolve(desktopRoot, 'resources', 'coral')
const commandDir = resolve(desktopRoot, 'resources', 'bin')
const binaryName = process.platform === 'win32' ? 'coral.exe' : 'coral'
const targetBinary = resolve(repoRoot, 'target', 'release', binaryName)

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

await run('npm', ['ci', '--prefix', 'ui'])
await run('npm', ['run', 'build', '--prefix', 'ui'])
await run('npm', ['ci', '--prefix', 'reef'])
await run('npm', ['run', 'build', '--prefix', 'reef'], {
  env: {
    ...process.env,
    CORAL_DESKTOP_APP: '1',
  },
})
await run('cargo', ['build', '--locked', '-p', 'coral-cli', '--release'])

await rm(outputDir, { recursive: true, force: true })
await rm(commandDir, { recursive: true, force: true })
await mkdir(outputDir, { recursive: true })
await copyFile(targetBinary, join(outputDir, binaryName))

if (process.platform !== 'win32') {
  await chmod(join(outputDir, binaryName), 0o755)
}

console.log(`[stage-coral] staged ${targetBinary} -> ${join(outputDir, binaryName)}`)

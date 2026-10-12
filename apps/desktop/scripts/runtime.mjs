// Builds the coral runtime that Desktop supervises.
import { spawnSync } from 'node:child_process'
import { resolve } from 'node:path'

export const repoRoot = resolve(import.meta.dirname, '..', '..', '..')

/**
 * Runs `cargo build` for the runtime and returns the binary's path. Cargo
 * reports it, wherever CARGO_TARGET_DIR and `target` put it.
 */
export function buildRuntime({ release = false, target } = {}) {
  const cargo = spawnSync(
    'cargo',
    [
      'build',
      '--locked',
      ...(release ? ['--release'] : []),
      ...(target ? ['--target', target] : []),
      '-p',
      'coral',
      '--bin',
      'coral',
      '--message-format=json-render-diagnostics',
    ],
    {
      cwd: repoRoot,
      stdio: ['ignore', 'pipe', 'inherit'],
      encoding: 'utf8',
      maxBuffer: 256 * 1024 * 1024,
    },
  )
  if (cargo.status !== 0) throw new Error(`cargo build failed (${cargo.status ?? cargo.signal})`)
  const executable = cargo.stdout
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line))
    .find(
      (message) => message.reason === 'compiler-artifact' && message.target?.name === 'coral',
    )?.executable
  if (!executable) throw new Error('cargo did not report the coral executable')
  return executable
}

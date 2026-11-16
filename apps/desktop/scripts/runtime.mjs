// Builds the coral runtime that Desktop supervises: the released binary,
// coral-ee, which staging installs as `coral` (ADR 0033).
import { spawnSync } from 'node:child_process'
import { resolve } from 'node:path'

export const repoRoot = resolve(import.meta.dirname, '..', '..', '..')

/**
 * Runs `cargo build` for the runtime and returns the binary's path. Cargo
 * reports it, wherever CARGO_TARGET_DIR and `target` put it.
 *
 * A release runtime is the one that ships, so its build links only the V8
 * archive that Cargo.toml records.
 */
export function buildRuntime({ release = false, target } = {}) {
  const env = release
    ? { ...process.env, RUSTY_V8_ARCHIVE_SHA256: v8ArchiveSha256(target) }
    : process.env
  const cargo = spawnSync(
    'cargo',
    [
      'build',
      '--locked',
      ...(release ? ['--release'] : []),
      ...(target ? ['--target', target] : []),
      '-p',
      'coral-ee',
      '--bin',
      'coral-ee',
      '--message-format=json-render-diagnostics',
    ],
    {
      cwd: repoRoot,
      env,
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
    // Only a binary's artifact has an executable; a library of the same name
    // does not.
    .find(
      (message) =>
        message.reason === 'compiler-artifact' &&
        message.target?.name === 'coral-ee' &&
        message.executable,
    )?.executable
  if (!executable) throw new Error('cargo did not report the coral-ee executable')
  return executable
}

/**
 * The SHA-256 that Cargo.toml records for the V8 archive of `target`, or of
 * the host when the build names no target. The v8 crate's build script
 * downloads that archive, and stops the build when RUSTY_V8_ARCHIVE_SHA256
 * names another digest. A target without a row cannot ship.
 */
function v8ArchiveSha256(target) {
  const triple = target ?? /^host: (\S+)/m.exec(capture('rustc', ['-vV']))?.[1]
  if (!triple) throw new Error('rustc -vV did not report its host')
  const { metadata } = JSON.parse(
    capture('cargo', ['metadata', '--locked', '--no-deps', '--format-version', '1']),
  )
  const sha256 = metadata?.['v8-archive']?.sha256?.[triple]
  if (!sha256) {
    throw new Error(
      `Cargo.toml records no V8 archive digest for ${triple}. ` +
        'Add its row to [workspace.metadata.v8-archive.sha256].',
    )
  }
  return sha256
}

function capture(command, args) {
  const result = spawnSync(command, args, {
    cwd: repoRoot,
    stdio: ['ignore', 'pipe', 'inherit'],
    encoding: 'utf8',
    maxBuffer: 256 * 1024 * 1024,
  })
  if (result.status !== 0) {
    const cause = result.error?.message ?? result.status ?? result.signal
    throw new Error(`${command} ${args[0]} failed (${cause})`)
  }
  return result.stdout
}

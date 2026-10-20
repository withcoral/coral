import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { chmod, copyFile, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { delimiter, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import test from 'node:test'

const script = fileURLToPath(new URL('./rusty-v8-sha256.sh', import.meta.url))
const linux = 'b'.repeat(64)
const windows = 'c'.repeat(64)
const pins = [
  '# a comment',
  `${linux}  v152.2.0/librusty_v8_release_x86_64-unknown-linux-gnu.a.gz`,
  `${windows}  v152.2.0/rusty_v8_release_x86_64-pc-windows-msvc.lib.gz`,
  '',
].join('\n')

// A checkout that locks `v8` at `version`, with `pins` and a `rustc` whose
// host is `host`. `eol` sets the line endings of every file and of `rustc`.
async function checkout(t, { version = '152.2.0', host = 'x86_64-unknown-linux-gnu', eol = '\n' } = {}) {
  const root = await mkdtemp(join(tmpdir(), 'coral-rusty-v8-sha256-'))
  t.after(() => rm(root, { recursive: true, force: true }))
  await mkdir(join(root, 'scripts'))
  await mkdir(join(root, 'crates/coral-v8'), { recursive: true })
  await mkdir(join(root, 'bin'))
  await copyFile(script, join(root, 'scripts/rusty-v8-sha256.sh'))
  const lock = ['[[package]]', 'name = "v8"', `version = "${version}"`, ''].join('\n')
  await writeFile(join(root, 'Cargo.lock'), lock.replaceAll('\n', eol))
  await writeFile(join(root, 'crates/coral-v8/rusty_v8.sha256'), pins.replaceAll('\n', eol))
  const rustc = join(root, 'bin/rustc')
  await writeFile(rustc, `#!/bin/sh\nprintf 'rustc 1.98.0${eol}host: ${host}${eol}'\n`)
  await chmod(rustc, 0o755)
  return (...args) =>
    spawnSync(join(root, 'scripts/rusty-v8-sha256.sh'), args, {
      encoding: 'utf8',
      env: { ...process.env, PATH: `${join(root, 'bin')}${delimiter}${process.env.PATH}` },
    })
}

const pinned = {
  'the host Linux archive': [{}, [], linux],
  'a named Windows archive': [{}, ['x86_64-pc-windows-msvc'], windows],
  'the host archive from a CRLF checkout': [{ host: 'x86_64-pc-windows-msvc', eol: '\r\n' }, [], windows],
}

for (const [name, [options, args, sha256]] of Object.entries(pinned)) {
  test(`prints the pin for ${name}`, async (t) => {
    const result = (await checkout(t, options))(...args)
    assert.equal(result.status, 0, result.stderr)
    assert.equal(result.stdout, `${sha256}\n`)
  })
}

const unpinned = {
  'a target without a pin': [{}, ['aarch64-unknown-linux-gnu']],
  'a v8 version without pins': [{ version: '153.0.0' }, []],
}

for (const [name, [options, args]] of Object.entries(unpinned)) {
  test(`fails for ${name}`, async (t) => {
    const result = (await checkout(t, options))(...args)
    assert.equal(result.status, 1)
    assert.equal(result.stdout, '')
    assert.match(result.stderr, /pins no v\d+\.\d+\.\d+\/\S+\.gz\.\nRun scripts\/rusty-v8-sha256\.sh --update/)
  })
}

import assert from 'node:assert/strict'
import { chmod, mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import test from 'node:test'

import {
  createStageCoralPlan,
  PREBUILT_CORAL_ENV,
  stageCoralBinary,
  validatePrebuiltCoral,
} from './stage-coral-plan.mjs'

async function createFixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'coral-desktop-stage-'))
  t.after(() => rm(root, { recursive: true, force: true }))

  const repoRoot = join(root, 'repo')
  const desktopRoot = join(repoRoot, 'apps', 'desktop')
  await mkdir(desktopRoot, { recursive: true })
  return { root, repoRoot, desktopRoot }
}

function createPlan(fixture, env, platform = 'darwin') {
  return createStageCoralPlan({
    env,
    platform,
    repoRoot: fixture.repoRoot,
    desktopRoot: fixture.desktopRoot,
  })
}

test('stages a valid prebuilt Coral file and makes the output executable', async (t) => {
  const fixture = await createFixture(t)
  const sourceBinary = join(fixture.root, 'prebuilt', 'coral')
  await mkdir(dirname(sourceBinary), { recursive: true })
  await writeFile(sourceBinary, 'prebuilt coral')
  await chmod(sourceBinary, 0o644)

  const plan = createPlan(fixture, {
    [PREBUILT_CORAL_ENV]: sourceBinary,
  })
  await mkdir(plan.outputDir, { recursive: true })
  const staleOutput = join(plan.outputDir, 'stale')
  await writeFile(staleOutput, 'remove me')
  await stageCoralBinary(plan)

  assert.equal(plan.mode, 'prebuilt')
  assert.equal(await readFile(plan.destinationBinary, 'utf8'), 'prebuilt coral')
  assert.notEqual((await stat(plan.destinationBinary)).mode & 0o111, 0)
  await assert.rejects(stat(staleOutput), { code: 'ENOENT' })
})

test('rejects a prebuilt Coral file inside the staging output before cleanup', async (t) => {
  const fixture = await createFixture(t)
  const outputDir = join(fixture.desktopRoot, 'resources', 'coral')
  const sourceBinary = join(outputDir, 'coral')
  await mkdir(outputDir, { recursive: true })
  await writeFile(sourceBinary, 'already staged coral')

  assert.throws(
    () => createPlan(fixture, { [PREBUILT_CORAL_ENV]: sourceBinary }),
    new RegExp(`${PREBUILT_CORAL_ENV} must point outside the staging output directory`),
  )
  assert.equal(await readFile(sourceBinary, 'utf8'), 'already staged coral')

  await assert.rejects(
    stageCoralBinary({
      mode: 'prebuilt',
      sourceBinary,
      destinationBinary: sourceBinary,
      outputDir,
      platform: 'darwin',
    }),
    new RegExp(`${PREBUILT_CORAL_ENV} must point outside the staging output directory`),
  )
  assert.equal(await readFile(sourceBinary, 'utf8'), 'already staged coral')
})

test('rejects relative, missing, empty, directory, and unreadable prebuilt inputs', async (t) => {
  const fixture = await createFixture(t)

  assert.throws(
    () => createPlan(fixture, { [PREBUILT_CORAL_ENV]: 'relative/coral' }),
    new RegExp(`${PREBUILT_CORAL_ENV} must be an absolute path`),
  )

  const missing = join(fixture.root, 'missing-coral')
  await assert.rejects(
    validatePrebuiltCoral(missing),
    new RegExp(`${PREBUILT_CORAL_ENV} does not exist or cannot be inspected`),
  )

  const empty = join(fixture.root, 'empty-coral')
  await writeFile(empty, '')
  await assert.rejects(
    validatePrebuiltCoral(empty),
    new RegExp(`${PREBUILT_CORAL_ENV} must point to a non-empty file`),
  )

  const directory = join(fixture.root, 'coral-directory')
  await mkdir(directory)
  await assert.rejects(
    validatePrebuiltCoral(directory),
    new RegExp(`${PREBUILT_CORAL_ENV} must point to a regular file`),
  )

  const unreadable = join(fixture.root, 'unreadable-coral')
  await writeFile(unreadable, 'coral')
  await assert.rejects(
    validatePrebuiltCoral(unreadable, {
      accessFile: async () => {
        const error = new Error('permission denied')
        error.code = 'EACCES'
        throw error
      },
    }),
    new RegExp(`${PREBUILT_CORAL_ENV} must point to a readable file`),
  )
})

test('rejects prebuilt and universal modes together', async (t) => {
  const fixture = await createFixture(t)
  const sourceBinary = join(fixture.root, 'coral')
  await writeFile(sourceBinary, 'coral')

  assert.throws(
    () =>
      createPlan(fixture, {
        [PREBUILT_CORAL_ENV]: sourceBinary,
        CORAL_DESKTOP_UNIVERSAL: '1',
      }),
    /cannot be combined with CORAL_DESKTOP_UNIVERSAL=1/,
  )
})

test('prebuilt mode builds Reef without scheduling UI or Rust toolchain commands', async (t) => {
  const fixture = await createFixture(t)
  const sourceBinary = join(fixture.root, 'coral')
  await writeFile(sourceBinary, 'coral')

  const plan = createPlan(fixture, { [PREBUILT_CORAL_ENV]: sourceBinary })

  assert.deepEqual(plan.commands, [
    {
      command: 'npm',
      args: ['ci', '--prefix', 'apps/reef'],
    },
    {
      command: 'npm',
      args: ['run', 'build', '--prefix', 'apps/reef'],
      env: {
        VITE_CORAL_DESKTOP_APP: '1',
      },
    },
  ])
  assert.equal(
    plan.commands.some(
      ({ command, args }) =>
        ['cargo', 'rustup', 'lipo'].includes(command) || args.includes('apps/ui'),
    ),
    false,
  )
})

test('native mode preserves the existing UI, Reef, and Cargo command plan', async (t) => {
  const fixture = await createFixture(t)
  const plan = createPlan(fixture, {}, 'linux')

  assert.equal(plan.mode, 'native')
  assert.deepEqual(plan.commands, [
    {
      command: 'npm',
      args: ['ci', '--prefix', 'apps/ui'],
    },
    {
      command: 'npm',
      args: ['run', 'build', '--prefix', 'apps/ui'],
    },
    {
      command: 'npm',
      args: ['ci', '--prefix', 'apps/reef'],
    },
    {
      command: 'npm',
      args: ['run', 'build', '--prefix', 'apps/reef'],
      env: {
        VITE_CORAL_DESKTOP_APP: '1',
      },
    },
    {
      command: 'cargo',
      args: ['build', '--locked', '-p', 'coral-cli', '--release'],
    },
  ])
  assert.equal(plan.sourceBinary, join(fixture.repoRoot, 'target', 'release', 'coral'))
})

test('universal mode preserves the existing UI, Reef, Rust, and lipo command plan', async (t) => {
  const fixture = await createFixture(t)
  const plan = createPlan(fixture, { CORAL_DESKTOP_UNIVERSAL: '1' })
  const x86Binary = join(
    fixture.repoRoot,
    'target',
    'x86_64-apple-darwin',
    'release',
    'coral',
  )
  const armBinary = join(
    fixture.repoRoot,
    'target',
    'aarch64-apple-darwin',
    'release',
    'coral',
  )
  const universalBinary = join(fixture.repoRoot, 'target', 'release', 'coral-universal')

  assert.equal(plan.mode, 'universal')
  assert.deepEqual(plan.commands, [
    {
      command: 'npm',
      args: ['ci', '--prefix', 'apps/ui'],
    },
    {
      command: 'npm',
      args: ['run', 'build', '--prefix', 'apps/ui'],
    },
    {
      command: 'npm',
      args: ['ci', '--prefix', 'apps/reef'],
    },
    {
      command: 'npm',
      args: ['run', 'build', '--prefix', 'apps/reef'],
      env: {
        VITE_CORAL_DESKTOP_APP: '1',
      },
    },
    {
      command: 'rustup',
      args: ['target', 'add', 'x86_64-apple-darwin', 'aarch64-apple-darwin'],
    },
    {
      command: 'cargo',
      args: [
        'build',
        '--locked',
        '-p',
        'coral-cli',
        '--release',
        '--target',
        'x86_64-apple-darwin',
      ],
    },
    {
      command: 'cargo',
      args: [
        'build',
        '--locked',
        '-p',
        'coral-cli',
        '--release',
        '--target',
        'aarch64-apple-darwin',
      ],
    },
    {
      command: 'lipo',
      args: ['-create', x86Binary, armBinary, '-output', universalBinary],
    },
  ])
  assert.equal(plan.sourceBinary, universalBinary)
})

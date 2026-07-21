import { constants } from 'node:fs'
import { access, chmod, copyFile, mkdir, realpath, rm, stat } from 'node:fs/promises'
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path'

export const PREBUILT_CORAL_ENV = 'CORAL_DESKTOP_PREBUILT_CORAL'

const macTargets = ['x86_64-apple-darwin', 'aarch64-apple-darwin']

function npmCommand(args, env) {
  return {
    command: 'npm',
    args,
    ...(env ? { env } : {}),
  }
}

function commonBuildCommands(includeUi) {
  return [
    ...(includeUi
      ? [
          npmCommand(['ci', '--prefix', 'apps/ui']),
          npmCommand(['run', 'build', '--prefix', 'apps/ui']),
        ]
      : []),
    npmCommand(['ci', '--prefix', 'apps/reef']),
    npmCommand(['run', 'build', '--prefix', 'apps/reef'], {
      CORAL_DESKTOP_APP: '1',
      VITE_CORAL_DESKTOP_APP: '1',
    }),
  ]
}

function requirePrebuiltOutsideOutputDirectory(sourceBinary, outputDir) {
  const relativeSource = relative(outputDir, sourceBinary)
  const sourceIsInsideOutput =
    relativeSource === '' ||
    (relativeSource !== '..' &&
      !relativeSource.startsWith(`..${sep}`) &&
      !isAbsolute(relativeSource))

  if (sourceIsInsideOutput) {
    throw new Error(
      `${PREBUILT_CORAL_ENV} must point outside the staging output directory ${outputDir}; that directory is cleared before copying.`,
    )
  }
}

async function canonicalizePotentialPath(path, realpathFile) {
  let existingAncestor = resolve(path)
  const missingSegments = []

  while (true) {
    try {
      const canonicalAncestor = await realpathFile(existingAncestor)
      return join(canonicalAncestor, ...missingSegments.reverse())
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error

      const parent = dirname(existingAncestor)
      if (parent === existingAncestor) throw error
      missingSegments.push(basename(existingAncestor))
      existingAncestor = parent
    }
  }
}

async function requireCanonicalPrebuiltOutsideOutputDirectory(
  sourceBinary,
  outputDir,
  realpathFile,
) {
  let canonicalPaths
  try {
    canonicalPaths = await Promise.all([
      realpathFile(sourceBinary),
      canonicalizePotentialPath(outputDir, realpathFile),
    ])
  } catch (error) {
    throw new Error(
      `${PREBUILT_CORAL_ENV} or its staging output directory could not be resolved.`,
      { cause: error },
    )
  }

  requirePrebuiltOutsideOutputDirectory(...canonicalPaths)
}

export function createStageCoralPlan({
  env = process.env,
  platform = process.platform,
  repoRoot,
  desktopRoot,
}) {
  const outputDir = resolve(desktopRoot, 'resources', 'coral')
  const binaryName = platform === 'win32' ? 'coral.exe' : 'coral'
  const destinationBinary = resolve(outputDir, binaryName)
  const universalMac = env.CORAL_DESKTOP_UNIVERSAL === '1'
  const prebuiltInput = env[PREBUILT_CORAL_ENV]
  const prebuiltSelected = prebuiltInput !== undefined

  if (prebuiltSelected && universalMac) {
    throw new Error(
      `${PREBUILT_CORAL_ENV} cannot be combined with CORAL_DESKTOP_UNIVERSAL=1.`,
    )
  }

  if (prebuiltSelected) {
    if (typeof prebuiltInput !== 'string' || !isAbsolute(prebuiltInput)) {
      throw new Error(
        `${PREBUILT_CORAL_ENV} must be an absolute path; received ${JSON.stringify(prebuiltInput)}.`,
      )
    }

    const sourceBinary = resolve(prebuiltInput)
    requirePrebuiltOutsideOutputDirectory(sourceBinary, outputDir)

    return {
      mode: 'prebuilt',
      commands: commonBuildCommands(false),
      sourceBinary,
      destinationBinary,
      outputDir,
      platform,
    }
  }

  const commands = commonBuildCommands(true)
  if (!universalMac) {
    commands.push({
      command: 'cargo',
      args: ['build', '--locked', '-p', 'coral-cli', '--release'],
    })
    return {
      mode: 'native',
      commands,
      sourceBinary: resolve(repoRoot, 'target', 'release', binaryName),
      destinationBinary,
      outputDir,
      platform,
    }
  }

  if (platform !== 'darwin') {
    throw new Error('CORAL_DESKTOP_UNIVERSAL=1 is only supported on macOS.')
  }

  commands.push(
    {
      command: 'rustup',
      args: ['target', 'add', ...macTargets],
    },
    ...macTargets.map((target) => ({
      command: 'cargo',
      args: ['build', '--locked', '-p', 'coral-cli', '--release', '--target', target],
    })),
  )

  const universalBinary = resolve(repoRoot, 'target', 'release', 'coral-universal')
  commands.push({
    command: 'lipo',
    args: [
      '-create',
      ...macTargets.map((target) =>
        resolve(repoRoot, 'target', target, 'release', binaryName),
      ),
      '-output',
      universalBinary,
    ],
  })

  return {
    mode: 'universal',
    commands,
    sourceBinary: universalBinary,
    destinationBinary,
    outputDir,
    platform,
  }
}

export async function validatePrebuiltCoral(
  sourceBinary,
  { statFile = stat, accessFile = access, realpathFile = realpath, outputDir } = {},
) {
  let metadata
  try {
    metadata = await statFile(sourceBinary)
  } catch (error) {
    throw new Error(
      `${PREBUILT_CORAL_ENV} does not exist or cannot be inspected: ${sourceBinary}.`,
      { cause: error },
    )
  }

  if (!metadata.isFile()) {
    throw new Error(
      `${PREBUILT_CORAL_ENV} must point to a regular file: ${sourceBinary}.`,
    )
  }
  if (metadata.size === 0) {
    throw new Error(
      `${PREBUILT_CORAL_ENV} must point to a non-empty file: ${sourceBinary}.`,
    )
  }

  try {
    await accessFile(sourceBinary, constants.R_OK)
  } catch (error) {
    throw new Error(
      `${PREBUILT_CORAL_ENV} must point to a readable file: ${sourceBinary}.`,
      { cause: error },
    )
  }

  if (outputDir) {
    await requireCanonicalPrebuiltOutsideOutputDirectory(
      sourceBinary,
      outputDir,
      realpathFile,
    )
  }
}

export async function stageCoralBinary(plan) {
  if (plan.mode === 'prebuilt') {
    requirePrebuiltOutsideOutputDirectory(plan.sourceBinary, plan.outputDir)
    await validatePrebuiltCoral(plan.sourceBinary, { outputDir: plan.outputDir })
  }

  await rm(plan.outputDir, { recursive: true, force: true })
  await mkdir(plan.outputDir, { recursive: true })
  await copyFile(plan.sourceBinary, plan.destinationBinary)

  if (plan.platform !== 'win32') {
    await chmod(plan.destinationBinary, 0o755)
  }
}

import { spawn } from 'node:child_process'
import { resolve } from 'node:path'

import {
  createStageCoralPlan,
  stageCoralBinary,
  validatePrebuiltCoral,
} from './stage-coral-plan.mjs'

const desktopRoot = resolve(import.meta.dirname, '..')
const repoRoot = resolve(desktopRoot, '..', '..')

function run({ command, args, env }) {
  return new Promise((resolveRun, rejectRun) => {
    const child = spawn(command, args, {
      cwd: repoRoot,
      stdio: 'inherit',
      shell: process.platform === 'win32',
      ...(env ? { env: { ...process.env, ...env } } : {}),
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

const plan = createStageCoralPlan({
  env: process.env,
  platform: process.platform,
  repoRoot,
  desktopRoot,
})

if (plan.mode === 'prebuilt') {
  console.log('[stage-coral] prebuilt mode selected')
  console.log(`[stage-coral] prebuilt source: ${plan.sourceBinary}`)
  console.log(`[stage-coral] prebuilt destination: ${plan.destinationBinary}`)
  await validatePrebuiltCoral(plan.sourceBinary, { outputDir: plan.outputDir })
}

for (const command of plan.commands) {
  await run(command)
}

await stageCoralBinary(plan)

console.log(`[stage-coral] staged ${plan.sourceBinary} -> ${plan.destinationBinary}`)

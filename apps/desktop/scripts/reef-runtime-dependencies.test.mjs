import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

async function packageJson(relativePath) {
  return JSON.parse(await readFile(new URL(relativePath, import.meta.url), 'utf8'))
}

test('desktop packages every Reef runtime dependency', async () => {
  const [desktopPackage, reefPackage] = await Promise.all([
    packageJson('../package.json'),
    packageJson('../../reef/package.json'),
  ])
  const desktopDependencies = desktopPackage.dependencies ?? {}
  const reefDependencies = Object.keys(reefPackage.dependencies ?? {})
  const missingDependencies = reefDependencies.filter(
    (dependency) => !(dependency in desktopDependencies),
  )

  assert.deepEqual(
    missingDependencies,
    [],
    `packaged Reef server dependencies missing from apps/desktop/package.json: ${missingDependencies.join(', ')}`,
  )
})
